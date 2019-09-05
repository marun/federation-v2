/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sync

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/kubefed/pkg/apis/core/typeconfig"
	fedv1b1 "sigs.k8s.io/kubefed/pkg/apis/core/v1beta1"
	"sigs.k8s.io/kubefed/pkg/controller/sync/dispatch"
	"sigs.k8s.io/kubefed/pkg/controller/sync/version"
	"sigs.k8s.io/kubefed/pkg/controller/util"
)

// FederatedResource encapsulates the behavior of a logical federated
// resource which may be implemented by one or more kubernetes
// resources in the cluster hosting the KubeFed control plane.
type FederatedResource interface {
	dispatch.FederatedResourceForDispatch

	FederatedName() util.QualifiedName
	FederatedKind() string
	UpdateVersions(selectedClusters []string, versionMap map[string]string) error
	DeleteVersions()
	ComputePlacement(clusters []*fedv1b1.KubeFedCluster) (selectedClusters sets.String, err error)
	NamespaceNotFederated() bool
}

type federatedResource struct {
	sync.RWMutex

	limitedScope      bool
	typeConfig        typeconfig.Interface
	targetIsNamespace bool
	targetName        util.QualifiedName
	federatedKind     string
	federatedName     util.QualifiedName
	federatedResource *unstructured.Unstructured
	versionManager    *version.VersionManager
	overridesMap      util.OverridesMap
	versionMap        map[string]string
	namespace         *unstructured.Unstructured
	fedNamespace      *unstructured.Unstructured
	eventRecorder     record.EventRecorder
}

func (r *federatedResource) FederatedName() util.QualifiedName {
	return r.federatedName
}

func (r *federatedResource) FederatedKind() string {
	return r.typeConfig.GetFederatedType().Kind
}

func (r *federatedResource) TargetName() util.QualifiedName {
	return r.targetName
}

func (r *federatedResource) TargetKind() string {
	return r.typeConfig.GetTargetType().Kind
}

func (r *federatedResource) TargetGVK() schema.GroupVersionKind {
	apiResource := r.typeConfig.GetTargetType()
	return apiResourceToGVK(&apiResource)
}

func (r *federatedResource) Object() *unstructured.Unstructured {
	return r.federatedResource
}

func (r *federatedResource) TemplateVersion() (string, error) {
	obj := r.federatedResource
	return GetTemplateHash(obj.Object)
}

func (r *federatedResource) OverrideVersion() (string, error) {
	// TODO(marun) Consider hashing overrides per cluster to minimize
	// unnecessary updates.
	return GetOverrideHash(r.federatedResource)
}

func (r *federatedResource) VersionForCluster(clusterName string) (string, error) {
	r.Lock()
	defer r.Unlock()
	if r.versionMap == nil {
		var err error
		r.versionMap, err = r.versionManager.Get(r)
		if err != nil {
			return "", err
		}
	}
	return r.versionMap[clusterName], nil
}

func (r *federatedResource) UpdateVersions(selectedClusters []string, versionMap map[string]string) error {
	return r.versionManager.Update(r, selectedClusters, versionMap)
}

func (r *federatedResource) DeleteVersions() {
	r.versionManager.Delete(r.federatedName)
}

func (r *federatedResource) ComputePlacement(clusters []*fedv1b1.KubeFedCluster) (sets.String, error) {
	if r.typeConfig.GetNamespaced() {
		return computeNamespacedPlacement(r.federatedResource, r.fedNamespace, clusters, r.limitedScope)
	}
	return computePlacement(r.federatedResource, clusters)
}

func (r *federatedResource) NamespaceNotFederated() bool {
	return r.typeConfig.GetNamespaced() && r.fedNamespace == nil
}

func (r *federatedResource) IsNamespaceInHostCluster(clusterObj pkgruntime.Object) bool {
	// TODO(marun) This comment should be added to the documentation
	// and removed from this function (where it is no longer
	// relevant).
	//
	// `Namespace` is the only Kubernetes type that can contain other
	// types, and adding a KubeFed-specific container type would be
	// difficult or impossible. This requires that namespaced
	// federated resources exist in regular namespaces.
	//
	// An implication of using regular namespaces is that the sync
	// controller cannot delete a namespace in the host cluster in
	// response to a contained federated namespace not selecting the
	// host cluster for placement.  Doing so would remove the
	// federated namespace and result in the removal of the namespace
	// from all clusters (not just from the host cluster).
	//
	// Deletion of a federated namespace should also not result in
	// deletion of its containing namespace, since that could result
	// in the deletion of a namespaced KubeFed control plane.
	return r.targetIsNamespace && util.IsPrimaryCluster(r.namespace, clusterObj)
}

// TODO(marun) Marshall the template once per reconcile, not per-cluster
func (r *federatedResource) ObjectForCluster(clusterName string) (*unstructured.Unstructured, error) {
	templateBody, ok, err := unstructured.NestedMap(r.federatedResource.Object, util.SpecField, util.TemplateField)
	if err != nil {
		return nil, errors.Wrap(err, "Error retrieving template body")
	}
	if !ok {
		// Some resources (like namespaces) can be created from an
		// empty template.
		templateBody = make(map[string]interface{})
	}
	obj := &unstructured.Unstructured{Object: templateBody}

	notSupportedTemplate := "metadata.%s cannot be set via template to avoid conflicting with controllers " +
		"in member clusters. Consider using an override to add or remove elements from this collection."
	if len(obj.GetAnnotations()) > 0 {
		r.RecordError("AnnotationsNotSupported", errors.Errorf(notSupportedTemplate, "annotations"))
		obj.SetAnnotations(nil)
	}
	if len(obj.GetFinalizers()) > 0 {
		r.RecordError("FinalizersNotSupported", errors.Errorf(notSupportedTemplate, "finalizers"))
		obj.SetFinalizers(nil)
	}

	// Avoid having to duplicate these details in the template or have
	// the name/namespace vary between the KubeFed api and member
	// clusters.
	//
	// TODO(marun) this should be documented
	obj.SetName(r.federatedResource.GetName())
	if !r.targetIsNamespace {
		namespace := util.NamespaceForCluster(clusterName, r.federatedResource.GetNamespace())
		obj.SetNamespace(namespace)
	}
	targetApiResource := r.typeConfig.GetTargetType()
	obj.SetKind(targetApiResource.Kind)

	// If the template does not specify an api version, default it to
	// the one configured for the target type in the FTC.
	if len(obj.GetAPIVersion()) == 0 {
		obj.SetAPIVersion(fmt.Sprintf("%s/%s", targetApiResource.Group, targetApiResource.Version))
	}

	return obj, nil
}

// ApplyOverrides applies overrides for the named cluster to the given
// object. The managed label is added afterwards to ensure labeling even if an
// override was attempted.
func (r *federatedResource) ApplyOverrides(obj *unstructured.Unstructured, clusterName string) error {
	overrides, err := r.overridesForCluster(clusterName)
	if err != nil {
		return err
	}
	if overrides != nil {
		if err := util.ApplyJsonPatch(obj, overrides); err != nil {
			return err
		}
	}

	// Ensure that resources managed by KubeFed always have the
	// managed label.  The label is intended to be targeted by all the
	// KubeFed controllers.
	util.AddManagedLabel(obj)

	return nil
}

// TODO(marun) Use an enumeration for errorCode.
func (r *federatedResource) RecordError(errorCode string, err error) {
	r.eventRecorder.Eventf(r.Object(), corev1.EventTypeWarning, errorCode, err.Error())
}

func (r *federatedResource) RecordEvent(reason, messageFmt string, args ...interface{}) {
	r.eventRecorder.Eventf(r.Object(), corev1.EventTypeNormal, reason, messageFmt, args...)
}

func (r *federatedResource) overridesForCluster(clusterName string) (util.ClusterOverrides, error) {
	r.Lock()
	defer r.Unlock()
	if r.overridesMap == nil {
		overridesMap, err := util.GetOverrides(r.federatedResource)
		if err != nil {
			return nil, errors.Wrapf(err, "Error reading cluster overrides")
		}
		r.overridesMap = overridesMap
	}
	return r.overridesMap[clusterName], nil
}

func GetTemplateHash(fieldMap map[string]interface{}) (string, error) {
	fields := []string{util.SpecField, util.TemplateField}
	fieldMap, ok, err := unstructured.NestedMap(fieldMap, fields...)
	if err != nil {
		return "", errors.Wrapf(err, "Error retrieving %q", strings.Join(fields, "."))
	}
	if !ok {
		return "", nil
	}
	obj := &unstructured.Unstructured{Object: fieldMap}
	description := strings.Join(fields, ".")
	return hashUnstructured(obj, description)
}

func GetOverrideHash(rawObj *unstructured.Unstructured) (string, error) {
	override := util.GenericOverride{}
	err := util.UnstructuredToInterface(rawObj, &override)
	if err != nil {
		return "", errors.Wrap(err, "Error retrieving overrides")
	}
	if override.Spec == nil {
		return "", nil
	}
	// Only hash the overrides
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"overrides": override.Spec.Overrides,
		},
	}

	return hashUnstructured(obj, "overrides")
}

// TODO(marun) Investigate alternate ways of computing the hash of a field map.
func hashUnstructured(obj *unstructured.Unstructured, description string) (string, error) {
	jsonBytes, err := obj.MarshalJSON()
	if err != nil {
		return "", errors.Wrapf(err, "Failed to marshal %q to json", description)
	}
	hash := md5.New()
	if _, err := hash.Write(jsonBytes); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func apiResourceToGVK(apiResource *metav1.APIResource) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   apiResource.Group,
		Version: apiResource.Version,
		Kind:    apiResource.Kind,
	}
}

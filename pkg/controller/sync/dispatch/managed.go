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

package dispatch

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/kubefed/pkg/client/generic"
	"sigs.k8s.io/kubefed/pkg/controller/sync/status"
	"sigs.k8s.io/kubefed/pkg/controller/util"
)

// FederatedResourceForDispatch is the subset of the FederatedResource
// interface required for dispatching operations to managed resources.
type FederatedResourceForDispatch interface {
	TargetName() util.QualifiedName
	TargetKind() string
	TargetGVK() schema.GroupVersionKind
	Object() *unstructured.Unstructured
	VersionForCluster(clusterName string) (string, error)
	ObjectForCluster(clusterName string) (*unstructured.Unstructured, error)
	ApplyOverrides(obj *unstructured.Unstructured, clusterName string) error
	RecordError(errorCode string, err error)
	RecordEvent(reason, messageFmt string, args ...interface{})
	IsNamespaceInHostCluster(clusterObj pkgruntime.Object) bool
}

// ManagedDispatcher dispatches operations to member clusters for resources
// managed by a federated resource.
type ManagedDispatcher interface {
	UnmanagedDispatcher

	Create(clusterName string)
	Update(clusterName string, clusterObj *unstructured.Unstructured)
	VersionMap() map[string]string
	CollectedStatus() status.CollectedPropagationStatus

	RecordClusterError(propStatus status.PropagationStatus, clusterName string, err error)
	RecordStatus(clusterName string, propStatus status.PropagationStatus)
}

type managedDispatcherImpl struct {
	sync.RWMutex

	dispatcher            *operationDispatcherImpl
	unmanagedDispatcher   *unmanagedDispatcherImpl
	fedResource           FederatedResourceForDispatch
	versionMap            map[string]string
	statusMap             status.PropagationStatusMap
	skipAdoptingResources bool

	// Track when resource updates are performed to allow indicating
	// when a change was last propagated to member clusters.
	resourcesUpdated bool
}

func NewManagedDispatcher(clientAccessor clientAccessorFunc, fedResource FederatedResourceForDispatch, skipAdoptingResources bool) ManagedDispatcher {
	d := &managedDispatcherImpl{
		fedResource:           fedResource,
		versionMap:            make(map[string]string),
		statusMap:             make(status.PropagationStatusMap),
		skipAdoptingResources: skipAdoptingResources,
	}
	d.dispatcher = newOperationDispatcher(clientAccessor, d)
	d.unmanagedDispatcher = newUnmanagedDispatcher(d.dispatcher, d, fedResource.TargetGVK(), fedResource.TargetName())
	return d
}

func (d *managedDispatcherImpl) Wait() (bool, error) {
	ok, err := d.dispatcher.Wait()
	if err != nil {
		return ok, err
	}

	// Transition the status of clusters that still have a default
	// timed out status.
	d.RLock()
	defer d.RUnlock()
	// Transition timed out status for this set to ok.
	okTimedOut := sets.NewString(
		string(status.CreationTimedOut),
		string(status.UpdateTimedOut),
	)
	for key, value := range d.statusMap {
		propStatus := string(value)
		if okTimedOut.Has(propStatus) {
			d.statusMap[key] = status.ClusterPropagationOK
		} else if propStatus == string(status.DeletionTimedOut) {
			// If deletion was successful, then assume the resource is
			// pending garbage collection.
			d.statusMap[key] = status.WaitingForRemoval
		} else if propStatus == string(status.LabelRemovalTimedOut) {
			// If label removal was successful, the resource is
			// effectively unmanaged for the cluster even though it
			// still may be cached.
			delete(d.statusMap, key)
		}
	}
	return ok, nil
}

func (d *managedDispatcherImpl) Create(clusterName string) {
	// Default the status to an operation-specific timeout.  Otherwise
	// when a timeout occurs it won't be possible to determine which
	// operation timed out.  The timeout status will be cleared by
	// Wait() if a timeout does not occur.
	d.RecordStatus(clusterName, status.CreationTimedOut)

	d.dispatcher.incrementOperationsInitiated()
	const op = "create"
	go d.dispatcher.clusterOperation(clusterName, op, func(client generic.Client) util.ReconciliationStatus {
		d.recordEvent(clusterName, op, "Creating")

		obj, err := d.fedResource.ObjectForCluster(clusterName)
		if err != nil {
			return d.recordOperationError(status.ComputeResourceFailed, clusterName, op, err)
		}

		err = d.fedResource.ApplyOverrides(obj, clusterName)
		if err != nil {
			return d.recordOperationError(status.ApplyOverridesFailed, clusterName, op, err)
		}

		err = client.Create(context.Background(), obj)
		if err == nil {
			version := util.ObjectVersion(obj)
			d.recordVersion(clusterName, version)
			return util.StatusAllOK
		}

		// TODO(marun) Figure out why attempting to create a namespace that
		// already exists indicates ServerTimeout instead of AlreadyExists.
		alreadyExists := apierrors.IsAlreadyExists(err) || d.fedResource.TargetKind() == util.NamespaceKind && apierrors.IsServerTimeout(err)
		if !alreadyExists {
			return d.recordOperationError(status.CreationFailed, clusterName, op, err)
		}

		// Attempt to update the existing resource to ensure that it
		// is labeled as a managed resource.
		err = client.Get(context.Background(), obj, obj.GetNamespace(), obj.GetName())
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to retrieve object potentially requiring adoption")
			return d.recordOperationError(status.RetrievalFailed, clusterName, op, wrappedErr)
		}

		if d.skipAdoptingResources && !d.fedResource.IsNamespaceInHostCluster(obj) {
			_ = d.recordOperationError(status.AlreadyExists, clusterName, op, errors.Errorf("Resource pre-exist in cluster"))
			return util.StatusAllOK
		}

		d.recordError(clusterName, op, errors.Errorf("An update will be attempted instead of a creation due to an existing resource"))
		d.Update(clusterName, obj)
		return util.StatusAllOK
	})
}

func (d *managedDispatcherImpl) Update(clusterName string, clusterObj *unstructured.Unstructured) {
	d.RecordStatus(clusterName, status.UpdateTimedOut)

	d.dispatcher.incrementOperationsInitiated()
	const op = "update"
	go d.dispatcher.clusterOperation(clusterName, op, func(client generic.Client) util.ReconciliationStatus {
		obj, err := d.fedResource.ObjectForCluster(clusterName)
		if err != nil {
			return d.recordOperationError(status.ComputeResourceFailed, clusterName, op, err)
		}

		err = RetainClusterFields(d.fedResource.TargetKind(), obj, clusterObj, d.fedResource.Object())
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to retain fields")
			return d.recordOperationError(status.FieldRetentionFailed, clusterName, op, wrappedErr)
		}

		err = d.fedResource.ApplyOverrides(obj, clusterName)
		if err != nil {
			return d.recordOperationError(status.ApplyOverridesFailed, clusterName, op, err)
		}

		version, err := d.fedResource.VersionForCluster(clusterName)
		if err != nil {
			return d.recordOperationError(status.VersionRetrievalFailed, clusterName, op, err)
		}
		if !util.ObjectNeedsUpdate(obj, clusterObj, version) {
			// Resource is current
			return util.StatusAllOK
		}

		// Only record an event if the resource is not current
		d.recordEvent(clusterName, op, "Updating")

		err = client.Update(context.Background(), obj)
		if err != nil {
			return d.recordOperationError(status.UpdateFailed, clusterName, op, err)
		}
		d.setResourcesUpdated()
		version = util.ObjectVersion(obj)
		d.recordVersion(clusterName, version)
		return util.StatusAllOK
	})
}

func (d *managedDispatcherImpl) Delete(clusterName string) {
	d.RecordStatus(clusterName, status.DeletionTimedOut)

	d.unmanagedDispatcher.Delete(clusterName)
}

func (d *managedDispatcherImpl) RemoveManagedLabel(clusterName string, clusterObj *unstructured.Unstructured) {
	d.RecordStatus(clusterName, status.LabelRemovalTimedOut)

	d.unmanagedDispatcher.RemoveManagedLabel(clusterName, clusterObj)
}

func (d *managedDispatcherImpl) RecordClusterError(propStatus status.PropagationStatus, clusterName string, err error) {
	d.fedResource.RecordError(string(propStatus), err)
	d.RecordStatus(clusterName, propStatus)
}

func (d *managedDispatcherImpl) RecordStatus(clusterName string, propStatus status.PropagationStatus) {
	d.Lock()
	defer d.Unlock()
	d.statusMap[clusterName] = propStatus
}

func (d *managedDispatcherImpl) recordOperationError(propStatus status.PropagationStatus, clusterName, operation string, err error) util.ReconciliationStatus {
	d.recordError(clusterName, operation, err)
	d.RecordStatus(clusterName, propStatus)
	return util.StatusError
}

func (d *managedDispatcherImpl) recordError(clusterName, operation string, err error) {
	targetName := d.unmanagedDispatcher.targetNameForCluster(clusterName)
	args := []interface{}{operation, d.fedResource.TargetKind(), targetName, clusterName}
	eventType := fmt.Sprintf("%sInClusterFailed", strings.Replace(strings.Title(operation), " ", "", -1))
	d.fedResource.RecordError(eventType, errors.Wrapf(err, "Failed to "+eventTemplate, args...))
}

func (d *managedDispatcherImpl) recordEvent(clusterName, operation, operationContinuous string) {
	targetName := d.unmanagedDispatcher.targetNameForCluster(clusterName)
	args := []interface{}{operationContinuous, d.fedResource.TargetKind(), targetName, clusterName}
	eventType := fmt.Sprintf("%sInCluster", strings.Replace(strings.Title(operation), " ", "", -1))
	d.fedResource.RecordEvent(eventType, eventTemplate, args...)
}

func (d *managedDispatcherImpl) VersionMap() map[string]string {
	d.RLock()
	defer d.RUnlock()
	versionMap := make(map[string]string)
	for key, value := range d.versionMap {
		versionMap[key] = value
	}
	return versionMap
}

func (d *managedDispatcherImpl) recordVersion(clusterName, version string) {
	d.Lock()
	defer d.Unlock()
	d.versionMap[clusterName] = version
}

func (d *managedDispatcherImpl) setResourcesUpdated() {
	d.Lock()
	defer d.Unlock()
	d.resourcesUpdated = true
}

func (d *managedDispatcherImpl) CollectedStatus() status.CollectedPropagationStatus {
	d.RLock()
	defer d.RUnlock()
	statusMap := make(status.PropagationStatusMap)
	for key, value := range d.statusMap {
		statusMap[key] = value
	}
	return status.CollectedPropagationStatus{
		StatusMap:        statusMap,
		ResourcesUpdated: d.resourcesUpdated,
	}
}

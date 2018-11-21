/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-sigs/federation-v2/pkg/apis/core/typeconfig"
	fedv1a1 "github.com/kubernetes-sigs/federation-v2/pkg/apis/core/v1alpha1"
	fedclientset "github.com/kubernetes-sigs/federation-v2/pkg/client/clientset/versioned"
	"github.com/kubernetes-sigs/federation-v2/pkg/controller/sync/placement"
	"github.com/kubernetes-sigs/federation-v2/pkg/controller/sync/version"
	"github.com/kubernetes-sigs/federation-v2/pkg/controller/util"
	"github.com/kubernetes-sigs/federation-v2/pkg/controller/util/deletionhelper"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

const (
	allClustersKey = "ALL_CLUSTERS"
)

// FederationSyncController synchronizes the state of a federated type
// to clusters that are members of the federation.
type FederationSyncController struct {
	// TODO(marun) add comment
	worker util.ReconcileWorker

	// For triggering reconciliation of all target resources. This is
	// used when a new cluster becomes available.
	clusterDeliverer *util.DelayingDeliverer

	// Contains resources present in members of federation.
	informer util.FederatedInformer
	// For updating members of federation.
	updater util.FederatedUpdater

	// Store for the templates of the federated type
	templateStore cache.Store
	// Informer for the templates of the federated type
	templateController cache.Controller

	// Store for the override directives of the federated type
	overrideStore cache.Store
	// Informer controller for override directives of the federated type
	overrideController cache.Controller

	placementPlugin placement.PlacementPlugin

	// Helper for propagated version comparison for resource types.
	comparisonHelper util.ComparisonHelper

	// Manages propagated versions for the controller
	versionManager *version.VersionManager

	// For events
	eventRecorder record.EventRecorder

	deletionHelper *deletionhelper.DeletionHelper

	clusterAvailableDelay   time.Duration
	clusterUnavailableDelay time.Duration
	smallDelay              time.Duration
	updateTimeout           time.Duration

	typeConfig typeconfig.Interface

	fedClient      fedclientset.Interface
	templateClient util.ResourceClient

	fedNamespace string

	hostClusterFinder util.HostClusterFinder
}

// StartFederationSyncController starts a new sync controller for a type config
func StartFederationSyncController(controllerConfig *util.ControllerConfig, hostClusterFinder util.HostClusterFinder, stopChan <-chan struct{}, typeConfig typeconfig.Interface) error {
	controller, err := newFederationSyncController(controllerConfig, hostClusterFinder, typeConfig)
	if err != nil {
		return err
	}
	if controllerConfig.MinimizeLatency {
		controller.minimizeLatency()
	}
	glog.Infof(fmt.Sprintf("Starting sync controller for %q", typeConfig.GetTemplate().Kind))
	controller.Run(stopChan)
	return nil
}

// newFederationSyncController returns a new sync controller for the configuration
func newFederationSyncController(controllerConfig *util.ControllerConfig, hostClusterFinder util.HostClusterFinder, typeConfig typeconfig.Interface) (*FederationSyncController, error) {
	templateAPIResource := typeConfig.GetTemplate()
	userAgent := fmt.Sprintf("%s-controller", strings.ToLower(templateAPIResource.Kind))

	// Initialize non-dynamic clients first to avoid polluting config
	fedClient, kubeClient, crClient := controllerConfig.AllClients(userAgent)

	pool := dynamic.NewDynamicClientPool(controllerConfig.KubeConfig)

	templateClient, err := util.NewResourceClient(pool, &templateAPIResource)
	if err != nil {
		return nil, err
	}

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: userAgent})

	s := &FederationSyncController{
		clusterAvailableDelay:   controllerConfig.ClusterAvailableDelay,
		clusterUnavailableDelay: controllerConfig.ClusterUnavailableDelay,
		smallDelay:              time.Second * 3,
		updateTimeout:           time.Second * 30,
		eventRecorder:           recorder,
		typeConfig:              typeConfig,
		fedClient:               fedClient,
		templateClient:          templateClient,
		fedNamespace:            controllerConfig.FederationNamespace,
		hostClusterFinder:       hostClusterFinder,
	}

	s.worker = util.NewReconcileWorker(s.reconcile, util.WorkerTiming{
		ClusterSyncDelay: s.clusterAvailableDelay,
	})

	// Build deliverer for triggering cluster reconciliations.
	s.clusterDeliverer = util.NewDelayingDeliverer()

	// Start informers on the resources for the federated type
	enqueueObj := s.worker.EnqueueObject

	targetNamespace := controllerConfig.TargetNamespace

	s.templateStore, s.templateController = util.NewResourceInformer(templateClient, targetNamespace, enqueueObj)

	if overrideAPIResource := typeConfig.GetOverride(); overrideAPIResource != nil {
		client, err := util.NewResourceClient(pool, overrideAPIResource)
		if err != nil {
			return nil, err
		}
		s.overrideStore, s.overrideController = util.NewResourceInformer(client, targetNamespace, enqueueObj)
	}

	placementAPIResource := typeConfig.GetPlacement()
	placementClient, err := util.NewResourceClient(pool, &placementAPIResource)
	if err != nil {
		return nil, err
	}
	targetAPIResource := typeConfig.GetTarget()
	if targetAPIResource.Kind == util.NamespaceKind {
		s.placementPlugin = placement.NewNamespacePlacementPlugin(placementClient, targetNamespace, enqueueObj)
	} else if targetNamespace == metav1.NamespaceAll {
		s.placementPlugin = placement.NewResourcePlacementPlugin(placementClient, targetNamespace, enqueueObj)
	} else {
		// TODO(marun) Source this configuration from the API
		namespacePlacementAPIResource := metav1.APIResource{
			Kind:       "FederatedNamespacePlacement",
			Name:       "federatednamespaceplacements",
			Group:      "core.federation.k8s.io",
			Version:    "v1alpha1",
			Namespaced: true,
		}
		namespacePlacementClient, err := util.NewResourceClient(pool, &namespacePlacementAPIResource)
		if err != nil {
			return nil, err
		}
		s.placementPlugin = placement.NewNamespacedPlacementPlugin(placementClient, namespacePlacementClient, targetNamespace, enqueueObj)
	}

	s.versionManager = version.NewVersionManager(
		fedClient, templateAPIResource.Namespaced, templateAPIResource.Kind, targetAPIResource.Kind, targetNamespace,
	)

	s.comparisonHelper, err = util.NewComparisonHelper(typeConfig.GetComparisonField())
	if err != nil {
		return nil, err
	}

	// Federated informer on the resource type in members of federation.
	s.informer = util.NewFederatedInformer(
		fedClient,
		kubeClient,
		crClient,
		controllerConfig.FederationNamespaces,
		&targetAPIResource,
		func(obj pkgruntime.Object) {
			qualifiedName := util.NewQualifiedName(obj)
			s.worker.EnqueueForRetry(qualifiedName)
		},
		&util.ClusterLifecycleHandlerFuncs{
			ClusterAvailable: func(cluster *fedv1a1.FederatedCluster) {
				// When new cluster becomes available process all the target resources again.
				s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterAvailableDelay))
			},
			// When a cluster becomes unavailable process all the target resources again.
			ClusterUnavailable: func(cluster *fedv1a1.FederatedCluster, _ []interface{}) {
				s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterUnavailableDelay))
			},
		},
	)

	// Federated updater along with Create/Update/Delete operations.
	s.updater = util.NewFederatedUpdater(s.informer, targetAPIResource.Kind, s.updateTimeout, s.eventRecorder,
		func(client util.ResourceClient, rawObj pkgruntime.Object) (string, error) {
			obj := rawObj.(*unstructured.Unstructured)
			createdObj, err := client.Resources(obj.GetNamespace()).Create(obj)
			if err != nil {
				return "", err
			}
			return s.comparisonHelper.GetVersion(createdObj), err
		},
		func(client util.ResourceClient, rawObj pkgruntime.Object) (string, error) {
			obj := rawObj.(*unstructured.Unstructured)
			updatedObj, err := client.Resources(obj.GetNamespace()).Update(obj)
			if err != nil {
				return "", err
			}
			return s.comparisonHelper.GetVersion(updatedObj), err
		},
		func(client util.ResourceClient, obj pkgruntime.Object) (string, error) {
			qualifiedName := util.NewQualifiedName(obj)
			orphanDependents := false
			return "", client.Resources(qualifiedName.Namespace).Delete(qualifiedName.Name, &metav1.DeleteOptions{OrphanDependents: &orphanDependents})
		})

	// TODO(marun) - need to add finalizers to placement and overrides, too

	s.deletionHelper = deletionhelper.NewDeletionHelper(
		// updateObjFunc
		func(rawObj pkgruntime.Object) (pkgruntime.Object, error) {
			obj := rawObj.(*unstructured.Unstructured)
			return templateClient.Resources(obj.GetNamespace()).Update(obj)
		},
		// objNameFunc
		func(obj pkgruntime.Object) string {
			return util.NewQualifiedName(obj).String()
		},
		s.informer,
		s.updater,
	)

	return s, nil
}

// minimizeLatency reduces delays and timeouts to make the controller more responsive (useful for testing).
func (s *FederationSyncController) minimizeLatency() {
	s.clusterAvailableDelay = time.Second
	s.clusterUnavailableDelay = time.Second
	s.smallDelay = 20 * time.Millisecond
	s.updateTimeout = 5 * time.Second
	s.worker.SetDelay(50*time.Millisecond, s.clusterAvailableDelay)
}

func (s *FederationSyncController) Run(stopChan <-chan struct{}) {
	go s.versionManager.Sync(stopChan)
	go s.templateController.Run(stopChan)
	if s.overrideController != nil {
		go s.overrideController.Run(stopChan)
	}
	go s.placementPlugin.Run(stopChan)
	s.informer.Start()
	s.clusterDeliverer.StartWithHandler(func(_ *util.DelayingDelivererItem) {
		s.reconcileOnClusterChange()
	})

	s.worker.Run(stopChan)

	// Ensure all goroutines are cleaned up when the stop channel closes
	go func() {
		<-stopChan
		s.informer.Stop()
		s.clusterDeliverer.Stop()
	}()
}

// Check whether all data stores are in sync. False is returned if any of the informer/stores is not yet
// synced with the corresponding api server.
func (s *FederationSyncController) isSynced() bool {
	if !s.informer.ClustersSynced() {
		glog.V(2).Infof("Cluster list not synced")
		return false
	}
	if !s.placementPlugin.HasSynced() {
		glog.V(2).Infof("Placement not synced")
		return false
	}
	if !s.versionManager.HasSynced() {
		glog.V(2).Infof("Version manager not synced")
		return false
	}

	// TODO(marun) set clusters as ready in the test fixture?
	clusters, err := s.informer.GetReadyClusters()
	if err != nil {
		runtime.HandleError(fmt.Errorf("Failed to get ready clusters: %v", err))
		return false
	}
	if !s.informer.GetTargetStore().ClustersSynced(clusters) {
		return false
	}
	return true
}

// The function triggers reconciliation of all target federated resources.
func (s *FederationSyncController) reconcileOnClusterChange() {
	if !s.isSynced() {
		s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterAvailableDelay))
	}
	for _, obj := range s.templateStore.List() {
		qualifiedName := util.NewQualifiedName(obj.(pkgruntime.Object))
		s.worker.EnqueueWithDelay(qualifiedName, s.smallDelay)
	}
}

func (s *FederationSyncController) reconcile(qualifiedName util.QualifiedName) util.ReconciliationStatus {
	if !s.isSynced() {
		return util.StatusNotSynced
	}

	templateKind := s.typeConfig.GetTemplate().Kind
	key := qualifiedName.String()
	placementName := util.QualifiedName{Namespace: qualifiedName.Namespace, Name: qualifiedName.Name}
	namespace := qualifiedName.Namespace

	targetKind := s.typeConfig.GetTarget().Kind
	if targetKind == util.NamespaceKind {
		namespace = qualifiedName.Name
		// TODO(font): Need a configurable or discoverable list of namespaces
		// to not propagate beyond just the default system namespaces e.g.
		// clusterregistry.
		if isSystemNamespace(s.fedNamespace, namespace) {
			glog.V(4).Infof("Ignoring system namespace %v", namespace)
			return util.StatusAllOK
		}

		// TODO(font): Consider how best to deal with qualifiedName keys for
		// cluster-scoped template resources whose placement and/or override
		// resources are namespace-scoped e.g. Namespaces. For now, update the
		// Namespace template and placement keys depending on if we are
		// reconciling due to a Namespace or FederatedNamespacePlacement
		// update. This insures we will successfully retrieve the Namespace
		// template or placement objects from the cache by using the proper
		// cluster-scoped or namespace-scoped key.
		if qualifiedName.Namespace == "" {
			qualifiedName.Namespace = namespace
			placementName.Namespace = namespace
			glog.V(4).Infof("Received Namespace update for %v. Using placement key %v", key, placementName)
		} else if qualifiedName.Namespace != "" {
			qualifiedName.Namespace = ""
			key = qualifiedName.String()
			glog.V(4).Infof("Received FederatedNamespacePlacement update for %v. Using template key %v", placementName, key)
		}
	}

	glog.V(4).Infof("Starting to reconcile %v %v", templateKind, key)
	startTime := time.Now()
	defer glog.V(4).Infof("Finished reconciling %v %v (duration: %v)", templateKind, key, time.Now().Sub(startTime))

	template, err := s.objFromCache(s.templateStore, templateKind, key)
	if err != nil {
		return util.StatusError
	}
	if template == nil {
		glog.V(4).Infof("No template for %v %v found", templateKind, key)
		return util.StatusAllOK
	}

	if template.GetDeletionTimestamp() != nil {
		err := s.delete(template, templateKind, qualifiedName)
		if err != nil {
			msg := "Failed to delete %s %q: %v"
			args := []interface{}{templateKind, qualifiedName, err}
			runtime.HandleError(fmt.Errorf(msg, args...))
			s.eventRecorder.Eventf(template, corev1.EventTypeWarning, "DeleteFailed", msg, args...)
			return util.StatusError
		}
		return util.StatusAllOK
	}

	glog.V(3).Infof("Ensuring finalizers exist on %s %q", templateKind, key)
	finalizedTemplate, err := s.deletionHelper.EnsureFinalizers(template)
	if err != nil {
		runtime.HandleError(fmt.Errorf("Failed to ensure finalizers for %s %q: %v", templateKind, key, err))
		return util.StatusError
	}
	template = finalizedTemplate.(*unstructured.Unstructured)

	clusterNames, err := s.clusterNames()
	if err != nil {
		runtime.HandleError(fmt.Errorf("Failed to get cluster list: %v", err))
		return util.StatusNotSynced
	}

	selectedClusters, unselectedClusters, err := s.placementPlugin.ComputePlacement(placementName, clusterNames)
	if err != nil {
		runtime.HandleError(fmt.Errorf("Failed to compute placement for %s %q: %v", templateKind, placementName, err))
		return util.StatusError
	}

	var override *unstructured.Unstructured
	if s.overrideStore != nil {
		override, err = s.objFromCache(s.overrideStore, s.typeConfig.GetOverride().Kind, key)
		if err != nil {
			return util.StatusError
		}
	}

	return s.syncToClusters(selectedClusters, unselectedClusters, template, override)
}

func (s *FederationSyncController) rawObjFromCache(store cache.Store, kind, key string) (pkgruntime.Object, error) {
	cachedObj, exist, err := store.GetByKey(key)
	if err != nil {
		wrappedErr := fmt.Errorf("Failed to query %s store for %q: %v", kind, key, err)
		runtime.HandleError(wrappedErr)
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	return cachedObj.(pkgruntime.Object).DeepCopyObject(), nil
}

func (s *FederationSyncController) objFromCache(store cache.Store, kind, key string) (*unstructured.Unstructured, error) {
	obj, err := s.rawObjFromCache(store, kind, key)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, nil
	}
	return obj.(*unstructured.Unstructured), nil
}

func (s *FederationSyncController) clusterNames() ([]string, error) {
	clusters, err := s.informer.GetReadyClusters()
	if err != nil {
		return nil, err
	}
	clusterNames := []string{}
	for _, cluster := range clusters {
		clusterNames = append(clusterNames, cluster.Name)
	}

	return clusterNames, nil
}

// delete deletes the given resource or returns error if the deletion was not complete.
func (s *FederationSyncController) delete(template pkgruntime.Object,
	kind string, qualifiedName util.QualifiedName) error {
	glog.V(3).Infof("Handling deletion of %s %q", kind, qualifiedName)

	_, err := s.deletionHelper.HandleObjectInUnderlyingClusters(template, kind)
	if err != nil {
		return err
	}

	if kind == util.NamespaceKind {
		// Return immediately if we are a namespace as it will be deleted
		// simply by removing its finalizers.
		return nil
	}

	s.versionManager.Delete(qualifiedName)

	err = s.templateClient.Resources(qualifiedName.Namespace).Delete(qualifiedName.Name, nil)
	if err != nil {
		// Its all good if the error is not found error. That means it is deleted already and we do not have to do anything.
		// This is expected when we are processing an update as a result of finalizer deletion.
		// The process that deleted the last finalizer is also going to delete the resource and we do not have to do anything.
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// syncToClusters ensures that the state of the given object is synchronized to
// member clusters.
func (s *FederationSyncController) syncToClusters(selectedClusters, unselectedClusters []string,
	template, override *unstructured.Unstructured) util.ReconciliationStatus {

	templateKind := s.typeConfig.GetTemplate().Kind
	key := util.NewQualifiedName(template).String()

	glog.V(3).Infof("Syncing %s %q in underlying clusters, selected clusters are: %s, unselected clusters are: %s",
		templateKind, key, selectedClusters, unselectedClusters)

	operations, err := s.clusterOperations(selectedClusters, unselectedClusters, template, override, key)
	if err != nil {
		s.eventRecorder.Eventf(template, corev1.EventTypeWarning, "FedClusterOperationsError",
			"Error obtaining sync operations for %s: %s error: %s", templateKind, key, err.Error())
		return util.StatusError
	}

	if len(operations) == 0 {
		return util.StatusAllOK
	}

	// TODO(marun) raise the visibility of operationErrors to aid in debugging
	versionMap, operationErrors := s.updater.Update(operations)

	s.versionManager.Update(template, override, selectedClusters, versionMap)

	if len(operationErrors) > 0 {
		runtime.HandleError(fmt.Errorf("Failed to execute updates for %s %q: %v", templateKind,
			key, operationErrors))
		return util.StatusError
	}

	return util.StatusAllOK
}

// clusterOperations returns the list of operations needed to synchronize the
// state of the given object to the provided clusters.
func (s *FederationSyncController) clusterOperations(selectedClusters, unselectedClusters []string,
	template, override *unstructured.Unstructured, key string) ([]util.FederatedOperation, error) {

	operations := make([]util.FederatedOperation, 0)

	clusterOverrides, err := util.GetClusterOverrides(s.typeConfig, override)
	if err != nil {
		templateKind := s.typeConfig.GetTemplate().Kind
		return nil, fmt.Errorf("Failed to marshall cluster overrides for %s %q: %v", templateKind, key, err)
	}

	versionMap := s.versionManager.Get(template, override)

	targetKind := s.typeConfig.GetTarget().Kind
	for _, clusterName := range selectedClusters {
		// Federated operations are not performed on namespaces in the host cluster.
		if targetKind == util.NamespaceKind {
			isHostCluster, err := s.hostClusterFinder.IsHostCluster(clusterName)
			if err != nil {
				return nil, fmt.Errorf("Failed to determine whether %q was the host cluster: %v", clusterName, err)
			}
			if isHostCluster {
				continue
			}
		}

		// TODO(marun) Create the desired object only if needed
		desiredObj, err := s.objectForCluster(template, clusterOverrides, clusterName)
		if err != nil {
			return nil, err
		}

		// TODO(marun) Wait until result of add operation has reached
		// the target store before attempting subsequent operations?
		// Otherwise the object won't be found but an add operation
		// will fail with AlreadyExists.
		clusterObj, found, err := s.informer.GetTargetStore().GetByKey(clusterName, key)
		if err != nil {
			wrappedErr := fmt.Errorf("Failed to get %s %q from cluster %q: %v", targetKind, key, clusterName, err)
			runtime.HandleError(wrappedErr)
			return nil, wrappedErr
		}

		var operationType util.FederatedOperationType = ""

		if found {
			clusterObj := clusterObj.(*unstructured.Unstructured)

			desiredObj, err = s.objectForUpdateOp(desiredObj, clusterObj)
			if err != nil {
				wrappedErr := fmt.Errorf("Failed to determine desired object %s %q for cluster %q: %v", targetKind, key, clusterName, err)
				runtime.HandleError(wrappedErr)
				return nil, wrappedErr
			}

			version, ok := versionMap[clusterName]
			if !ok {
				// No target version recorded for template+override version
				operationType = util.OperationTypeUpdate
			} else {
				targetVersion := s.comparisonHelper.GetVersion(clusterObj)

				// Check if versions don't match. If they match then check its
				// ObjectMeta which only applies to resources where Generation
				// is used to track versions because Generation is only updated
				// when Spec changes.
				if version != targetVersion {
					operationType = util.OperationTypeUpdate
				} else if !s.comparisonHelper.Equivalent(desiredObj, clusterObj) {
					// TODO(marun) Since only the metadata is compared
					// in the call to Equivalent(), use the template
					// to avoid having to worry about overrides.
					operationType = util.OperationTypeUpdate
				}
			}
		} else {
			operationType = util.OperationTypeAdd
		}

		if len(operationType) > 0 {
			operations = append(operations, util.FederatedOperation{
				Type:        operationType,
				Obj:         desiredObj,
				ClusterName: clusterName,
				Key:         key,
			})
		}
	}

	for _, clusterName := range unselectedClusters {
		// Federated operations are not performed on namespaces in the host cluster.
		if targetKind == util.NamespaceKind {
			isHostCluster, err := s.hostClusterFinder.IsHostCluster(clusterName)
			if err != nil {
				return nil, fmt.Errorf("Failed to determine whether %q was the host cluster: %v", clusterName, err)
			}
			if isHostCluster {
				continue
			}
		}

		clusterObj, found, err := s.informer.GetTargetStore().GetByKey(clusterName, key)
		if err != nil {
			wrappedErr := fmt.Errorf("Failed to get %s %q from cluster %q: %v", targetKind, key, clusterName, err)
			runtime.HandleError(wrappedErr)
			return nil, wrappedErr
		}
		if found {
			operations = append(operations, util.FederatedOperation{
				Type:        util.OperationTypeDelete,
				Obj:         clusterObj.(pkgruntime.Object),
				ClusterName: clusterName,
				Key:         key,
			})
		}
	}

	return operations, nil
}

// TODO(marun) Marshall the template once per reconcile, not per-cluster
func (s *FederationSyncController) objectForCluster(template *unstructured.Unstructured, clusterOverrides util.ClusterOverrides, clusterName string) (*unstructured.Unstructured, error) {
	// Federation of namespaces uses Namespace resources as the
	// template for resource creation in member clusters. All other
	// federated types rely on a template type distinct from the
	// target type.
	//
	// Namespace is the only type that can contain other resources,
	// and adding a federation-specific container type would be
	// difficult or impossible. This implies that federation
	// primitives need to exist in regular namespaces.
	//
	// TODO(marun) Ensure this is reflected in documentation
	targetKind := s.typeConfig.GetTarget().Kind
	obj := &unstructured.Unstructured{}
	if targetKind == util.NamespaceKind {
		metadata, ok, err := unstructured.NestedMap(template.Object, "metadata")
		if err != nil {
			return nil, fmt.Errorf("Error retrieving namespace metadata: %s", err)
		}
		if !ok {
			return nil, fmt.Errorf("Unable to retrieve namespace metadata")
		}
		// Retain only the target fields from the template
		targetFields := sets.NewString("name", "namespace", "labels", "annotations")
		for key := range metadata {
			if !targetFields.Has(key) {
				delete(metadata, key)
			}
		}
		obj.Object = make(map[string]interface{})
		obj.Object["metadata"] = metadata
	} else {
		var ok bool
		var err error
		obj.Object, ok, err = unstructured.NestedMap(template.Object, "spec", "template")
		if err != nil {
			return nil, fmt.Errorf("Error retrieving template body: %v", err)
		}
		if !ok {
			return nil, fmt.Errorf("Unable to retrieve template body")
		}
		// Avoid having to duplicate these details in the template or have
		// the name/namespace vary between the federation api and member
		// clusters.
		//
		// TODO(marun) this should be documented
		obj.SetName(template.GetName())
		obj.SetNamespace(template.GetNamespace())
		targetApiResource := s.typeConfig.GetTarget()
		obj.SetKind(targetApiResource.Kind)
		obj.SetAPIVersion(fmt.Sprintf("%s/%s", targetApiResource.Group, targetApiResource.Version))
	}

	overrides, ok := clusterOverrides[clusterName]
	if ok {
		for _, override := range overrides {
			unstructured.SetNestedField(obj.Object, override.FieldValue, override.Path...)
		}
	}

	return obj, nil
}

// TODO(marun) Support webhooks for custom update behavior
func (s *FederationSyncController) objectForUpdateOp(desiredObj, clusterObj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	// Pass the same ResourceVersion as in the cluster object for update operation, otherwise operation will fail.
	desiredObj.SetResourceVersion(clusterObj.GetResourceVersion())

	if s.typeConfig.GetTarget().Kind == util.ServiceKind {
		return serviceForUpdateOp(desiredObj, clusterObj)
	}
	return desiredObj, nil
}

func serviceForUpdateOp(desiredObj, clusterObj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	// ClusterIP and NodePort are allocated to Service by cluster, so retain the same if any while updating

	// Retain clusterip
	clusterIP, ok, err := unstructured.NestedString(clusterObj.Object, "spec", "clusterIP")
	if err != nil {
		return nil, fmt.Errorf("Error retrieving clusterIP from cluster service: %v", err)
	}
	// !ok could indicate that a cluster ip was not assigned
	if ok && clusterIP != "" {
		err := unstructured.SetNestedField(desiredObj.Object, clusterIP, "spec", "clusterIP")
		if err != nil {
			return nil, fmt.Errorf("Error setting clusterIP for service: %v", err)
		}
	}

	// Retain nodeports
	clusterPorts, ok, err := unstructured.NestedSlice(clusterObj.Object, "spec", "ports")
	if err != nil {
		return nil, fmt.Errorf("Error retrieving ports from cluster service: %v", err)
	}
	if !ok {
		return desiredObj, nil
	}
	var desiredPorts []interface{}
	desiredPorts, ok, err = unstructured.NestedSlice(desiredObj.Object, "spec", "ports")
	if err != nil {
		return nil, fmt.Errorf("Error retrieving ports from service: %v", err)
	}
	if !ok {
		desiredPorts = []interface{}{}
	}
	for desiredIndex := range desiredPorts {
		for clusterIndex := range clusterPorts {
			fPort := desiredPorts[desiredIndex].(map[string]interface{})
			cPort := clusterPorts[clusterIndex].(map[string]interface{})
			if !(fPort["name"] == cPort["name"] && fPort["protocol"] == cPort["protocol"] && fPort["port"] == cPort["port"]) {
				continue
			}
			nodePort, ok := cPort["nodePort"]
			if ok {
				cPort["nodePort"] = nodePort
			}
		}
	}
	err = unstructured.SetNestedSlice(desiredObj.Object, desiredPorts, "spec", "ports")
	if err != nil {
		return nil, fmt.Errorf("Error setting ports for service: %v", err)
	}

	return desiredObj, nil
}

// TODO (font): Externalize this list to a package var to allow it to be
// configurable.
func isSystemNamespace(fedNamespace, namespace string) bool {
	switch namespace {
	case "kube-system", fedNamespace:
		return true
	default:
		return false
	}
}

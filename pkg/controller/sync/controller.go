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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"

	"sigs.k8s.io/kubefed/pkg/apis/core/typeconfig"
	fedv1b1 "sigs.k8s.io/kubefed/pkg/apis/core/v1beta1"
	genericclient "sigs.k8s.io/kubefed/pkg/client/generic"
	"sigs.k8s.io/kubefed/pkg/controller/sync/dispatch"
	"sigs.k8s.io/kubefed/pkg/controller/sync/status"
	"sigs.k8s.io/kubefed/pkg/controller/util"
	finalizersutil "sigs.k8s.io/kubefed/pkg/controller/util/finalizers"
)

const (
	allClustersKey = "ALL_CLUSTERS"

	// If this finalizer is present on a federated resource, the sync
	// controller will have the opportunity to perform pre-deletion operations
	// (like deleting managed resources from member clusters).
	FinalizerSyncController = "kubefed.io/sync-controller"
)

// KubeFedSyncController synchronizes the state of federated resources
// in the host cluster with resources in member clusters.
type KubeFedSyncController struct {
	// TODO(marun) add comment
	worker util.ReconcileWorker

	// For triggering reconciliation of all target resources. This is
	// used when a new cluster becomes available.
	clusterDeliverer *util.DelayingDeliverer

	// Informer for resources in member clusters
	informer util.FederatedInformer

	// For events
	eventRecorder record.EventRecorder

	clusterAvailableDelay   time.Duration
	clusterUnavailableDelay time.Duration
	smallDelay              time.Duration

	typeConfig typeconfig.Interface

	fedAccessor FederatedResourceAccessor

	hostClusterClient genericclient.Client

	skipAdoptingResources bool

	limitedScope bool

	scaleTesting bool
}

// StartKubeFedSyncController starts a new sync controller for a type config
func StartKubeFedSyncController(controllerConfig *util.ControllerConfig, stopChan <-chan struct{}, typeConfig typeconfig.Interface, fedNamespaceAPIResource *metav1.APIResource) error {
	controller, err := newKubeFedSyncController(controllerConfig, typeConfig, fedNamespaceAPIResource)
	if err != nil {
		return err
	}
	if controllerConfig.MinimizeLatency {
		controller.minimizeLatency()
	}
	klog.Infof(fmt.Sprintf("Starting sync controller for %q", typeConfig.GetFederatedType().Kind))
	controller.Run(stopChan)
	return nil
}

// newKubeFedSyncController returns a new sync controller for the configuration
func newKubeFedSyncController(controllerConfig *util.ControllerConfig, typeConfig typeconfig.Interface, fedNamespaceAPIResource *metav1.APIResource) (*KubeFedSyncController, error) {
	federatedTypeAPIResource := typeConfig.GetFederatedType()
	userAgent := fmt.Sprintf("%s-controller", strings.ToLower(federatedTypeAPIResource.Kind))

	// Initialize non-dynamic clients first to avoid polluting config
	client := genericclient.NewForConfigOrDieWithUserAgent(controllerConfig.KubeConfig, userAgent)
	kubeClient := kubeclient.NewForConfigOrDie(controllerConfig.KubeConfig)

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: userAgent})

	s := &KubeFedSyncController{
		clusterAvailableDelay:   controllerConfig.ClusterAvailableDelay,
		clusterUnavailableDelay: controllerConfig.ClusterUnavailableDelay,
		smallDelay:              time.Second * 3,
		eventRecorder:           recorder,
		typeConfig:              typeConfig,
		hostClusterClient:       client,
		skipAdoptingResources:   controllerConfig.SkipAdoptingResources,
		limitedScope:            controllerConfig.LimitedScope(),
		scaleTesting:            controllerConfig.ScaleTesting,
	}

	s.worker = util.NewReconcileWorker(s.reconcile, util.WorkerTiming{
		ClusterSyncDelay: s.clusterAvailableDelay,
	})

	// Build deliverer for triggering cluster reconciliations.
	s.clusterDeliverer = util.NewDelayingDeliverer()

	targetAPIResource := typeConfig.GetTargetType()

	// Federated informer for resources in member clusters
	var err error
	s.informer, err = util.NewFederatedInformer(
		controllerConfig,
		client,
		&targetAPIResource,
		func(obj pkgruntime.Object) {
			qualifiedName := util.NewQualifiedName(obj)
			s.worker.EnqueueForRetry(qualifiedName)
		},
		&util.ClusterLifecycleHandlerFuncs{
			ClusterAvailable: func(cluster *fedv1b1.KubeFedCluster) {
				// When new cluster becomes available process all the target resources again.
				s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterAvailableDelay))
			},
			// When a cluster becomes unavailable process all the target resources again.
			ClusterUnavailable: func(cluster *fedv1b1.KubeFedCluster, _ []interface{}) {
				s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterUnavailableDelay))
			},
		},
	)
	if err != nil {
		return nil, err
	}

	s.fedAccessor, err = NewFederatedResourceAccessor(
		controllerConfig, typeConfig, fedNamespaceAPIResource,
		client, s.worker.EnqueueObject, recorder)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// minimizeLatency reduces delays and timeouts to make the controller more responsive (useful for testing).
func (s *KubeFedSyncController) minimizeLatency() {
	s.clusterAvailableDelay = time.Second
	s.clusterUnavailableDelay = time.Second
	s.smallDelay = 20 * time.Millisecond
	s.worker.SetDelay(50*time.Millisecond, s.clusterAvailableDelay)
}

func (s *KubeFedSyncController) Run(stopChan <-chan struct{}) {
	s.fedAccessor.Run(stopChan)
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
func (s *KubeFedSyncController) isSynced() bool {
	if !s.informer.ClustersSynced() {
		klog.V(2).Infof("Cluster list not synced")
		return false
	}
	if !s.fedAccessor.HasSynced() {
		// The fed accessor will have logged why sync is not yet
		// complete.
		return false
	}

	// TODO(marun) set clusters as ready in the test fixture?
	clusters, err := s.informer.GetReadyClusters()
	if err != nil {
		runtime.HandleError(errors.Wrap(err, "Failed to get ready clusters"))
		return false
	}
	if !s.informer.GetTargetStore().ClustersSynced(clusters) {
		return false
	}
	return true
}

// The function triggers reconciliation of all target federated resources.
func (s *KubeFedSyncController) reconcileOnClusterChange() {
	if !s.isSynced() {
		s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterAvailableDelay))
	}
	s.fedAccessor.VisitFederatedResources(func(obj interface{}) {
		qualifiedName := util.NewQualifiedName(obj.(pkgruntime.Object))
		s.worker.EnqueueWithDelay(qualifiedName, s.smallDelay)
	})
}

func (s *KubeFedSyncController) reconcile(qualifiedName util.QualifiedName) util.ReconciliationStatus {
	if !s.isSynced() {
		return util.StatusNotSynced
	}

	kind := s.typeConfig.GetFederatedType().Kind

	fedResource, possibleOrphan, err := s.fedAccessor.FederatedResource(qualifiedName)
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "Error creating FederatedResource helper for %s %q", kind, qualifiedName))
		return util.StatusError
	}
	if possibleOrphan {
		apiResource := s.typeConfig.GetTargetType()
		gvk := apiResourceToGVK(&apiResource)
		klog.V(2).Infof("Ensuring the removal of the label %q from %s %q in member clusters.", util.ManagedByKubeFedLabelKey, gvk.Kind, qualifiedName)
		err = s.removeManagedLabel(gvk, qualifiedName)
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to remove the label %q from %s %q in member clusters", util.ManagedByKubeFedLabelKey, gvk.Kind, qualifiedName)
			runtime.HandleError(wrappedErr)
			return util.StatusError
		}

		return util.StatusAllOK
	}
	if fedResource == nil {
		return util.StatusAllOK
	}

	key := fedResource.FederatedName().String()

	klog.V(4).Infof("Starting to reconcile %s %q", kind, key)
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished reconciling %s %q (duration: %v)", kind, key, time.Since(startTime))
	}()

	if fedResource.Object().GetDeletionTimestamp() != nil {
		return s.ensureDeletion(fedResource)
	}
	err = s.ensureFinalizer(fedResource)
	if err != nil {
		fedResource.RecordError("EnsureFinalizerError", errors.Wrap(err, "Failed to ensure finalizer"))
		return util.StatusError
	}

	return s.syncToClusters(fedResource)
}

// syncToClusters ensures that the state of the given object is
// synchronized to member clusters.
func (s *KubeFedSyncController) syncToClusters(fedResource FederatedResource) util.ReconciliationStatus {
	clusters, err := s.informer.GetClusters()
	if err != nil {
		fedResource.RecordError(string(status.ClusterRetrievalFailed), errors.Wrap(err, "Failed to retrieve list of clusters"))
		return s.setFederatedStatus(fedResource, status.ClusterRetrievalFailed, nil)
	}

	selectedClusterNames, err := fedResource.ComputePlacement(clusters)
	if err != nil {
		fedResource.RecordError(string(status.ComputePlacementFailed), errors.Wrap(err, "Failed to compute placement"))
		return s.setFederatedStatus(fedResource, status.ComputePlacementFailed, nil)
	}

	kind := fedResource.TargetKind()
	key := fedResource.TargetName().String()
	klog.V(4).Infof("Ensuring %s %q in clusters: %s", kind, key, strings.Join(selectedClusterNames.List(), ","))

	dispatcher := dispatch.NewManagedDispatcher(s.informer.GetClientForCluster, fedResource, s.skipAdoptingResources)

	for _, cluster := range clusters {
		clusterName := cluster.Name
		selectedCluster := selectedClusterNames.Has(clusterName)

		if !util.IsClusterReady(&cluster.Status) {
			if selectedCluster {
				// Cluster state only needs to be reported in resource
				// status for clusters selected for placement.
				err := errors.New("Cluster not ready")
				dispatcher.RecordClusterError(status.ClusterNotReady, clusterName, err)
			}
			continue
		}

		rawClusterObj, _, err := s.informer.GetTargetStore().GetByKey(clusterName, key)
		if err != nil {
			wrappedErr := errors.Wrap(err, "Failed to retrieve cached cluster object")
			dispatcher.RecordClusterError(status.CachedRetrievalFailed, clusterName, wrappedErr)
			continue
		}

		var clusterObj *unstructured.Unstructured
		if rawClusterObj != nil {
			clusterObj = rawClusterObj.(*unstructured.Unstructured)
		}

		// Resource should not exist in the named cluster
		if !selectedCluster {
			if clusterObj == nil {
				// Resource does not exist in the cluster
				continue
			}
			if clusterObj.GetDeletionTimestamp() != nil {
				// Resource is marked for deletion
				dispatcher.RecordStatus(clusterName, status.WaitingForRemoval)
				continue
			}
			if fedResource.IsNamespaceInHostCluster(clusterObj) {
				// Host cluster namespace needs to have the managed
				// label removed so it won't be cached anymore.
				dispatcher.RemoveManagedLabel(clusterName, clusterObj)
			} else {
				dispatcher.Delete(clusterName)
			}
			continue
		}

		// Resource should appear in the named cluster

		// TODO(marun) Consider waiting until the result of resource
		// creation has reached the target store before attempting
		// subsequent operations.  Otherwise the object won't be found
		// but an add operation will fail with AlreadyExists.
		if clusterObj == nil {
			dispatcher.Create(clusterName)
		} else {
			dispatcher.Update(clusterName, clusterObj)
		}
	}
	_, timeoutErr := dispatcher.Wait()
	if timeoutErr != nil {
		fedResource.RecordError("OperationTimeoutError", timeoutErr)
	}

	// Write updated versions to the API.
	updatedVersionMap := dispatcher.VersionMap()
	err = fedResource.UpdateVersions(selectedClusterNames.List(), updatedVersionMap)
	if err != nil {
		// Versioning of federated resources is an optimization to
		// avoid unnecessary updates, and failure to record version
		// information does not indicate a failure of propagation.
		runtime.HandleError(err)
	}

	collectedStatus := dispatcher.CollectedStatus()
	return s.setFederatedStatus(fedResource, status.AggregateSuccess, &collectedStatus)
}

func (s *KubeFedSyncController) setFederatedStatus(fedResource FederatedResource,
	reason status.AggregateReason, collectedStatus *status.CollectedPropagationStatus) util.ReconciliationStatus {

	if collectedStatus == nil {
		collectedStatus = &status.CollectedPropagationStatus{}
	}

	kind := fedResource.FederatedKind()
	name := fedResource.FederatedName()
	obj := fedResource.Object()

	// Only a single reason for propagation failure is reported at any one time, so only report
	// NamespaceNotFederated if no other explicit error has been indicated.
	if reason == status.AggregateSuccess {
		// For a cluster-scoped control plane, report when the containing namespace of a federated
		// resource is not federated.  The KubeFed system namespace is implicitly federated in a
		// namespace-scoped control plane.
		if !s.limitedScope && fedResource.NamespaceNotFederated() {
			reason = status.NamespaceNotFederated
		}
	}

	// If the underlying resource has changed, attempt to retrieve and
	// update it repeatedly.
	err := wait.PollImmediate(1*time.Second, 5*time.Second, func() (bool, error) {
		if updateRequired, err := status.SetFederatedStatus(obj, reason, *collectedStatus); err != nil {
			return false, errors.Wrapf(err, "failed to set the status")
		} else if !updateRequired {
			klog.V(4).Infof("No status update necessary for %s %q", kind, name)
			return true, nil
		}

		err := s.hostClusterClient.UpdateStatus(context.TODO(), obj)
		if err == nil {
			return true, nil
		}
		if apierrors.IsConflict(err) {
			klog.V(2).Infof("Failed to set propagation status for %s %q due to conflict (will retry): %v.", kind, name, err)
			err := s.hostClusterClient.Get(context.TODO(), obj, obj.GetNamespace(), obj.GetName())
			if err != nil {
				return false, errors.Wrapf(err, "failed to retrieve resource")
			}
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to update resource")
	})
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "failed to set propagation status for %s %q", kind, name))
		return util.StatusError
	}

	return util.StatusAllOK
}

func (s *KubeFedSyncController) ensureDeletion(fedResource FederatedResource) util.ReconciliationStatus {
	fedResource.DeleteVersions()

	key := fedResource.FederatedName().String()
	kind := fedResource.FederatedKind()

	klog.V(2).Infof("Ensuring deletion of %s %q", kind, key)

	obj := fedResource.Object()

	finalizers := sets.NewString(obj.GetFinalizers()...)
	if !finalizers.Has(FinalizerSyncController) {
		klog.V(2).Infof("%s %q does not have the %q finalizer. Nothing to do.", kind, key, FinalizerSyncController)
		return util.StatusAllOK
	}

	if util.IsOrphaningEnabled(obj) {
		klog.V(2).Infof("Found %q annotation on %s %q. Removing the finalizer.",
			util.OrphanManagedResourcesAnnotation, kind, key)
		err := s.removeFinalizer(fedResource)
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to remove finalizer %q from %s %q", FinalizerSyncController, kind, key)
			runtime.HandleError(wrappedErr)
			return util.StatusError
		}
		klog.V(2).Infof("Initiating the removal of the label %q from resources previously managed by %s %q.", util.ManagedByKubeFedLabelKey, kind, key)
		err = s.removeManagedLabel(fedResource.TargetGVK(), fedResource.TargetName())
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to remove the label %q from all resources previously managed by %s %q", util.ManagedByKubeFedLabelKey, kind, key)
			runtime.HandleError(wrappedErr)
			return util.StatusError
		}
		return util.StatusAllOK
	}

	klog.V(2).Infof("Deleting resources managed by %s %q from member clusters.", kind, key)
	recheckRequired, err := s.deleteFromClusters(fedResource)
	if err != nil {
		wrappedErr := errors.Wrapf(err, "failed to delete %s %q", kind, key)
		runtime.HandleError(wrappedErr)
		return util.StatusError
	}
	if recheckRequired {
		return util.StatusNeedsRecheck
	}
	return util.StatusAllOK
}

// removeManagedLabel attempts to remove the managed label from
// resources with the given name in member clusters.
func (s *KubeFedSyncController) removeManagedLabel(gvk schema.GroupVersionKind, qualifiedName util.QualifiedName) error {
	ok, err := s.handleDeletionInClusters(gvk, qualifiedName, func(dispatcher dispatch.UnmanagedDispatcher, clusterName string, clusterObj *unstructured.Unstructured) {
		if clusterObj.GetDeletionTimestamp() != nil {
			return
		}

		dispatcher.RemoveManagedLabel(clusterName, clusterObj)
	})
	if err != nil {
		return err
	}
	if !ok {
		return errors.Errorf("failed to remove the label from resources in one or more clusters.")
	}
	return nil
}

func (s *KubeFedSyncController) deleteFromClusters(fedResource FederatedResource) (bool, error) {
	gvk := fedResource.TargetGVK()
	qualifiedName := fedResource.TargetName()

	remainingClusters := []string{}
	ok, err := s.handleDeletionInClusters(gvk, qualifiedName, func(dispatcher dispatch.UnmanagedDispatcher, clusterName string, clusterObj *unstructured.Unstructured) {
		// If the containing namespace of a FederatedNamespace is
		// marked for deletion, it is impossible to require the
		// removal of the namespace in advance of removal of the sync
		// controller finalizer.  Return immediately and avoid
		// including the cluster in the list of remaining clusters.
		if fedResource.IsNamespaceInHostCluster(clusterObj) && clusterObj.GetDeletionTimestamp() != nil {
			return
		}

		remainingClusters = append(remainingClusters, clusterName)

		// Avoid attempting any operation on a deleted resource.
		if clusterObj.GetDeletionTimestamp() != nil {
			return
		}

		if fedResource.IsNamespaceInHostCluster(clusterObj) {
			// Creation or deletion of namespaces in the host cluster
			// is not the responsibility of the sync controller.
			// Removing the managed label will ensure a host cluster
			// namespace is no longer cached.
			dispatcher.RemoveManagedLabel(clusterName, clusterObj)
		} else {
			dispatcher.Delete(clusterName)
		}
	})
	if err != nil {
		return false, err
	}
	if !ok {
		return false, errors.Errorf("failed to remove managed resources from one or more clusters.")
	}
	if len(remainingClusters) > 0 {
		fedKind := fedResource.FederatedKind()
		fedName := fedResource.FederatedName()
		klog.V(2).Infof("Waiting for resources managed by %s %q to be removed from the following clusters: %s", fedKind, fedName, strings.Join(remainingClusters, ", "))
		return true, nil
	}
	err = s.ensureRemovedOrUnmanaged(fedResource)
	if err != nil {
		return false, errors.Wrapf(err, "failed to verify that managed resources no longer exist in any cluster")
	}
	// Managed resources no longer exist in any member cluster
	return false, s.removeFinalizer(fedResource)
}

// ensureRemovedOrUnmanaged ensures that no resources in member
// clusters that could be managed by the given federated resources are
// present or labeled as managed.  The checks are performed without
// the informer to cover the possibility that the resources have not
// yet been cached.
func (s *KubeFedSyncController) ensureRemovedOrUnmanaged(fedResource FederatedResource) error {
	clusters, err := s.informer.GetClusters()
	if err != nil {
		return errors.Wrap(err, "failed to get a list of clusters")
	}

	dispatcher := dispatch.NewCheckUnmanagedDispatcher(s.informer.GetClientForCluster, fedResource.TargetGVK(), fedResource.TargetName(), s.scaleTesting)
	unreadyClusters := []string{}
	for _, cluster := range clusters {
		if !util.IsClusterReady(&cluster.Status) {
			unreadyClusters = append(unreadyClusters, cluster.Name)
			continue
		}
		dispatcher.CheckRemovedOrUnlabeled(cluster.Name, fedResource.IsNamespaceInHostCluster)
	}
	ok, timeoutErr := dispatcher.Wait()
	if timeoutErr != nil {
		return timeoutErr
	}
	if len(unreadyClusters) > 0 {
		return errors.Errorf("the following clusters were not ready: %s", strings.Join(unreadyClusters, ", "))
	}
	if !ok {
		return errors.Errorf("one or more checks failed")
	}
	return nil
}

// handleDeletionInClusters invokes the provided deletion handler for
// each managed resource in member clusters.
func (s *KubeFedSyncController) handleDeletionInClusters(gvk schema.GroupVersionKind, qualifiedName util.QualifiedName,
	deletionFunc func(dispatcher dispatch.UnmanagedDispatcher, clusterName string, clusterObj *unstructured.Unstructured)) (bool, error) {

	clusters, err := s.informer.GetClusters()
	if err != nil {
		return false, errors.Wrap(err, "failed to get a list of clusters")
	}

	dispatcher := dispatch.NewUnmanagedDispatcher(s.informer.GetClientForCluster, gvk, qualifiedName, s.scaleTesting)
	key := qualifiedName.String()
	retrievalFailureClusters := []string{}
	unreadyClusters := []string{}
	for _, cluster := range clusters {
		clusterName := cluster.Name

		if !util.IsClusterReady(&cluster.Status) {
			unreadyClusters = append(unreadyClusters, clusterName)
			continue
		}

		if s.scaleTesting {
			// The namespace of the resource is the name of the
			// cluster when scale testing.
			key = util.QualifiedName{
				Namespace: clusterName,
				Name:      qualifiedName.Name,
			}.String()
		}

		rawClusterObj, _, err := s.informer.GetTargetStore().GetByKey(clusterName, key)
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to retrieve %s %q for cluster %q", gvk.Kind, key, clusterName)
			runtime.HandleError(wrappedErr)
			retrievalFailureClusters = append(retrievalFailureClusters, clusterName)
			continue
		}
		if rawClusterObj == nil {
			continue
		}
		clusterObj := rawClusterObj.(*unstructured.Unstructured)
		deletionFunc(dispatcher, clusterName, clusterObj)
	}
	ok, timeoutErr := dispatcher.Wait()
	if timeoutErr != nil {
		return false, timeoutErr
	}
	if len(retrievalFailureClusters) > 0 {
		return false, errors.Errorf("failed to retrieve a managed resource for the following cluster(s): %s", strings.Join(retrievalFailureClusters, ", "))
	}
	if len(unreadyClusters) > 0 {
		return false, errors.Errorf("the following clusters were not ready: %s", strings.Join(unreadyClusters, ", "))
	}
	return ok, nil
}

func (s *KubeFedSyncController) ensureFinalizer(fedResource FederatedResource) error {
	obj := fedResource.Object()
	isUpdated, err := finalizersutil.AddFinalizers(obj, sets.NewString(FinalizerSyncController))
	if err != nil || !isUpdated {
		return err
	}
	klog.V(2).Infof("Adding finalizer %s to %s %q", FinalizerSyncController, fedResource.FederatedKind(), fedResource.FederatedName())
	return s.hostClusterClient.Update(context.TODO(), obj)
}

func (s *KubeFedSyncController) removeFinalizer(fedResource FederatedResource) error {
	obj := fedResource.Object()
	isUpdated, err := finalizersutil.RemoveFinalizers(obj, sets.NewString(FinalizerSyncController))
	if err != nil || !isUpdated {
		return err
	}
	klog.V(2).Infof("Removing finalizer %s from %s %q", FinalizerSyncController, fedResource.FederatedKind(), fedResource.FederatedName())
	return s.hostClusterClient.Update(context.TODO(), obj)
}

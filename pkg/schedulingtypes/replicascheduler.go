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

package schedulingtypes

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kubefed/pkg/apis/core/typeconfig"
	fedschedulingv1a1 "sigs.k8s.io/kubefed/pkg/apis/scheduling/v1alpha1"
	genericclient "sigs.k8s.io/kubefed/pkg/client/generic"
	ctlutil "sigs.k8s.io/kubefed/pkg/controller/util"
	"sigs.k8s.io/kubefed/pkg/controller/util/planner"
	"sigs.k8s.io/kubefed/pkg/controller/util/podanalyzer"
)

const (
	RSPKind = "ReplicaSchedulingPreference"
)

func init() {
	schedulingType := SchedulingType{
		Kind:             RSPKind,
		SchedulerFactory: NewReplicaScheduler,
	}
	RegisterSchedulingType("deployments.apps", schedulingType)
	RegisterSchedulingType("replicasets.apps", schedulingType)
}

type ReplicaScheduler struct {
	controllerConfig *ctlutil.ControllerConfig

	eventHandlers SchedulerEventHandlers

	plugins *ctlutil.SafeMap

	client      genericclient.Client
	podInformer ctlutil.FederatedInformer
}

func NewReplicaScheduler(controllerConfig *ctlutil.ControllerConfig, eventHandlers SchedulerEventHandlers) (Scheduler, error) {
	client := genericclient.NewForConfigOrDieWithUserAgent(controllerConfig.KubeConfig, "replica-scheduler")
	scheduler := &ReplicaScheduler{
		plugins:          ctlutil.NewSafeMap(),
		controllerConfig: controllerConfig,
		eventHandlers:    eventHandlers,
		client:           client,
	}

	// TODO: Update this to use a typed client from single target informer.
	// As of now we have a separate informer for pods, whereas all we need
	// is a typed client.
	// We ignore the pod events in this informer from clusters.
	var err error
	scheduler.podInformer, err = ctlutil.NewFederatedInformer(
		controllerConfig,
		client,
		PodResource,
		func(pkgruntime.Object) {},
		eventHandlers.ClusterLifecycleHandlers,
	)
	if err != nil {
		return nil, err
	}

	return scheduler, nil
}

func (s *ReplicaScheduler) SchedulingKind() string {
	return RSPKind
}

func (s *ReplicaScheduler) StartPlugin(typeConfig typeconfig.Interface) error {
	kind := typeConfig.GetFederatedType().Kind
	// TODO(marun) Return an error if the kind is not supported

	plugin, err := NewPlugin(s.controllerConfig, s.eventHandlers, typeConfig)
	if err != nil {
		return errors.Wrapf(err, "Failed to initialize replica scheduling plugin for %q", kind)
	}

	plugin.Start()
	s.plugins.Store(kind, plugin)

	return nil
}

func (s *ReplicaScheduler) StopPlugin(kind string) {
	plugin, ok := s.plugins.Get(kind)
	if !ok {
		return
	}

	plugin.(*Plugin).Stop()
	s.plugins.Delete(kind)
}

func (s *ReplicaScheduler) ObjectType() pkgruntime.Object {
	return &fedschedulingv1a1.ReplicaSchedulingPreference{}
}

func (s *ReplicaScheduler) Start() {
	s.podInformer.Start()
}

func (s *ReplicaScheduler) HasSynced() bool {
	for _, plugin := range s.plugins.GetAll() {
		if !plugin.(*Plugin).HasSynced() {
			return false
		}
	}

	if !s.podInformer.ClustersSynced() {
		klog.V(2).Infof("Cluster list not synced")
		return false
	}
	clusters, err := s.podInformer.GetReadyClusters()
	if err != nil {
		runtime.HandleError(errors.Wrap(err, "Failed to get ready clusters"))
		return false
	}
	return s.podInformer.GetTargetStore().ClustersSynced(clusters)
}

func (s *ReplicaScheduler) Stop() {
	for _, plugin := range s.plugins.GetAll() {
		plugin.(*Plugin).Stop()
	}
	s.plugins.DeleteAll()
	s.podInformer.Stop()
}

func (s *ReplicaScheduler) Reconcile(obj pkgruntime.Object, qualifiedName ctlutil.QualifiedName) ctlutil.ReconciliationStatus {
	rsp, ok := obj.(*fedschedulingv1a1.ReplicaSchedulingPreference)
	if !ok {
		runtime.HandleError(errors.Errorf("Incorrect runtime object for RSP: %v", rsp))
		return ctlutil.StatusError
	}

	clusterNames, err := s.clusterNames()
	if err != nil {
		runtime.HandleError(errors.Wrap(err, "Failed to get cluster list"))
		return ctlutil.StatusError
	}
	if len(clusterNames) == 0 {
		// no joined clusters, nothing to do
		return ctlutil.StatusAllOK
	}

	kind := rsp.Spec.TargetKind
	if kind != "FederatedDeployment" && kind != "FederatedReplicaSet" {
		runtime.HandleError(errors.Wrapf(err, "RSP target kind: %s is incorrect", kind))
		return ctlutil.StatusNeedsRecheck
	}

	plugin, ok := s.plugins.Get(kind)
	if !ok {
		return ctlutil.StatusAllOK
	}

	if !plugin.(*Plugin).FederatedTypeExists(qualifiedName.String()) {
		// target FederatedType does not exist, nothing to do
		return ctlutil.StatusAllOK
	}

	key := qualifiedName.String()
	result, err := s.GetSchedulingResult(rsp, qualifiedName, clusterNames)
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "Failed to compute the schedule information while reconciling RSP named %q", key))
		return ctlutil.StatusError
	}

	err = plugin.(*Plugin).Reconcile(qualifiedName, result)
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "Failed to reconcile federated targets for RSP named %q", key))
		return ctlutil.StatusError
	}

	return ctlutil.StatusAllOK
}

// The list of clusters could come from any target informer
func (s *ReplicaScheduler) clusterNames() ([]string, error) {
	clusters, err := s.podInformer.GetReadyClusters()
	if err != nil {
		return nil, err
	}
	clusterNames := []string{}
	for _, cluster := range clusters {
		clusterNames = append(clusterNames, cluster.Name)
	}

	return clusterNames, nil
}

func (s *ReplicaScheduler) GetSchedulingResult(rsp *fedschedulingv1a1.ReplicaSchedulingPreference, qualifiedName ctlutil.QualifiedName, clusterNames []string) (map[string]int64, error) {
	key := qualifiedName.String()

	objectGetter := func(clusterName, key string) (interface{}, bool, error) {
		plugin, ok := s.plugins.Get(rsp.Spec.TargetKind)
		if !ok {
			return nil, false, nil
		}
		return plugin.(*Plugin).targetInformer.GetTargetStore().GetByKey(clusterName, key)
	}
	podsGetter := func(clusterName string, unstructuredObj *unstructured.Unstructured) (*corev1.PodList, error) {
		client, err := s.podInformer.GetClientForCluster(clusterName)
		if err != nil {
			return nil, err
		}
		selectorLabels, ok, err := unstructured.NestedStringMap(unstructuredObj.Object, "spec", "selector", "matchLabels")
		if !ok {
			return nil, errors.New("missing selector on object")
		}
		if err != nil {
			return nil, errors.Wrap(err, "error retrieving selector from object")
		}

		podList := &corev1.PodList{}
		err = client.List(context.Background(), podList, unstructuredObj.GetNamespace(), crclient.MatchingLabels(selectorLabels))
		if err != nil {
			return nil, err
		}
		return podList, nil
	}

	currentReplicasPerCluster, estimatedCapacity, err := clustersReplicaState(clusterNames, key, objectGetter, podsGetter)
	if err != nil {
		return nil, err
	}

	// TODO: Move this to API defaulting logic
	if len(rsp.Spec.Clusters) == 0 {
		rsp.Spec.Clusters = map[string]fedschedulingv1a1.ClusterPreferences{
			"*": {Weight: 1},
		}
	}

	plnr := planner.NewPlanner(rsp)
	return schedule(plnr, key, clusterNames, currentReplicasPerCluster, estimatedCapacity)
}

func schedule(planner *planner.Planner, key string, clusterNames []string, currentReplicasPerCluster map[string]int64, estimatedCapacity map[string]int64) (map[string]int64, error) {
	scheduleResult, overflow, err := planner.Plan(clusterNames, currentReplicasPerCluster, estimatedCapacity, key)
	if err != nil {
		return nil, err
	}

	// TODO: Check if we really need to place the federated type in clusters
	// with 0 replicas. Override replicas would be set to 0 in this case.
	result := make(map[string]int64)
	for clusterName := range currentReplicasPerCluster {
		result[clusterName] = 0
	}

	for clusterName, replicas := range scheduleResult {
		result[clusterName] = replicas
	}
	for clusterName, replicas := range overflow {
		result[clusterName] += replicas
	}

	if klog.V(4) {
		buf := bytes.NewBufferString(fmt.Sprintf("Schedule - %q\n", key))
		sort.Strings(clusterNames)
		for _, clusterName := range clusterNames {
			cur := currentReplicasPerCluster[clusterName]
			target := scheduleResult[clusterName]
			fmt.Fprintf(buf, "%s: current: %d target: %d", clusterName, cur, target)
			if over, found := overflow[clusterName]; found {
				fmt.Fprintf(buf, " overflow: %d", over)
			}
			if capacity, found := estimatedCapacity[clusterName]; found {
				fmt.Fprintf(buf, " capacity: %d", capacity)
			}
			fmt.Fprintf(buf, "\n")
		}
		klog.V(4).Infof(buf.String())
	}
	return result, nil
}

// clustersReplicaState returns information about the scheduling state of the pods running in the federated clusters.
func clustersReplicaState(
	clusterNames []string,
	key string,
	objectGetter func(clusterName string, key string) (interface{}, bool, error),
	podsGetter func(clusterName string, obj *unstructured.Unstructured) (*corev1.PodList, error)) (currentReplicasPerCluster map[string]int64, estimatedCapacity map[string]int64, err error) {

	currentReplicasPerCluster = make(map[string]int64)
	estimatedCapacity = make(map[string]int64)

	for _, clusterName := range clusterNames {
		obj, exists, err := objectGetter(clusterName, key)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			continue
		}

		unstructuredObj := obj.(*unstructured.Unstructured)
		replicas, ok, err := unstructured.NestedInt64(unstructuredObj.Object, "spec", "replicas")
		if err != nil {
			return nil, nil, errors.Wrap(err, "Error retrieving 'replicas' field")
		}
		if !ok {
			replicas = int64(0)
		}
		readyReplicas, ok, err := unstructured.NestedInt64(unstructuredObj.Object, "status", "readyreplicas")
		if err != nil {
			return nil, nil, errors.Wrap(err, "Error retrieving 'readyreplicas' field")
		}
		if !ok {
			readyReplicas = int64(0)
		}

		if replicas == readyReplicas {
			currentReplicasPerCluster[clusterName] = readyReplicas
		} else {
			currentReplicasPerCluster[clusterName] = int64(0)
			podList, err := podsGetter(clusterName, unstructuredObj)
			if err != nil {
				return nil, nil, err
			}

			podStatus := podanalyzer.AnalyzePods(podList, time.Now())
			currentReplicasPerCluster[clusterName] = int64(podStatus.RunningAndReady) // include pending as well?
			unschedulable := int64(podStatus.Unschedulable)
			if unschedulable > 0 {
				estimatedCapacity[clusterName] = replicas - unschedulable
			}
		}
	}
	return currentReplicasPerCluster, estimatedCapacity, nil
}

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

package util

import (
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	kubeclientset "k8s.io/client-go/kubernetes"
)

type HostClusterFinder interface {
	IsHostCluster(clusterName string) (bool, error)
}

type hostClusterFinder struct {
	sync.RWMutex
	controllerConfig    *ControllerConfig
	hostClusterName     string
	notHostClusterNames sets.String
}

func NewHostClusterFinder(controllerConfig *ControllerConfig) HostClusterFinder {
	return &hostClusterFinder{
		controllerConfig:    controllerConfig,
		notHostClusterNames: sets.NewString(),
	}
}

func (f *hostClusterFinder) isHostCluster(clusterName string) (bool, bool) {
	f.RLock()
	defer f.RUnlock()
	if len(f.hostClusterName) > 0 {
		return (clusterName == f.hostClusterName), true
	}
	return false, f.notHostClusterNames.Has(clusterName)
}

func (f *hostClusterFinder) setIsHostCluster(clusterName string, isHostCluster bool) {
	f.Lock()
	defer f.Unlock()
	if isHostCluster {
		f.hostClusterName = clusterName
	} else {
		f.notHostClusterNames.Insert(clusterName)
	}
}

func (f *hostClusterFinder) IsHostCluster(clusterName string) (bool, error) {
	isHostCluster, ok := f.isHostCluster(clusterName)
	if ok {
		return isHostCluster, nil
	}

	userAgent := "federation-controller-config"

	cc := f.controllerConfig

	fedClient, kubeClient, crClient := cc.AllClients(userAgent)

	fedCluster, err := fedClient.CoreV1alpha1().FederatedClusters(cc.FederationNamespace).Get(clusterName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to retrieving FederatedCluster: %v", err)
	}
	clusterConfig, err := BuildClusterConfig(fedCluster, kubeClient, crClient, cc.FederationNamespace, cc.ClusterNamespace)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve config for cluster %q: %v", clusterName, err)
	}
	clusterClient, err := kubeclientset.NewForConfig(clusterConfig)
	if err != nil {
		return false, fmt.Errorf("failed to create client for cluster %q: %v", clusterName, err)
	}
	clusterNamespace, err := clusterClient.CoreV1().Namespaces().Get(cc.FederationNamespace, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to retrieve federation system namespace from cluster %q: %v", clusterName, err)
	}
	localNamespace, err := kubeClient.CoreV1().Namespaces().Get(cc.FederationNamespace, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to retrieve federation system namespace from host cluster: %v", err)
	}

	isHostCluster = IsPrimaryCluster(clusterNamespace, localNamespace)
	f.setIsHostCluster(clusterName, isHostCluster)
	return isHostCluster, nil
}

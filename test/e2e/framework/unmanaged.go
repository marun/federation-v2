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

package framework

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"sigs.k8s.io/kubefed/pkg/apis/core/typeconfig"
	fedv1b1 "sigs.k8s.io/kubefed/pkg/apis/core/v1beta1"
	genericclient "sigs.k8s.io/kubefed/pkg/client/generic"
	"sigs.k8s.io/kubefed/pkg/controller/util"
	"sigs.k8s.io/kubefed/test/common"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	// Allow high rates of client calls from e2e tests. The clusters
	// targeted for testing are expected to be dedicated and it
	// shouldn't be necessary to restrict the rate of client calls.
	testConfigQPS   = 80
	testConfigBurst = 100
)

var (
	clusterControllerFixture *ControllerFixture
	// The client and set of deleted namespaces is used on suite
	// teardown to ensure namespaces are deleted before finalizing
	// controllers can be shutdown.
	hostClusterClient kubeclientset.Interface
	deletedNamespaces []string
)

func SetUpControlPlane() {
	if clusterControllerFixture != nil {
		return
	}

	config, _, err := loadConfig(TestContext.KubeConfig, TestContext.KubeContext)
	Expect(err).NotTo(HaveOccurred())

	clusterControllerFixture = NewClusterControllerFixture(NewE2ELogger(), &util.ControllerConfig{
		KubeFedNamespaces: util.KubeFedNamespaces{
			KubeFedNamespace: TestContext.KubeFedSystemNamespace,
		},
		KubeConfig: config,
	})
}

func TearDownControlPlane() {
	if TestContext.InMemoryControllers {
		if clusterControllerFixture != nil {
			clusterControllerFixture.TearDown(NewE2ELogger())
			clusterControllerFixture = nil
		}
	} else if TestContext.WaitForFinalization {
		for _, namespace := range deletedNamespaces {
			err := waitForNamespaceDeletion(hostClusterClient, namespace)
			if err != nil {
				Errorf("%s", err)
			}
		}
	}
}

type UnmanagedFramework struct {
	// To make sure that this framework cleans up after itself, no matter what,
	// we install a Cleanup action before each test and clear it after.  If we
	// should abort, the AfterSuite hook should run all Cleanup actions.
	cleanupHandle CleanupActionHandle

	testNamespaceName string

	Config     *restclient.Config
	Kubeconfig *clientcmdapi.Config

	BaseName string

	logger common.TestLogger
}

func NewUnmanagedFramework(baseName string) KubeFedFrameworkImpl {
	f := &UnmanagedFramework{
		BaseName: baseName,
		logger:   NewE2ELogger(),
	}
	return f
}

// BeforeEach reads the cluster configuration if it has not yet been read.
func (f *UnmanagedFramework) BeforeEach() {
	// The fact that we need this feels like a bug in ginkgo.
	// https://github.com/onsi/ginkgo/issues/222
	f.cleanupHandle = AddCleanupAction(f.AfterEach)

	if f.Config == nil {
		By("Reading cluster configuration")
		var err error
		f.Config, f.Kubeconfig, err = loadConfig(TestContext.KubeConfig, TestContext.KubeContext)
		Expect(err).NotTo(HaveOccurred())
	}
}

// AfterEach deletes the namespace, after reading its events.
func (f *UnmanagedFramework) AfterEach() {
	RemoveCleanupAction(f.cleanupHandle)

	userAgent := fmt.Sprintf("%s-teardown", f.BaseName)

	// Cleanup needs to remain as a defer function because
	// DumpEventsInNamespace contains assertions that could exit the function.
	defer func() {
		// DeleteNamespace at the very end in defer, but before tearing down
		// the namespace sync controller to avoid any expectation failures
		// preventing deleting the namespace due to finalizers no longer able
		// to be removed.
		if f.testNamespaceName == "" {
			return
		}
		// Clear the name first to ensure other tests always get a
		// fresh namespace even if namespace deletion fails
		namespaceName := f.testNamespaceName
		f.testNamespaceName = ""

		// Running namespaced implies that the test namespace is the
		// KubeFed system namespace and should not be removed.
		if !TestContext.LimitedScope {
			client := f.KubeClient(userAgent)
			DeleteNamespace(client, namespaceName)
		}
	}()

	// Print events if the test failed and ran in a namespace.
	if CurrentGinkgoTestDescription().Failed && f.testNamespaceName != "" {
		kubeClient := f.KubeClient(userAgent)
		DumpEventsInNamespace(func(opts metav1.ListOptions, ns string) (*corev1.EventList, error) {
			return kubeClient.CoreV1().Events(ns).List(opts)
		}, f.testNamespaceName)
	}
}

func (f *UnmanagedFramework) ControllerConfig() *util.ControllerConfig {
	return &util.ControllerConfig{
		KubeFedNamespaces: util.KubeFedNamespaces{
			KubeFedNamespace: TestContext.KubeFedSystemNamespace,
			TargetNamespace:  f.inMemoryTargetNamespace(),
		},
		KubeConfig:      f.Config,
		MinimizeLatency: true,
		ScaleTesting:    TestContext.ScaleTest,
	}
}

func (f *UnmanagedFramework) Logger() common.TestLogger {
	return f.logger
}

func (f *UnmanagedFramework) KubeConfig() *restclient.Config {
	return f.Config
}

func (f *UnmanagedFramework) KubeClient(userAgent string) kubeclientset.Interface {
	config := restclient.CopyConfig(f.Config)
	restclient.AddUserAgent(config, userAgent)
	return kubeclientset.NewForConfigOrDie(config)
}

func (f *UnmanagedFramework) Client(userAgent string) genericclient.Client {
	return genericclient.NewForConfigOrDieWithUserAgent(f.Config, userAgent)
}

func (f *UnmanagedFramework) ClusterNames(userAgent string) []string {
	var clusters []string
	client := f.Client(userAgent)
	clusterList := &fedv1b1.KubeFedClusterList{}
	err := client.List(context.TODO(), clusterList, TestContext.KubeFedSystemNamespace)
	ExpectNoError(err, fmt.Sprintf("Error retrieving list of federated clusters: %+v", err))

	for _, cluster := range clusterList.Items {
		clusters = append(clusters, cluster.Name)
	}
	return clusters
}

func (f *UnmanagedFramework) ClusterDynamicClients(apiResource *metav1.APIResource, userAgent string) map[string]common.TestCluster {
	testClusters := make(map[string]common.TestCluster)
	for clusterName, clusterConfig := range f.ClusterConfigs(userAgent) {
		client, err := util.NewResourceClient(clusterConfig.Config, apiResource)
		if err != nil {
			Failf("Error creating a resource client in cluster %q for kind %q: %v", clusterName, apiResource.Kind, err)
		}
		// Check if this cluster is the same name as the host cluster name to
		// make it the primary cluster.
		testClusters[clusterName] = common.TestCluster{
			TestClusterConfig: clusterConfig,
			Client:            client,
		}
	}
	return testClusters
}

func (f *UnmanagedFramework) ClusterKubeClients(userAgent string) map[string]kubeclientset.Interface {
	typedClients := make(map[string]kubeclientset.Interface)
	for clusterName, clusterConfig := range f.ClusterConfigs(userAgent) {
		client, err := kubeclientset.NewForConfig(clusterConfig.Config)
		if err != nil {
			Failf("Error creating a typed client in cluster %q: %v", clusterName, err)
		}
		typedClients[clusterName] = client
	}
	return typedClients
}

func (f *UnmanagedFramework) ClusterConfigs(userAgent string) map[string]common.TestClusterConfig {
	// TODO(marun) Avoid having to reload configuration on every call.
	// Clusters may be added or removed between calls, but
	// configuration is unlikely to change.
	//
	// Could also accept 'forceReload' parameter for tests that require it.

	By("Obtaining a list of federated clusters")
	client := f.Client(userAgent)
	clusterList := ListKubeFedClusters(NewE2ELogger(), client, TestContext.KubeFedSystemNamespace)

	// Assume host cluster name is the same as the current context name.
	hostClusterName := f.Kubeconfig.CurrentContext

	clusterConfigs := make(map[string]common.TestClusterConfig)
	for _, cluster := range clusterList.Items {
		config, err := util.BuildClusterConfig(&cluster, client, TestContext.KubeFedSystemNamespace)
		Expect(err).NotTo(HaveOccurred())
		restclient.AddUserAgent(config, userAgent)
		clusterConfigs[cluster.Name] = common.TestClusterConfig{
			Config:    config,
			IsPrimary: (cluster.Name == hostClusterName),
		}
	}

	return clusterConfigs
}

func (f *UnmanagedFramework) HostConfig(userAgent string) *restclient.Config {
	for _, clusterConfig := range f.ClusterConfigs(userAgent) {
		if clusterConfig.IsPrimary {
			return clusterConfig.Config
		}
	}
	return nil
}

func (f *UnmanagedFramework) KubeFedSystemNamespace() string {
	return TestContext.KubeFedSystemNamespace
}

func (f *UnmanagedFramework) TestNamespaceName() string {
	if f.testNamespaceName == "" {
		if TestContext.LimitedScope {
			f.testNamespaceName = TestContext.KubeFedSystemNamespace
		} else {
			client := f.KubeClient(fmt.Sprintf("%s-create-namespace", f.BaseName))
			f.testNamespaceName = CreateTestNamespace(client, f.BaseName)
		}
	}
	return f.testNamespaceName
}

func (f *UnmanagedFramework) inMemoryTargetNamespace() string {
	if TestContext.LimitedScopeInMemoryControllers {
		return f.TestNamespaceName()
	}
	return metav1.NamespaceAll
}

func (f *UnmanagedFramework) setUpSyncControllerFixture(typeConfig typeconfig.Interface, namespacePlacement *metav1.APIResource) TestFixture {
	// Hybrid setup where just the sync controller is run and we do not rely on
	// the already deployed (unmanaged) controller manager. Only do this if
	// in-memory-controllers is true.
	if TestContext.InMemoryControllers {
		controllerConfig := f.ControllerConfig()
		// Namespaces are cluster scoped so all namespaces must be targeted
		if typeConfig.GetTargetType().Kind == util.NamespaceKind {
			controllerConfig.TargetNamespace = metav1.NamespaceAll
		}
		return NewSyncControllerFixture(f.logger, controllerConfig, typeConfig, namespacePlacement)
	}
	return nil
}

func DeleteNamespace(client kubeclientset.Interface, namespaceName string) {
	orphanDependents := false
	if err := client.CoreV1().Namespaces().Delete(namespaceName, &metav1.DeleteOptions{OrphanDependents: &orphanDependents}); err != nil {
		if !apierrors.IsNotFound(err) {
			Failf("Error while deleting namespace %s: %s", namespaceName, err)
		}
	}

	if TestContext.InMemoryControllers {
		if !TestContext.WaitForFinalization {
			// Skip waiting for namespace deletion so that tests run
			// as fast as possible (with the potential cost of leaving
			// wedged resources).
			return
		}
		// Wait for namespace deletion to ensure that in-memory
		// controllers have a chance to remove finalizers that could
		// block deletion of federated resources and their containing
		// namespace.
		err := waitForNamespaceDeletion(client, namespaceName)
		if err != nil {
			Failf("%s", err)
		}
	} else {
		if hostClusterClient == nil {
			hostClusterClient = client
		}
		// Track the namespace to allow deletion to be verified on
		// suite teardown.
		deletedNamespaces = append(deletedNamespaces, namespaceName)
	}
}

func waitForNamespaceDeletion(client kubeclientset.Interface, namespace string) error {
	err := wait.PollImmediate(PollInterval, TestContext.SingleCallTimeout, func() (bool, error) {
		if _, err := client.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			Errorf("Error while waiting for namespace to be removed: %v", err)
		}
		return false, nil
	})
	if err != nil {
		return errors.Errorf("Namespace %q was not deleted after %v", namespace, TestContext.SingleCallTimeout)
	}
	return nil
}

func loadConfig(configPath, context string) (*restclient.Config, *clientcmdapi.Config, error) {
	Logf(">>> kubeConfig: %s", configPath)
	c, err := clientcmd.LoadFromFile(configPath)
	if err != nil {
		return nil, nil, errors.Errorf("error loading kubeConfig %s: %v", configPath, err.Error())
	}
	if context != "" {
		Logf(">>> kubeContext: %s", context)
		c.CurrentContext = context
	}
	cfg, err := clientcmd.NewDefaultClientConfig(*c, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, nil, errors.Errorf("error creating default client config: %v", err.Error())
	}
	cfg.QPS = testConfigQPS
	cfg.Burst = testConfigBurst
	return cfg, c, nil
}

// byFirstTimestamp sorts a slice of events by first timestamp, using their involvedObject's name as a tie breaker.
type byFirstTimestamp []corev1.Event

func (o byFirstTimestamp) Len() int      { return len(o) }
func (o byFirstTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func (o byFirstTimestamp) Less(i, j int) bool {
	if o[i].FirstTimestamp.Equal(&o[j].FirstTimestamp) {
		return o[i].InvolvedObject.Name < o[j].InvolvedObject.Name
	}
	return o[i].FirstTimestamp.Before(&o[j].FirstTimestamp)
}

type EventsLister func(opts metav1.ListOptions, ns string) (*corev1.EventList, error)

func DumpEventsInNamespace(eventsLister EventsLister, namespace string) {
	By(fmt.Sprintf("Collecting events from namespace %q.", namespace))
	events, err := eventsLister(metav1.ListOptions{}, namespace)
	Expect(err).NotTo(HaveOccurred())

	By(fmt.Sprintf("Found %d events.", len(events.Items)))
	// Sort events by their first timestamp
	sortedEvents := events.Items
	if len(sortedEvents) > 1 {
		sort.Sort(byFirstTimestamp(sortedEvents))
	}
	for _, e := range sortedEvents {
		Logf("At %v - event for %v: %v %v: %v", e.FirstTimestamp, e.InvolvedObject.Name, e.Source, e.Reason, e.Message)
	}
	// Note that we don't wait for any Cleanup to propagate, which means
	// that if you delete a bunch of pods right before ending your test,
	// you may or may not see the killing/deletion/Cleanup events.
}

func WaitForUnmanagedClusterReadiness() {
	config, _, err := loadConfig(TestContext.KubeConfig, TestContext.KubeContext)
	Expect(err).NotTo(HaveOccurred())
	restclient.AddUserAgent(config, "readiness-check")
	client := genericclient.NewForConfigOrDie(config)
	WaitForClusterReadiness(NewE2ELogger(), client, TestContext.KubeFedSystemNamespace, PollInterval, TestContext.SingleCallTimeout)
}

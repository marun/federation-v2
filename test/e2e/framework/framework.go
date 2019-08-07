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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"

	"sigs.k8s.io/kubefed/pkg/apis/core/typeconfig"
	fedv1b1 "sigs.k8s.io/kubefed/pkg/apis/core/v1beta1"
	genericclient "sigs.k8s.io/kubefed/pkg/client/generic"
	"sigs.k8s.io/kubefed/pkg/controller/util"
	"sigs.k8s.io/kubefed/test/common"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TODO(marun) Replace the framework with the unmanaged
// implementation.

type KubeFedFrameworkImpl interface {
	BeforeEach()
	AfterEach()

	ControllerConfig() *util.ControllerConfig

	Logger() common.TestLogger

	KubeConfig() *restclient.Config

	KubeClient(userAgent string) kubeclientset.Interface
	Client(userAgent string) genericclient.Client

	ClusterConfigs(userAgent string) map[string]common.TestClusterConfig
	HostConfig(userAgent string) *restclient.Config
	ClusterDynamicClients(apiResource *metav1.APIResource, userAgent string) map[string]common.TestCluster
	ClusterKubeClients(userAgent string) map[string]kubeclientset.Interface
	ClusterNames(userAgent string) []string

	KubeFedSystemNamespace() string

	// Name of the namespace for the current test to target
	TestNamespaceName() string

	// Internal method that accepts the namespace placement api
	// resource to support namespaced controllers.
	setUpSyncControllerFixture(typeConfig typeconfig.Interface, namespacePlacement *metav1.APIResource) TestFixture
}

// KubeFedFramework provides an interface to a test control plane so
// that the implementation can vary without affecting tests.
type KubeFedFramework interface {
	KubeFedFrameworkImpl

	// Registering a fixture ensures it will be torn down after the
	// current test has executed.
	RegisterFixture(fixture TestFixture)

	// Setup a sync controller if necessary and return the fixture.
	// This is implemented commonly to support running a sync
	// controller for tests that require it.
	SetUpSyncControllerFixture(typeConfig typeconfig.Interface) TestFixture

	// Ensure propagation of the test namespace to member clusters
	EnsureTestNamespacePropagation()

	// Ensure a federated namespace resource for the test namespace
	// exists so that the namespace will be propagated to either all
	// or no member clusters.
	EnsureTestFederatedNamespace(allClusters bool) *unstructured.Unstructured
}

// A framework needs to be instantiated before tests are executed to
// ensure registration of BeforeEach/AfterEach.  However, flags that
// dictate which framework should be used won't have been parsed yet.
// The workaround is using a wrapper that performs late-binding on the
// framework flavor.
type frameworkWrapper struct {
	impl                KubeFedFrameworkImpl
	baseName            string
	namespaceTypeConfig typeconfig.Interface

	// Fixtures to cleanup after each test
	fixtures []TestFixture
}

func NewKubeFedFramework(baseName string) KubeFedFramework {
	f := &frameworkWrapper{
		baseName: baseName,
		fixtures: []TestFixture{},
	}
	AfterEach(f.AfterEach)
	BeforeEach(f.BeforeEach)
	return f
}

func (f *frameworkWrapper) framework() KubeFedFrameworkImpl {
	if f.impl == nil {
		f.impl = NewUnmanagedFramework(f.baseName)
	}
	return f.impl
}

func (f *frameworkWrapper) BeforeEach() {
	f.framework().BeforeEach()
}

func (f *frameworkWrapper) AfterEach() {
	f.framework().AfterEach()

	// TODO(font): Delete the namespace finalizer manually rather than
	// relying on the federated namespace sync controller. This would
	// remove the dependency of namespace removal on fixture teardown,
	// which allows the teardown to be moved outside the defer, but before
	// the DumpEventsInNamespace that may contain assertions that could
	// exit the function.
	logger := f.framework().Logger()
	for len(f.fixtures) > 0 {
		fixture := f.fixtures[0]
		fixture.TearDown(logger)
		f.fixtures = f.fixtures[1:]
	}
}

func (f *frameworkWrapper) ControllerConfig() *util.ControllerConfig {
	return f.framework().ControllerConfig()
}

func (f *frameworkWrapper) Logger() common.TestLogger {
	return f.framework().Logger()
}

func (f *frameworkWrapper) KubeConfig() *restclient.Config {
	return f.framework().KubeConfig()
}

func (f *frameworkWrapper) KubeClient(userAgent string) kubeclientset.Interface {
	return f.framework().KubeClient(userAgent)
}

func (f *frameworkWrapper) Client(userAgent string) genericclient.Client {
	return f.framework().Client(userAgent)
}

func (f *frameworkWrapper) ClusterConfigs(userAgent string) map[string]common.TestClusterConfig {
	return f.framework().ClusterConfigs(userAgent)
}

func (f *frameworkWrapper) HostConfig(userAgent string) *restclient.Config {
	return f.framework().HostConfig(userAgent)
}

func (f *frameworkWrapper) ClusterNames(userAgent string) []string {
	return f.framework().ClusterNames(userAgent)
}

func (f *frameworkWrapper) ClusterDynamicClients(apiResource *metav1.APIResource, userAgent string) map[string]common.TestCluster {
	return f.framework().ClusterDynamicClients(apiResource, userAgent)
}

func (f *frameworkWrapper) ClusterKubeClients(userAgent string) map[string]kubeclientset.Interface {
	return f.framework().ClusterKubeClients(userAgent)
}

func (f *frameworkWrapper) KubeFedSystemNamespace() string {
	return f.framework().KubeFedSystemNamespace()
}

func (f *frameworkWrapper) TestNamespaceName() string {
	return f.framework().TestNamespaceName()
}

// setUpSyncControllerFixture is not intended to be called on the
// wrapper.  It's only implemented by the concrete frameworks to avoid
// having callers pass in the namespacePlacement arg.
func (f *frameworkWrapper) setUpSyncControllerFixture(typeConfig typeconfig.Interface, namespacePlacement *metav1.APIResource) TestFixture {
	return nil
}

func (f *frameworkWrapper) SetUpSyncControllerFixture(typeConfig typeconfig.Interface) TestFixture {
	namespaceTypeConfig := f.namespaceTypeConfigOrDie()
	fedNamespaceAPIResource := namespaceTypeConfig.GetFederatedType()
	return f.framework().setUpSyncControllerFixture(typeConfig, &fedNamespaceAPIResource)
}

func (f *frameworkWrapper) RegisterFixture(fixture TestFixture) {
	if fixture != nil {
		f.fixtures = append(f.fixtures, fixture)
	}
}

func (f *frameworkWrapper) EnsureTestNamespacePropagation() {
	// When targeting a single namespace, the test namespace will
	// already exist in member clusters.
	if TestContext.LimitedScope {
		return
	}

	// Ensure the test namespace is propagated to all clusters.
	f.EnsureTestFederatedNamespace(true)

	// Start the namespace sync controller to propagate the namespace
	namespaceTypeConfig := f.namespaceTypeConfigOrDie()
	fixture := f.framework().setUpSyncControllerFixture(namespaceTypeConfig, nil)
	f.RegisterFixture(fixture)
}

func (f *frameworkWrapper) EnsureTestFederatedNamespace(allClusters bool) *unstructured.Unstructured {
	tl := f.Logger()

	client, err := genericclient.New(f.KubeConfig())
	if err != nil {
		tl.Fatalf("Error initializing dynamic client: %v", err)
	}

	apiResource := f.namespaceTypeConfigOrDie().GetFederatedType()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   apiResource.Group,
		Kind:    apiResource.Kind,
		Version: apiResource.Version,
	})

	namespace := f.TestNamespaceName()

	// Return an existing federated namespace if it already exists.
	err = client.Get(context.Background(), obj, namespace, namespace)
	if err == nil {
		return obj
	}
	if !errors.IsNotFound(err) {
		tl.Fatalf("Error retrieving %s %q: %v", apiResource.Kind, err)
	}

	// Othewise create it.
	obj.SetName(namespace)
	obj.SetNamespace(namespace)
	spec := map[string]interface{}{}
	if allClusters {
		// An empty cluster selector field selects all clusters
		spec[util.PlacementField] = map[string]interface{}{
			util.ClusterSelectorField: map[string]interface{}{},
		}
	}
	obj.Object[util.SpecField] = spec

	err = client.Create(context.Background(), obj)
	if err != nil {
		tl.Fatalf("Error creating %s for namespace %q: %v", apiResource.Kind, namespace, err)
	}
	tl.Logf("Created new %s %q", apiResource.Kind, namespace)

	return obj
}

func (f *frameworkWrapper) namespaceTypeConfigOrDie() typeconfig.Interface {
	if f.namespaceTypeConfig == nil {
		tl := f.Logger()
		client, err := genericclient.New(f.KubeConfig())
		if err != nil {
			tl.Fatalf("Error initializing dynamic client: %v", err)
		}
		typeConfig := &fedv1b1.FederatedTypeConfig{}
		err = client.Get(context.Background(), typeConfig, f.KubeFedSystemNamespace(), util.NamespaceName)
		if err != nil {
			tl.Fatalf("Error retrieving federatedtypeconfig for %q: %v", util.NamespaceName, err)
		}
		f.namespaceTypeConfig = typeConfig
	}
	return f.namespaceTypeConfig
}

func CreateTestNamespace(client kubeclientset.Interface, baseName string) string {
	By("Creating a namespace to execute the test in")
	namespaceName, err := CreateNamespace(client, fmt.Sprintf("e2e-tests-%v-", baseName))
	Expect(err).NotTo(HaveOccurred())
	By(fmt.Sprintf("Created test namespace %s", namespaceName))
	return namespaceName
}

func CreateNamespace(client kubeclientset.Interface, generateName string) (string, error) {
	namespaceObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: generateName,
		},
	}
	// Be robust about making the namespace creation call.
	// TODO(marun) should all api calls be made 'robustly'?
	var namespaceName string
	if err := wait.PollImmediate(PollInterval, TestContext.SingleCallTimeout, func() (bool, error) {
		namespace, err := client.CoreV1().Namespaces().Create(namespaceObj)
		if err != nil {
			Logf("Unexpected error while creating namespace: %v", err)
			return false, nil
		}
		namespaceName = namespace.Name
		return true, nil
	}); err != nil {
		return "", err
	}
	return namespaceName, nil
}

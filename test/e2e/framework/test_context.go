/*
Copyright 2017 The Kubernetes Authors.

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
	"flag"
	"fmt"
	"os"
	"time"

	"k8s.io/klog"

	"sigs.k8s.io/kubefed/pkg/controller/util"
)

type TestContextType struct {
	InMemoryControllers             bool
	KubeConfig                      string
	KubeContext                     string
	KubeFedSystemNamespace          string
	SingleCallTimeout               time.Duration
	LimitedScope                    bool
	LimitedScopeInMemoryControllers bool
	WaitForFinalization             bool
	ScaleTest                       bool
	ScaleClusterCount               int
}

func (t *TestContextType) RunControllers() bool {
	return t.InMemoryControllers
}

// NamespaceScopedControlPlane indicates that the control plane is
// effectively namespace-scoped. This may be either because in-memory
// controllers are running namespace-scoped against a cluster-scoped
// deployment (a debugging optimization) or if a deployed control
// plane is running namespace-scoped.
func (t *TestContextType) NamespaceScopedControlPlane() bool {
	return t.InMemoryControllers && t.LimitedScopeInMemoryControllers || t.LimitedScope
}

var TestContext *TestContextType = &TestContextType{}

func registerFlags(t *TestContextType) {
	flag.BoolVar(&t.InMemoryControllers, "in-memory-controllers", false,
		"Whether KubeFed controllers should be started in memory.")
	flag.StringVar(&t.KubeConfig, "kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to kubeconfig containing embedded authinfo.")
	flag.StringVar(&t.KubeContext, "context", "",
		"kubeconfig context to use/override. If unset, will use value from 'current-context'.")
	flag.StringVar(&t.KubeFedSystemNamespace, "kubefed-namespace", util.DefaultKubeFedSystemNamespace,
		fmt.Sprintf("The namespace the KubeFed control plane is deployed in.  If unset, will default to %q.", util.DefaultKubeFedSystemNamespace))
	flag.DurationVar(&t.SingleCallTimeout, "single-call-timeout", DefaultSingleCallTimeout,
		fmt.Sprintf("The maximum duration of a single call.  If unset, will default to %v", DefaultSingleCallTimeout))
	flag.BoolVar(&t.LimitedScope, "limited-scope", false, "Whether the KubeFed namespace (configurable via --kubefed-namespace) will be the only target for the control plane.")
	flag.BoolVar(&t.LimitedScopeInMemoryControllers, "limited-scope-in-memory-controllers", true,
		"Whether KubeFed controllers started in memory should target only the test namespace.  If debugging a cluster-scoped control plane outside of a test namespace, this should be set to false.")
	flag.BoolVar(&t.WaitForFinalization, "wait-for-finalization", true,
		"Whether the test suite should wait for finalization before stopping fixtures or exiting.  Setting this to false will speed up test execution but likely result in wedged namespaces and is only recommended for disposeable clusters.")
	flag.BoolVar(&t.ScaleTest, "scale-test", false, "Whether the test suite should be configured for scale testing.  Not compatible with most tests.")
	flag.IntVar(&t.ScaleClusterCount, "scale-cluster-count", 1, "How many member clusters to simulate when scale testing.")
}

func validateFlags(t *TestContextType) {
	if len(t.KubeConfig) == 0 {
		klog.Fatalf("kubeconfig is required")
	}

	if t.ScaleTest {
		t.InMemoryControllers = true
		t.LimitedScope = true
		t.WaitForFinalization = false
	}

	if t.InMemoryControllers {
		klog.Info("in-memory-controllers=true - this will launch the KubeFed controllers outside the cluster hosting the KubeFed control plane.")
	}
}

func ParseFlags() {
	registerFlags(TestContext)
	flag.Parse()
	validateFlags(TestContext)
}

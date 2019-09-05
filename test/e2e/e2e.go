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

package e2e

import (
	"testing"

	"k8s.io/klog"

	"sigs.k8s.io/kubefed/test/e2e/framework"
	"sigs.k8s.io/kubefed/test/e2e/framework/ginkgowrapper"

	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	"github.com/onsi/gomega"
)

// RunE2ETests checks configuration parameters (specified through flags) and then runs
// E2E tests using the Ginkgo runner.
// This function is called on each Ginkgo node in parallel mode.
func RunE2ETests(t *testing.T) {
	gomega.RegisterFailHandler(ginkgowrapper.Fail)
	klog.Infof("Starting e2e run %q on Ginkgo node %d", framework.RunId, config.GinkgoConfig.ParallelNode)
	ginkgo.RunSpecs(t, "KubeFed e2e suite")
}

// There are certain operations we only want to run once per overall test invocation
// (such as deleting old namespaces, or verifying that all system pods are running.
// Because of the way Ginkgo runs tests in parallel, we must use SynchronizedBeforeSuite
// to ensure that these operations only run on the first parallel Ginkgo node.
//
// This function takes two parameters: one function which runs on only the first Ginkgo node,
// returning an opaque byte array, and then a second function which runs on all Ginkgo nodes,
// accepting the byte array.
var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	// Run only on Ginkgo node 1

	if !framework.TestContext.ScaleTest {
		// Scale testing requires performing these steps after the
		// creation of a simulated federation.

		if framework.TestContext.InMemoryControllers {
			// Start the cluster controller to ensure clusters will be
			// reported as healthy when running the control plane
			// in-memory.
			framework.SetUpControlPlane()
		}
		// Wait for readiness of registered clusters to ensure tests
		// run against a healthy federation.
		framework.WaitForUnmanagedClusterReadiness()
	}

	return nil

}, func(data []byte) {
	// Run on all Ginkgo nodes
})

// Similar to SynchornizedBeforeSuite, we want to run some operations only once (such as collecting cluster logs).
// Here, the order of functions is reversed; first, the function which runs everywhere,
// and then the function that only runs on the first Ginkgo node.
var _ = ginkgo.SynchronizedAfterSuite(func() {
	// Run on all Ginkgo nodes
	framework.Logf("Running AfterSuite actions on all node")
	framework.RunCleanupActions()
}, func() {
	// Run only Ginkgo on node 1
	framework.Logf("Running AfterSuite actions on node 1")
	framework.TearDownControlPlane()
})

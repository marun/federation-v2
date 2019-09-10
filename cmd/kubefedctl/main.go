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

// kubefedctl is a tool for managing a KubeFed control plane.
package main

import (
	"fmt"
	"os"

	_ "k8s.io/client-go/plugin/pkg/client/auth" // Load all client auth plugins for GCP, Azure, Openstack, etc
	"k8s.io/component-base/logs"

	"sigs.k8s.io/kubefed/pkg/kubefedctl"
)

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()

	if err := kubefedctl.NewKubeFedCtlCommand(os.Stdout).Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

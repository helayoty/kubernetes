/*
Copyright 2026 The Kubernetes Authors.

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

// kube-activation-manager is the standalone server that owns the synchronous
// activation bind data plane: it serves the Activate / ReportDemand gRPC
// contract (k8s.io/activation) against a ready-set of warm capacity pods. It is
// deliberately NOT the scheduler — Activate hands out already-scheduled warm
// capacity and must not share a failure domain or process with scheduling.
package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/metrics/prometheus/clientgo" // load all the prometheus client-go plugin
	_ "k8s.io/component-base/metrics/prometheus/version"  // for version metric registration
	"k8s.io/kubernetes/cmd/kube-activation-manager/app"
)

func main() {
	command := app.NewActivationManagerCommand()
	code := cli.Run(command)
	os.Exit(code)
}

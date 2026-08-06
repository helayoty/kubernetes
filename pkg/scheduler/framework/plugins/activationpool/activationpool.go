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

// Package activationpool scores nodes for ActivationPool capacity pods so warm
// capacity lands where a restore can be a hit. It reads the checkpoint the pod
// was warmed from off a pod annotation the refill controller stamps, and prefers
// nodes that advertise the same checkpoint as a node label. It reads no
// activation.k8s.io objects, so the scheduler needs no extra informer wiring.
package activationpool

import (
	"context"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// ActivationPool is a Score plugin implementing checkpoint locality for
// ActivationPool capacity pods.
type ActivationPool struct{}

var _ fwk.ScorePlugin = &ActivationPool{}

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.ActivationPool

// Name returns name of the plugin. It is used in logs, etc.
func (pl *ActivationPool) Name() string {
	return Name
}

// Score favors nodes that already hold the checkpoint this capacity pod was
// warmed from. Pods that are not activation capacity, or capacity pods without a
// checkpoint, express no preference and score neutral.
func (pl *ActivationPool) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	if pod.Labels[activationv1alpha1.LabelPoolName] == "" {
		return 0, nil
	}
	want := pod.Annotations[activationv1alpha1.AnnotationCheckpoint]
	if want == "" {
		return 0, nil
	}
	node := nodeInfo.Node()
	if node != nil && node.Labels[activationv1alpha1.AnnotationCheckpoint] == want {
		return fwk.MaxScore, nil
	}
	return 0, nil
}

// ScoreExtensions returns nil: Score already emits values in [0, MaxScore], so no
// normalization pass is needed.
func (pl *ActivationPool) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// New initializes a new plugin and returns it. The plugin is only instantiated
// when the ActivationPool feature gate is on (it is added to the default plugin
// set behind that gate), so it holds no gate check of its own.
func New(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &ActivationPool{}, nil
}

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

package activationpool

import (
	"testing"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestScore(t *testing.T) {
	const checkpoint = "ckpt-abc"

	// capacityPod is a warm capacity pod belonging to pool "demo" warmed from
	// checkpoint. Individual cases mutate a copy for the negative variants.
	capacityPod := st.MakePod().Name("cap").
		Label(activationv1alpha1.LabelPoolName, "demo").
		Annotation(activationv1alpha1.AnnotationCheckpoint, checkpoint).Obj()

	nodeWith := func(name, ckpt string) *v1.Node {
		n := st.MakeNode().Name(name)
		if ckpt != "" {
			n = n.Label(activationv1alpha1.AnnotationCheckpoint, ckpt)
		}
		return n.Obj()
	}

	tests := []struct {
		name string
		pod  *v1.Pod
		node *v1.Node
		want int64
	}{
		{
			name: "capacity pod on node holding its checkpoint scores max",
			pod:  capacityPod,
			node: nodeWith("n", checkpoint),
			want: fwk.MaxScore,
		},
		{
			name: "capacity pod on node holding a different checkpoint scores zero",
			pod:  capacityPod,
			node: nodeWith("n", "ckpt-other"),
			want: 0,
		},
		{
			name: "capacity pod on node without a checkpoint label scores zero",
			pod:  capacityPod,
			node: nodeWith("n", ""),
			want: 0,
		},
		{
			name: "capacity pod without a checkpoint annotation is neutral even on a matching node",
			pod: st.MakePod().Name("cap").
				Label(activationv1alpha1.LabelPoolName, "demo").Obj(),
			node: nodeWith("n", checkpoint),
			want: 0,
		},
		{
			name: "non-activation pod is neutral even on a checkpoint node",
			pod: st.MakePod().Name("other").
				Annotation(activationv1alpha1.AnnotationCheckpoint, checkpoint).Obj(),
			node: nodeWith("n", checkpoint),
			want: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			p, err := New(ctx, nil, nil)
			if err != nil {
				t.Fatalf("creating plugin: %v", err)
			}
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(tc.node)

			got, status := p.(fwk.ScorePlugin).Score(ctx, framework.NewCycleState(), tc.pod, nodeInfo)
			if !status.IsSuccess() {
				t.Fatalf("unexpected status: %v", status)
			}
			if got != tc.want {
				t.Errorf("Score() = %d, want %d", got, tc.want)
			}
		})
	}
}

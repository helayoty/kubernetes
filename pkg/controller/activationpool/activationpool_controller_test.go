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
	"fmt"
	"sort"
	"testing"
	"time"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestPlanRefill(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

	// pod builds a capacity pod. age orders creation time (larger age = older);
	// state "" is treated as warm; terminating marks a deletion timestamp.
	type podSpec struct {
		name        string
		state       string
		age         int
		terminating bool
	}
	newPods := func(specs []podSpec) []*v1.Pod {
		pods := make([]*v1.Pod, 0, len(specs))
		for _, s := range specs {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              s.name,
					CreationTimestamp: metav1.NewTime(base.Add(-time.Duration(s.age) * time.Minute)),
				},
			}
			if s.state != "" {
				pod.Annotations = map[string]string{activationv1alpha1.AnnotationState: s.state}
			}
			if s.terminating {
				dt := metav1.NewTime(base)
				pod.DeletionTimestamp = &dt
			}
			pods = append(pods, pod)
		}
		return pods
	}

	tests := []struct {
		name        string
		min         int32
		max         int32
		pods        []podSpec
		wantCreate  int
		wantDeletes []string
	}{
		{
			name:       "empty pool creates to floor",
			min:        3,
			max:        5,
			pods:       nil,
			wantCreate: 3,
		},
		{
			name:       "at floor does nothing",
			min:        2,
			max:        5,
			pods:       []podSpec{{name: "a"}, {name: "b"}},
			wantCreate: 0,
		},
		{
			name:       "claimed pods do not count toward warm floor",
			min:        2,
			max:        5,
			pods:       []podSpec{{name: "a", state: activationv1alpha1.StateWarm}, {name: "b", state: activationv1alpha1.StateClaimed}},
			wantCreate: 1,
		},
		{
			name:       "max below min is clamped to min",
			min:        5,
			max:        3,
			pods:       nil,
			wantCreate: 5, // ceil clamps up to floor=5; deficit=5, room=5
		},
		{
			name:       "room limits create below deficit",
			min:        4,
			max:        4,
			pods:       []podSpec{{name: "a", state: activationv1alpha1.StateClaimed}, {name: "b", state: activationv1alpha1.StateClaimed}},
			wantCreate: 2, // ceil=4; active=2; room=2; warm=0; deficit=4 -> capped to room 2
		},
		{
			name:        "overflow deletes newest idle first",
			min:         1,
			max:         2,
			pods:        []podSpec{{name: "old", age: 30}, {name: "mid", age: 20}, {name: "new", age: 10}},
			wantCreate:  0,
			wantDeletes: []string{"new"},
		},
		{
			name:       "overflow of claimed pods is not deleted",
			min:        1,
			max:        2,
			pods:       []podSpec{{name: "w", state: activationv1alpha1.StateWarm}, {name: "c1", state: activationv1alpha1.StateClaimed}, {name: "c2", state: activationv1alpha1.StateClaimed}},
			wantCreate: 0,
			// warm=1==floor, so deletableWarm=0 -> no deletes even though active(3)>max(2).
			wantDeletes: nil,
		},
		{
			name:       "terminating pods are ignored",
			min:        2,
			max:        4,
			pods:       []podSpec{{name: "a"}, {name: "b", terminating: true}},
			wantCreate: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool := &activationv1alpha1.ActivationPool{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pool"},
				Spec: activationv1alpha1.ActivationPoolSpec{
					Warm: activationv1alpha1.WarmSpec{Min: tt.min, Max: tt.max},
				},
			}
			got := planRefill(pool, newPods(tt.pods))
			if got.create != tt.wantCreate {
				t.Errorf("create = %d, want %d", got.create, tt.wantCreate)
			}
			sort.Strings(got.deleteNames)
			want := append([]string(nil), tt.wantDeletes...)
			sort.Strings(want)
			if len(got.deleteNames) != len(want) {
				t.Fatalf("deleteNames = %v, want %v", got.deleteNames, want)
			}
			for i := range want {
				if got.deleteNames[i] != want[i] {
					t.Fatalf("deleteNames = %v, want %v", got.deleteNames, want)
				}
			}
		})
	}
}

func TestBuildCapacityPod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		source            *activationv1alpha1.PoolSource
		priorityClass     string
		wantCheckpoint    string // "" means the annotation must be absent
		wantPriorityClass string
	}{
		{
			name:           "no source leaves checkpoint annotation unset",
			source:         nil,
			wantCheckpoint: "",
		},
		{
			name:           "source with checkpointRef stamps the checkpoint annotation",
			source:         &activationv1alpha1.PoolSource{CheckpointRef: "ckpt-abc"},
			wantCheckpoint: "ckpt-abc",
		},
		{
			name:           "source with empty checkpointRef leaves annotation unset",
			source:         &activationv1alpha1.PoolSource{CheckpointRef: ""},
			wantCheckpoint: "",
		},
		{
			name:              "pool priority class overrides the template",
			source:            &activationv1alpha1.PoolSource{CheckpointRef: "ckpt-xyz"},
			priorityClass:     "high",
			wantCheckpoint:    "ckpt-xyz",
			wantPriorityClass: "high",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool := &activationv1alpha1.ActivationPool{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pool"},
				Spec: activationv1alpha1.ActivationPoolSpec{
					Warm:              activationv1alpha1.WarmSpec{Min: 1, Max: 2},
					Source:            tt.source,
					PriorityClassName: tt.priorityClass,
				},
			}
			template := &v1.PodTemplate{
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels:      map[string]string{"app": "demo"},
						Annotations: map[string]string{"note": "keep"},
					},
					Spec: v1.PodSpec{PriorityClassName: "template-default"},
				},
			}

			pod := buildCapacityPod(pool, template)

			// Pool projection: pool label + warm state are always set.
			if got := pod.Labels[activationv1alpha1.LabelPoolName]; got != "pool" {
				t.Errorf("pool label = %q, want %q", got, "pool")
			}
			if got := pod.Annotations[activationv1alpha1.AnnotationState]; got != activationv1alpha1.StateWarm {
				t.Errorf("state annotation = %q, want %q", got, activationv1alpha1.StateWarm)
			}
			// Template metadata is preserved.
			if got := pod.Labels["app"]; got != "demo" {
				t.Errorf("template label app = %q, want %q", got, "demo")
			}
			if got := pod.Annotations["note"]; got != "keep" {
				t.Errorf("template annotation note = %q, want %q", got, "keep")
			}
			// Checkpoint annotation follows the source.
			got, ok := pod.Annotations[activationv1alpha1.AnnotationCheckpoint]
			if tt.wantCheckpoint == "" {
				if ok {
					t.Errorf("checkpoint annotation = %q, want it absent", got)
				}
			} else if got != tt.wantCheckpoint {
				t.Errorf("checkpoint annotation = %q, want %q", got, tt.wantCheckpoint)
			}
			// Priority class: pool overrides template only when set.
			wantPC := tt.wantPriorityClass
			if wantPC == "" {
				wantPC = "template-default"
			}
			if pod.Spec.PriorityClassName != wantPC {
				t.Errorf("priorityClassName = %q, want %q", pod.Spec.PriorityClassName, wantPC)
			}
		})
	}
}

// TestCoalescedRefill drives the real controller queue: a burst of warm-deficit
// signals (one per bind) arriving inside the pool's coalescing window must
// collapse to a single sync that creates exactly the deficit — not one refill
// per signal. This is the "N Activates in one window -> one sized burst" claim.
func TestCoalescedRefill(t *testing.T) {
	const (
		ns       = "ns"
		poolName = "pool"
		window   = 25 * time.Millisecond
	)

	newPod := func(name, state string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				Labels:    map[string]string{activationv1alpha1.LabelPoolName: poolName},
				Annotations: map[string]string{
					activationv1alpha1.AnnotationState: state,
				},
			},
		}
	}

	tests := []struct {
		name        string
		warmMin     int32
		warmMax     int32
		warm        int // warm pods remaining after the burst
		claimed     int // pods flipped to claimed (== number of bind signals)
		wantCreates int
	}{
		{
			name:        "three concurrent binds coalesce to one refill of three",
			warmMin:     4,
			warmMax:     8,
			warm:        1,
			claimed:     3,
			wantCreates: 3,
		},
		{
			name:        "single bind refills one",
			warmMin:     2,
			warmMax:     4,
			warm:        1,
			claimed:     1,
			wantCreates: 1,
		},
		{
			name:        "burst is capped by warm.max room, not by signal count",
			warmMin:     5,
			warmMax:     6,
			warm:        1,
			claimed:     4, // active=5, room to max=1
			wantCreates: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			pool := &activationv1alpha1.ActivationPool{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: poolName},
				Spec: activationv1alpha1.ActivationPoolSpec{
					TemplateRef: v1.TypedLocalObjectReference{Kind: podTemplateKind, Name: "tmpl"},
					Warm:        activationv1alpha1.WarmSpec{Min: tt.warmMin, Max: tt.warmMax},
					Activation: activationv1alpha1.ActivationSpec{
						Coalescing: &metav1.Duration{Duration: window},
					},
				},
			}
			template := &v1.PodTemplate{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "tmpl"},
				Template:   v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "c", Image: "img"}}}},
			}

			client := fake.NewSimpleClientset()
			// The fake clientset does not honor GenerateName, so every capacity pod
			// would be created with an empty (colliding) name. Assign one so the
			// creates actually land and stay distinct.
			client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				pod := action.(clienttesting.CreateAction).GetObject().(*v1.Pod)
				if pod.Name == "" && pod.GenerateName != "" {
					pod.Name = pod.GenerateName + rand.String(5)
				}
				return false, nil, nil
			})
			factory := informers.NewSharedInformerFactory(client, 0)
			poolInformer := factory.Activation().V1alpha1().ActivationPools()
			podInformer := factory.Core().V1().Pods()
			templateInformer := factory.Core().V1().PodTemplates()

			c, err := NewController(ctx, client, poolInformer, podInformer, templateInformer)
			if err != nil {
				t.Fatalf("NewController: %v", err)
			}

			// Seed the listers directly so the pod event handlers do not fire and
			// pollute the queue: we want to drive the burst explicitly below.
			if err := poolInformer.Informer().GetStore().Add(pool); err != nil {
				t.Fatalf("seed pool: %v", err)
			}
			if err := templateInformer.Informer().GetStore().Add(template); err != nil {
				t.Fatalf("seed template: %v", err)
			}
			for i := 0; i < tt.warm; i++ {
				if err := podInformer.Informer().GetStore().Add(newPod(fmt.Sprintf("warm-%d", i), activationv1alpha1.StateWarm)); err != nil {
					t.Fatalf("seed warm pod: %v", err)
				}
			}
			for i := 0; i < tt.claimed; i++ {
				if err := podInformer.Informer().GetStore().Add(newPod(fmt.Sprintf("claimed-%d", i), activationv1alpha1.StateClaimed)); err != nil {
					t.Fatalf("seed claimed pod: %v", err)
				}
			}

			// Burst: one deferred enqueue per bind, all inside the window.
			for i := 0; i < tt.claimed; i++ {
				c.enqueuePoolAfter(ns, poolName)
			}

			// After the window elapses, the delayed enqueues must have collapsed to
			// a single ready item.
			time.Sleep(4 * window)
			if got := c.queue.Len(); got != 1 {
				t.Fatalf("queue length after burst = %d, want 1 (burst did not coalesce)", got)
			}

			// One sync drains the single item and issues the sized refill.
			if !c.processNextWorkItem(ctx) {
				t.Fatalf("processNextWorkItem returned false")
			}

			creates := 0
			for _, a := range client.Actions() {
				if a.GetVerb() == "create" && a.GetResource().Resource == "pods" {
					creates++
				}
			}
			if creates != tt.wantCreates {
				t.Errorf("pod creates = %d, want %d", creates, tt.wantCreates)
			}
			if got := c.queue.Len(); got != 0 {
				t.Errorf("queue length after sync = %d, want 0", got)
			}
		})
	}
}

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

package activation

import (
	"fmt"
	"testing"
	"time"

	activationapi "k8s.io/activation/apis/v1alpha1"
	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestDurabilitySatisfied(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		offered  []activationv1alpha1.DurabilityTier
		required activationapi.DurabilityTier
		want     bool
	}{
		{
			name:     "empty offered treats as best-effort; best-effort required",
			offered:  nil,
			required: activationapi.DurabilityTier_BEST_EFFORT_DURABLE,
			want:     true,
		},
		{
			name:     "empty offered rejects sync durable",
			offered:  nil,
			required: activationapi.DurabilityTier_SYNC_DURABLE_CREATE,
			want:     false,
		},
		{
			name:     "explicit best-effort only",
			offered:  []activationv1alpha1.DurabilityTier{activationv1alpha1.DurabilityBestEffortDurable},
			required: activationapi.DurabilityTier_BEST_EFFORT_DURABLE,
			want:     true,
		},
		{
			name: "both offered accepts sync",
			offered: []activationv1alpha1.DurabilityTier{
				activationv1alpha1.DurabilityBestEffortDurable,
				activationv1alpha1.DurabilitySyncDurableCreate,
			},
			required: activationapi.DurabilityTier_SYNC_DURABLE_CREATE,
			want:     true,
		},
		{
			name:     "sync-only does not imply best-effort",
			offered:  []activationv1alpha1.DurabilityTier{activationv1alpha1.DurabilitySyncDurableCreate},
			required: activationapi.DurabilityTier_BEST_EFFORT_DURABLE,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool := &activationv1alpha1.ActivationPool{
				Spec: activationv1alpha1.ActivationPoolSpec{
					Durability: activationv1alpha1.DurabilitySpec{Offered: tt.offered},
				},
			}
			got := durabilitySatisfied(durabilityOffered(pool), tt.required)
			if got != tt.want {
				t.Fatalf("durabilitySatisfied() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPoolCacheTryClaimAndTTL(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		warmPods       int
		wantCount      int
		claimOK        bool
		expireAfter    time.Duration
		wantReadyAfter int
	}{
		{
			name:           "claim one of one",
			warmPods:       1,
			wantCount:      1,
			claimOK:        true,
			expireAfter:    DefaultClaimTTL + time.Second,
			wantReadyAfter: 1,
		},
		{
			name:           "shortfall does not partial-claim",
			warmPods:       1,
			wantCount:      2,
			claimOK:        false,
			wantReadyAfter: 1,
		},
		{
			name:           "claim all warm",
			warmPods:       3,
			wantCount:      3,
			claimOK:        true,
			expireAfter:    DefaultClaimTTL + time.Second,
			wantReadyAfter: 3,
		},
		{
			name:           "unexpired claim stays claimed",
			warmPods:       1,
			wantCount:      1,
			claimOK:        true,
			expireAfter:    DefaultClaimTTL / 2,
			wantReadyAfter: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := newPoolCache(DefaultClaimTTL)
			key := poolKey("ns", "pool")
			c.upsertPool(&activationv1alpha1.ActivationPool{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pool"},
				Spec:       activationv1alpha1.ActivationPoolSpec{EndpointPort: 9090},
			})
			for i := 0; i < tt.warmPods; i++ {
				c.observePod(warmCapacityPod(
					types.UID(fmt.Sprintf("uid-%d", i)),
					"ns",
					"pool",
					fmt.Sprintf("10.0.0.%d", i+1),
				))
			}
			claims := c.tryClaim(key, tt.wantCount, now)
			if tt.claimOK && claims == nil {
				t.Fatalf("tryClaim() = nil, want %d claims", tt.wantCount)
			}
			if !tt.claimOK && claims != nil {
				t.Fatalf("tryClaim() = %d claims, want nil", len(claims))
			}
			if tt.claimOK && len(claims) != tt.wantCount {
				t.Fatalf("tryClaim() len = %d, want %d", len(claims), tt.wantCount)
			}
			if tt.claimOK {
				for _, cl := range claims {
					if cl.generation == 0 {
						t.Fatalf("expected non-zero generation")
					}
					if cl.slot.port != 9090 {
						t.Fatalf("port = %d, want 9090", cl.slot.port)
					}
				}
				if c.warmCount(key) != 0 {
					t.Fatalf("warmCount after successful claim = %d, want 0", c.warmCount(key))
				}
			} else if c.warmCount(key) != tt.warmPods {
				t.Fatalf("warmCount after failed claim = %d, want %d", c.warmCount(key), tt.warmPods)
			}
			if tt.expireAfter > 0 {
				c.releaseExpired(now.Add(tt.expireAfter))
				if got := c.warmCount(key); got != tt.wantReadyAfter {
					t.Fatalf("warmCount after TTL = %d, want %d", got, tt.wantReadyAfter)
				}
			}
		})
	}
}

func TestPoolCacheInformerDoesNotClobberClaim(t *testing.T) {
	t.Parallel()
	c := newPoolCache(DefaultClaimTTL)
	key := poolKey("ns", "pool")
	c.upsertPool(&activationv1alpha1.ActivationPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pool"},
	})
	pod := warmCapacityPod("uid-1", "ns", "pool", "10.0.0.1")
	c.observePod(pod)
	if c.tryClaim(key, 1, time.Now()) == nil {
		t.Fatal("expected claim")
	}
	// Resync still shows warm — must not re-admit into ready-set.
	c.observePod(pod)
	if c.warmCount(key) != 0 {
		t.Fatalf("informer re-added claimed pod to ready-set")
	}
}

func TestParsePoolKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "ns/pool", want: "ns/pool"},
		{in: "  ns/pool  ", want: "ns/pool"},
		{in: "pool", wantErr: true},
		{in: "", wantErr: true},
		{in: "ns/pool/extra", wantErr: true},
		{in: "/pool", wantErr: true},
		{in: "ns/", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := parsePoolKey(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parsePoolKey(%q) err = nil, want error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePoolKey(%q) unexpected err: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("parsePoolKey(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestPoolCacheGenerationReconstruct(t *testing.T) {
	t.Parallel()
	const uid = types.UID("uid-1")
	tests := []struct {
		name        string
		state       string
		genAnnot    string
		preexisting uint64
		wantGen     uint64
	}{
		{
			name:     "warm pod seeds generation from annotation",
			state:    StateWarm,
			genAnnot: "5",
			wantGen:  5,
		},
		{
			name:     "claimed pod still seeds generation (not in ready-set)",
			state:    StateClaimed,
			genAnnot: "7",
			wantGen:  7,
		},
		{
			name:     "absent annotation seeds nothing",
			state:    StateWarm,
			genAnnot: "",
			wantGen:  0,
		},
		{
			name:     "malformed annotation seeds nothing",
			state:    StateWarm,
			genAnnot: "not-a-number",
			wantGen:  0,
		},
		{
			name:        "lower annotation does not downgrade a minted counter",
			state:       StateWarm,
			genAnnot:    "3",
			preexisting: 10,
			wantGen:     10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := newPoolCache(DefaultClaimTTL)
			if tt.preexisting > 0 {
				c.generations[uid] = tt.preexisting
			}
			c.observePod(capacityPod(uid, "ns", "pool", "10.0.0.1", tt.state, tt.genAnnot))
			if got := c.generations[uid]; got != tt.wantGen {
				t.Fatalf("generations[%s] = %d, want %d", uid, got, tt.wantGen)
			}
		})
	}
}

func warmCapacityPod(uid types.UID, ns, pool, ip string) *v1.Pod {
	return capacityPod(uid, ns, pool, ip, StateWarm, "")
}

// capacityPod builds a capacity pod with the given state and (optional) raw
// bind-generation annotation. Empty state/gen omits the annotation entirely.
func capacityPod(uid types.UID, ns, pool, ip, state, gen string) *v1.Pod {
	annotations := map[string]string{}
	if state != "" {
		annotations[AnnotationState] = state
	}
	if gen != "" {
		annotations[AnnotationBindGeneration] = gen
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:         uid,
			Name:        string(uid),
			Namespace:   ns,
			Labels:      map[string]string{LabelPoolName: pool},
			Annotations: annotations,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			PodIP: ip,
		},
	}
}

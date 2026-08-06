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
	activationapi "k8s.io/activation/apis/v1alpha1"
	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
)

// durabilityOffered returns the tiers the pool can satisfy. An empty Offered
// list is treated as {BEST_EFFORT_DURABLE} so degraded-mode pools bind without
// an explicit durability stanza.
func durabilityOffered(pool *activationv1alpha1.ActivationPool) []activationv1alpha1.DurabilityTier {
	if pool == nil || len(pool.Spec.Durability.Offered) == 0 {
		return []activationv1alpha1.DurabilityTier{activationv1alpha1.DurabilityBestEffortDurable}
	}
	return pool.Spec.Durability.Offered
}

// durabilitySatisfied reports whether required is in the pool's offered set.
func durabilitySatisfied(offered []activationv1alpha1.DurabilityTier, required activationapi.DurabilityTier) bool {
	want := tierFromProto(required)
	for _, t := range offered {
		if t == want {
			return true
		}
	}
	return false
}

func tierFromProto(t activationapi.DurabilityTier) activationv1alpha1.DurabilityTier {
	switch t {
	case activationapi.DurabilityTier_SYNC_DURABLE_CREATE:
		return activationv1alpha1.DurabilitySyncDurableCreate
	default:
		return activationv1alpha1.DurabilityBestEffortDurable
	}
}

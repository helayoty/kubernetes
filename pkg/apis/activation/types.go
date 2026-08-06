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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core "k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ActivationPool declares warm capacity for a workload template and how that
// pool replenishes.
type ActivationPool struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the desired warm pool.
	Spec ActivationPoolSpec

	// Status is the most recently observed status of the pool.
	// +optional
	Status ActivationPoolStatus
}

// ActivationPoolSpec is the specification of an ActivationPool.
type ActivationPoolSpec struct {
	// TemplateRef references the PodTemplate used for capacity pods in this pool.
	TemplateRef core.TypedLocalObjectReference

	// Warm configures how many restore-ready / idle sandboxes to hold.
	Warm WarmSpec

	// Source optionally identifies a checkpoint used to top up the pool.
	// +optional
	Source *PoolSource

	// Activation configures bind deadlines and demand coalescing.
	// +optional
	Activation ActivationSpec

	// Durability declares which durability tiers this pool can satisfy.
	// +optional
	Durability DurabilitySpec

	// Supply configures how the pool asks for replenishment capacity.
	// +optional
	Supply SupplySpec

	// EndpointPort is the port published in Bound.endpoints for capacity pods.
	// +optional
	EndpointPort int32

	// PriorityClassName is applied to capacity pods created for this pool.
	// +optional
	PriorityClassName string
}

// WarmSpec configures the warm floor and ceiling for a pool.
type WarmSpec struct {
	// Min is the number of warm sandboxes the refill controller should maintain.
	Min int32
	// Max is the upper bound on warm sandboxes. Must be >= Min.
	Max int32
}

// PoolSource identifies optional warm-state provenance for the pool.
type PoolSource struct {
	// CheckpointRef names a checkpoint artifact used for restore hits.
	// +optional
	CheckpointRef string
}

// ActivationSpec configures request-scoped bind behavior for the pool.
type ActivationSpec struct {
	// Deadline is the bind budget before the caller must fall back.
	// +optional
	Deadline *metav1.Duration
	// Coalescing is the window in which concurrent demand is batched.
	// +optional
	Coalescing *metav1.Duration
}

// DurabilitySpec lists durability tiers the pool's runtime offers.
type DurabilitySpec struct {
	// Offered is the set of durability tiers this pool can satisfy.
	// +optional
	Offered []DurabilityTier
}

// DurabilityTier is a durability capability a pool may offer / a request may require.
type DurabilityTier string

const (
	// DurabilityBestEffortDurable means the journal write may lag the bind.
	DurabilityBestEffortDurable DurabilityTier = "BEST_EFFORT_DURABLE"
	// DurabilitySyncDurableCreate means read-your-writes durable before first response.
	DurabilitySyncDurableCreate DurabilityTier = "SYNC_DURABLE_CREATE"
)

// SupplySpec configures how the pool signals demand for replenishment.
type SupplySpec struct {
	// CapacityRequestClass is a WAS provisioning class hook.
	// +optional
	CapacityRequestClass string
	// DeviceClaimScope controls device claim lifetime across activations.
	// +optional
	DeviceClaimScope DeviceClaimScope
}

// DeviceClaimScope is the scope of device claims across activations.
type DeviceClaimScope string

const (
	// DeviceClaimScopePool holds one claim for the pool's lifetime.
	DeviceClaimScopePool DeviceClaimScope = "Pool"
	// DeviceClaimScopePerActivation reallocates per activation.
	DeviceClaimScopePerActivation DeviceClaimScope = "PerActivation"
)

// ActivationPoolStatus is the status of an ActivationPool.
type ActivationPoolStatus struct {
	// Conditions represent the latest available observations of the pool.
	// +optional
	Conditions []metav1.Condition
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ActivationPoolList is a collection of ActivationPool objects.
type ActivationPoolList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items is the list of ActivationPool objects.
	Items []ActivationPool
}

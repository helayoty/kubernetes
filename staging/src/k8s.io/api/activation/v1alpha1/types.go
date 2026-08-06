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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// ActivationPool declares warm capacity for a workload template and how that
// pool replenishes. Routers bind ready capacity from a pool via the Activation
// gRPC contract (k8s.io/activation); this object is the control-plane
// declaration of the pool, not the bind path itself.
// +k8s:supportsSubresource="/status"
type ActivationPool struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec defines the desired warm pool.
	// +required
	Spec ActivationPoolSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// Status is the most recently observed status of the pool.
	// +optional
	Status ActivationPoolStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ActivationPoolSpec is the specification of an ActivationPool.
type ActivationPoolSpec struct {
	// TemplateRef references the PodTemplate used for capacity pods in this
	// pool. The referenced object must be in the same namespace as the pool.
	// +required
	TemplateRef corev1.TypedLocalObjectReference `json:"templateRef" protobuf:"bytes,1,opt,name=templateRef"`

	// Warm configures how many restore-ready / idle sandboxes to hold.
	// +required
	Warm WarmSpec `json:"warm" protobuf:"bytes,2,opt,name=warm"`

	// Source optionally identifies a checkpoint used to top up the pool
	// (KEP-5823). Ignored for Phase 0 degraded-mode (idle pod) pools.
	// +optional
	Source *PoolSource `json:"source,omitempty" protobuf:"bytes,3,opt,name=source"`

	// Activation configures bind deadlines and demand coalescing.
	// +optional
	Activation ActivationSpec `json:"activation,omitempty" protobuf:"bytes,4,opt,name=activation"`

	// Durability declares which durability tiers this pool can satisfy.
	// Activate binds only when the request's required tier is offered here.
	// +optional
	Durability DurabilitySpec `json:"durability,omitempty" protobuf:"bytes,5,opt,name=durability"`

	// Supply configures how the pool asks for replenishment capacity.
	// +optional
	Supply SupplySpec `json:"supply,omitempty" protobuf:"bytes,6,opt,name=supply"`

	// EndpointPort is the port published in Bound.endpoints for capacity pods
	// of this pool. PoC field until endpoints carry richer addressing.
	// +optional
	EndpointPort int32 `json:"endpointPort,omitempty" protobuf:"varint,7,opt,name=endpointPort"`

	// PriorityClassName is applied to capacity pods created for this pool so
	// idle warm capacity remains reclaimable. PoC field.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,8,opt,name=priorityClassName"`
}

// WarmSpec configures the warm floor and ceiling for a pool.
type WarmSpec struct {
	// Min is the number of warm sandboxes the refill controller should try to
	// maintain. Must be >= 0.
	// +required
	Min int32 `json:"min" protobuf:"varint,1,opt,name=min"`

	// Max is the upper bound on warm sandboxes. Must be >= Min.
	// +required
	Max int32 `json:"max" protobuf:"varint,2,opt,name=max"`
}

// PoolSource identifies optional warm-state provenance for the pool.
type PoolSource struct {
	// CheckpointRef names a checkpoint artifact (KEP-5823) used for restore
	// hits. Opaque string for Phase 0; restore path is Phase 2.
	// +optional
	CheckpointRef string `json:"checkpointRef,omitempty" protobuf:"bytes,1,opt,name=checkpointRef"`
}

// ActivationSpec configures request-scoped bind behavior for the pool.
type ActivationSpec struct {
	// Deadline is the bind budget before the caller must fall back.
	// Defaults to 800ms when unset.
	// +optional
	Deadline *metav1.Duration `json:"deadline,omitempty" protobuf:"bytes,1,opt,name=deadline"`

	// Coalescing is the window in which concurrent demand is batched into one
	// refill operation. Defaults to 50ms when unset.
	// +optional
	Coalescing *metav1.Duration `json:"coalescing,omitempty" protobuf:"bytes,2,opt,name=coalescing"`
}

// DurabilitySpec lists durability tiers the pool's runtime offers.
type DurabilitySpec struct {
	// Offered is the set of durability tiers this pool can satisfy.
	// +optional
	// +k8s:optional
	// +listType=atomic
	Offered []DurabilityTier `json:"offered,omitempty" protobuf:"bytes,1,rep,name=offered,casttype=DurabilityTier"`
}

// DurabilityTier is a durability capability a pool may offer / a request may require.
// +enum
type DurabilityTier string

const (
	// DurabilityBestEffortDurable means the journal write may lag the bind.
	// Fenced and crash-safe, but the session can be lost on node death before
	// the journal commits.
	DurabilityBestEffortDurable DurabilityTier = "BEST_EFFORT_DURABLE"

	// DurabilitySyncDurableCreate means read-your-writes durable before first
	// response. Binds only on a durability-capable runtime.
	DurabilitySyncDurableCreate DurabilityTier = "SYNC_DURABLE_CREATE"
)

// SupplySpec configures how the pool signals demand for replenishment.
type SupplySpec struct {
	// CapacityRequestClass is a WAS provisioning class hook. Ignored until
	// CapacityRequest exists in-tree; Phase 0 demand is pending capacity pods.
	// +optional
	CapacityRequestClass string `json:"capacityRequestClass,omitempty" protobuf:"bytes,1,opt,name=capacityRequestClass"`

	// DeviceClaimScope controls whether a ResourceClaim is held for the pool
	// lifetime or reallocated per activation.
	// +optional
	DeviceClaimScope DeviceClaimScope `json:"deviceClaimScope,omitempty" protobuf:"bytes,2,opt,name=deviceClaimScope,casttype=DeviceClaimScope"`
}

// DeviceClaimScope is the scope of device claims across activations.
// +enum
type DeviceClaimScope string

const (
	// DeviceClaimScopePool holds one claim for the pool's lifetime.
	DeviceClaimScopePool DeviceClaimScope = "Pool"
	// DeviceClaimScopePerActivation reallocates per activation (rejected for
	// warm-hit budget reasons in the Phase 0 opening position).
	DeviceClaimScopePerActivation DeviceClaimScope = "PerActivation"
)

// ActivationPoolStatus is the status of an ActivationPool.
type ActivationPoolStatus struct {
	// Conditions represent the latest available observations of the pool.
	// +optional
	// +k8s:optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// ActivationPoolList is a collection of ActivationPool objects.
type ActivationPoolList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items is the list of ActivationPool objects.
	Items []ActivationPool `json:"items" protobuf:"bytes,2,rep,name=items"`
}

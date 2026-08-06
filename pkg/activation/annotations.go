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
	"time"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
)

// Capacity-pod identity and fencing labels/annotations. The canonical definitions
// live in the API package so the refill controller and sandbox can share them
// without importing this (gRPC-heavy) package; re-exported here for the server's
// own use. The refill controller and sandbox own writes; Activate never patches.
const (
	LabelPoolName            = activationv1alpha1.LabelPoolName
	AnnotationState          = activationv1alpha1.AnnotationState
	StateWarm                = activationv1alpha1.StateWarm
	StateClaimed             = activationv1alpha1.StateClaimed
	AnnotationBindGeneration = activationv1alpha1.AnnotationBindGeneration
	AnnotationClaimedAt      = activationv1alpha1.AnnotationClaimedAt
)

// DefaultEndpointPort is used when ActivationPool.spec.endpointPort is unset.
const DefaultEndpointPort int32 = 8080

// DefaultActivateDeadline is used when neither the request nor the pool sets a deadline.
const DefaultActivateDeadline = 800 * time.Millisecond

// DefaultClaimTTL is how long an abandoned in-memory claim stays claimed before
// returning to the warm ready-set.
const DefaultClaimTTL = 30 * time.Second

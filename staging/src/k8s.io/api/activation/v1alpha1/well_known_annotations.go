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

// Well-known labels, annotations, and state values that project the ActivationPool
// contract onto capacity pods. They live in the API package so the refill controller
// (kube-controller-manager), the activation server, and the sandbox fixture can all
// agree on them without importing each other. Writes are owned by the refill
// controller and the sandbox; the Activate hot path never patches the apiserver.
const (
	// LabelPoolName marks a capacity pod as belonging to an ActivationPool.
	// The pool namespace is the pod's namespace.
	LabelPoolName = "activation.k8s.io/pool"

	// AnnotationState is the capacity-pod lifecycle marker observed by the ready-set.
	AnnotationState = "activation.k8s.io/state"

	// AnnotationBindGeneration is the fencing generation the sandbox enforces.
	// Written by the sandbox on bind; reconstructed by the activation server on start.
	AnnotationBindGeneration = "activation.k8s.io/bind-generation"

	// AnnotationClaimedAt records when the capacity pod was claimed (RFC3339).
	AnnotationClaimedAt = "activation.k8s.io/claimed-at"

	// AnnotationCheckpoint records the checkpoint a capacity pod was warmed from
	// (mirrors ActivationPool spec.source.checkpointRef). The refill controller
	// stamps it at pod creation. The scheduler prefers nodes that advertise the
	// same checkpoint as a node label under this same key (checkpoint locality),
	// so a restore hit can reuse a node-local artifact instead of pulling it.
	AnnotationCheckpoint = "activation.k8s.io/checkpoint"
)

// Values for AnnotationState.
const (
	// StateWarm is idle capacity eligible for Activate.
	StateWarm = "warm"
	// StateClaimed is capacity held by an in-flight or live activation.
	StateClaimed = "claimed"
)

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
	"time"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// defaultDeadline is the bind budget applied when spec.activation.deadline is unset.
const defaultDeadline = 800 * time.Millisecond

// defaultCoalescing is the demand-batching window applied when unset.
const defaultCoalescing = 50 * time.Millisecond

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_ActivationPoolSpec fills request-scoped bind defaults so callers
// and the refill controller share the same deadline/coalescing budget.
func SetDefaults_ActivationPoolSpec(obj *activationv1alpha1.ActivationPoolSpec) {
	if obj.Activation.Deadline == nil {
		obj.Activation.Deadline = &metav1.Duration{Duration: defaultDeadline}
	}
	if obj.Activation.Coalescing == nil {
		obj.Activation.Coalescing = &metav1.Duration{Duration: defaultCoalescing}
	}
	if len(obj.Durability.Offered) == 0 {
		obj.Durability.Offered = []activationv1alpha1.DurabilityTier{activationv1alpha1.DurabilityBestEffortDurable}
	}
}

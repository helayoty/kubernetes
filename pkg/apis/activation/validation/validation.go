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

package validation

import (
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/activation"
)

var supportedDurabilityTiers = map[activation.DurabilityTier]bool{
	activation.DurabilityBestEffortDurable: true,
	activation.DurabilitySyncDurableCreate: true,
}

var supportedDeviceClaimScopes = map[activation.DeviceClaimScope]bool{
	"":                                       true,
	activation.DeviceClaimScopePool:          true,
	activation.DeviceClaimScopePerActivation: true,
}

// ValidateActivationPool validates a new ActivationPool. It carries the
// cross-field and enum rules that declarative validation cannot express;
// per-field presence/bounds are enforced by the generated declarative rules.
func ValidateActivationPool(pool *activation.ActivationPool) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateObjectMeta(&pool.ObjectMeta, true, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, validateActivationPoolSpec(&pool.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateActivationPoolUpdate validates an update to an ActivationPool.
func ValidateActivationPoolUpdate(newPool, oldPool *activation.ActivationPool) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateObjectMetaUpdate(&newPool.ObjectMeta, &oldPool.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateActivationPoolSpec(&newPool.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateActivationPoolStatusUpdate validates a status update; spec is immutable here.
func ValidateActivationPoolStatusUpdate(newPool, oldPool *activation.ActivationPool) field.ErrorList {
	allErrs := apimachineryvalidation.ValidateObjectMetaUpdate(&newPool.ObjectMeta, &oldPool.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, metav1validation.ValidateConditions(newPool.Status.Conditions, field.NewPath("status", "conditions"))...)
	return allErrs
}

func validateActivationPoolSpec(spec *activation.ActivationPoolSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	tmplPath := fldPath.Child("templateRef")
	if spec.TemplateRef.Kind == "" {
		allErrs = append(allErrs, field.Required(tmplPath.Child("kind"), ""))
	}
	if spec.TemplateRef.Name == "" {
		allErrs = append(allErrs, field.Required(tmplPath.Child("name"), ""))
	}

	warmPath := fldPath.Child("warm")
	if spec.Warm.Min < 0 {
		allErrs = append(allErrs, field.Invalid(warmPath.Child("min"), spec.Warm.Min, "must be >= 0"))
	}
	if spec.Warm.Max < spec.Warm.Min {
		allErrs = append(allErrs, field.Invalid(warmPath.Child("max"), spec.Warm.Max, "must be >= warm.min"))
	}

	if spec.EndpointPort != 0 {
		for _, msg := range validatePort(int(spec.EndpointPort)) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("endpointPort"), spec.EndpointPort, msg))
		}
	}

	offeredPath := fldPath.Child("durability", "offered")
	for i, tier := range spec.Durability.Offered {
		if !supportedDurabilityTiers[tier] {
			allErrs = append(allErrs, field.NotSupported(offeredPath.Index(i), tier, []string{
				string(activation.DurabilityBestEffortDurable),
				string(activation.DurabilitySyncDurableCreate),
			}))
		}
	}

	if !supportedDeviceClaimScopes[spec.Supply.DeviceClaimScope] {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("supply", "deviceClaimScope"), spec.Supply.DeviceClaimScope, []string{
			string(activation.DeviceClaimScopePool),
			string(activation.DeviceClaimScopePerActivation),
		}))
	}

	return allErrs
}

func validatePort(port int) []string {
	if port < 1 || port > 65535 {
		return []string{"must be between 1 and 65535, inclusive"}
	}
	return nil
}

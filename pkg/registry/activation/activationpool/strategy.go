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
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/activation"
	"k8s.io/kubernetes/pkg/apis/activation/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// strategy implements create/update/delete behavior for ActivationPool.
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default create/update/delete strategy for ActivationPool.
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

var _ rest.RESTCreateStrategy = Strategy
var _ rest.RESTUpdateStrategy = Strategy
var _ rest.RESTDeleteStrategy = Strategy

func (strategy) NamespaceScoped() bool {
	return true
}

// GetResetFields returns the fields the create/update strategy resets.
func (strategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"activation.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
}

// PrepareForCreate clears status on create.
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pool := obj.(*activation.ActivationPool)
	pool.Status = activation.ActivationPoolStatus{}
}

// PrepareForUpdate clears status changes coming through the main resource.
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	pool := obj.(*activation.ActivationPool)
	pool.Status = old.(*activation.ActivationPool).Status
}

func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return validation.ValidateActivationPool(obj.(*activation.ActivationPool))
}

func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

func (strategy) Canonicalize(obj runtime.Object) {}

func (strategy) AllowCreateOnUpdate(ctx context.Context) bool { return false }

func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateActivationPoolUpdate(obj.(*activation.ActivationPool), old.(*activation.ActivationPool))
}

func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string { return nil }

func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool { return true }

type statusStrategy struct {
	strategy
}

// StatusStrategy is the update strategy for the ActivationPool /status subresource.
var StatusStrategy = statusStrategy{Strategy}

// GetResetFields returns the fields the status strategy resets.
func (statusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"activation.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

func (statusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPool := obj.(*activation.ActivationPool)
	oldPool := old.(*activation.ActivationPool)
	newPool.Spec = oldPool.Spec
	metav1.ResetObjectMetaForStatus(&newPool.ObjectMeta, &oldPool.ObjectMeta)
}

func (statusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateActivationPoolStatusUpdate(obj.(*activation.ActivationPool), old.(*activation.ActivationPool))
}

func (statusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

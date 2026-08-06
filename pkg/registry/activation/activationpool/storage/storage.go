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

package storage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	activationapi "k8s.io/kubernetes/pkg/apis/activation"
	"k8s.io/kubernetes/pkg/registry/activation/activationpool"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST is a RESTStorage for ActivationPool.
type REST struct {
	*genericregistry.Store
}

// StatusREST implements the /status subresource for ActivationPool.
type StatusREST struct {
	store *genericregistry.Store
}

var _ rest.StandardStorage = &REST{}
var _ rest.TableConvertor = &REST{}
var _ genericregistry.GenericStore = &REST{}

// NewREST returns REST storage for ActivationPool objects and their status.
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &activationapi.ActivationPool{} },
		NewListFunc:               func() runtime.Object { return &activationapi.ActivationPoolList{} },
		DefaultQualifiedResource:  activationapi.Resource("activationpools"),
		SingularQualifiedResource: activationapi.Resource("activationpool"),

		CreateStrategy:      activationpool.Strategy,
		UpdateStrategy:      activationpool.Strategy,
		DeleteStrategy:      activationpool.Strategy,
		ResetFieldsStrategy: activationpool.Strategy,

		TableConvertor: rest.NewDefaultTableConvertor(activationapi.Resource("activationpools")),
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = activationpool.StatusStrategy
	statusStore.ResetFieldsStrategy = activationpool.StatusStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// New returns an empty ActivationPool.
func (r *StatusREST) New() runtime.Object {
	return &activationapi.ActivationPool{}
}

// Destroy cleans up resources on shutdown.
func (r *StatusREST) Destroy() {
	// The underlying store is shared with REST; do not destroy it here.
}

// Get retrieves the object from storage; required to support Patch.
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update alters the status subset of an object.
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// Subresources never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields implements rest.ResetFieldsStrategy.
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

// ConvertToTable delegates to the shared store.
func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

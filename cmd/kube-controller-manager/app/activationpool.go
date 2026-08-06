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

package app

import (
	"context"
	"fmt"

	"k8s.io/component-base/featuregate"
	"k8s.io/kubernetes/cmd/kube-controller-manager/names"
	"k8s.io/kubernetes/pkg/controller/activationpool"
	"k8s.io/kubernetes/pkg/features"
)

func newActivationPoolControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:        names.ActivationPoolController,
		constructor: newActivationPoolController,
		requiredFeatureGates: []featuregate.Feature{
			features.ActivationPool,
		},
	}
}

func newActivationPoolController(ctx context.Context, controllerContext ControllerContext, controllerName string) (Controller, error) {
	client, err := controllerContext.NewClient(names.ActivationPoolController)
	if err != nil {
		return nil, err
	}

	ctrl, err := activationpool.NewController(
		ctx,
		client,
		controllerContext.InformerFactory.Activation().V1alpha1().ActivationPools(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().PodTemplates(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init activationpool controller: %w", err)
	}

	return newControllerLoop(func(ctx context.Context) {
		ctrl.Run(ctx, 1)
	}, controllerName), nil
}

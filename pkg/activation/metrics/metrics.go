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

package metrics

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const subsystem = "activation_manager"

var (
	registerOnce sync.Once

	// ActivateDuration tracks Activate RPC latency by result (bound|deferred).
	ActivateDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: subsystem,
			Name:      "activate_duration_seconds",
			Help:      "Activate RPC latency in seconds, by result.",
			// 100µs → ~3s covers warm-hit budget and Deferred paths.
			Buckets:        metrics.ExponentialBuckets(0.0001, 2, 16),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)
)

// Register registers activation-manager metrics. Safe to call multiple times.
func Register() {
	registerOnce.Do(func() {
		legacyregistry.MustRegister(ActivateDuration)
	})
}

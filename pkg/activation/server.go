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
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	activationapi "k8s.io/activation/apis/v1alpha1"
	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/activation/metrics"
)

// activationServer implements the Activation gRPC contract against poolCache.
// Authz (TokenReview + SAR) is added in a later task; loopback is OK for now.
type activationServer struct {
	activationapi.UnimplementedActivationServer
	cache  *poolCache
	logger klog.Logger
}

var _ activationapi.ActivationServer = &activationServer{}

func (s *activationServer) Activate(ctx context.Context, req *activationapi.ActivateRequest) (*activationapi.ActivateResponse, error) {
	start := time.Now()
	result := "deferred"
	defer func() {
		metrics.ActivateDuration.WithLabelValues(result).Observe(time.Since(start).Seconds())
	}()

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "ActivateRequest is required")
	}
	key, err := parsePoolKey(req.Pool)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	want := int(req.Count)
	if want == 0 {
		want = 1
	}

	pool := s.cache.getPool(key)
	if pool == nil {
		return deferred("PoolNotFound", fmt.Sprintf("ActivationPool %q not found in cache", key)), nil
	}

	if !durabilitySatisfied(durabilityOffered(pool), req.Durability) {
		return deferred("DurabilityMismatch",
			fmt.Sprintf("required durability %v is not offered by pool %q", req.Durability, key)), nil
	}

	deadline := activateDeadline(ctx, req, pool)
	for {
		now := time.Now()
		if !now.Before(deadline) {
			return deferred("DeadlineExceeded", fmt.Sprintf("no warm capacity in pool %q before deadline", key)), nil
		}
		claims := s.cache.tryClaim(key, want, now)
		if claims != nil {
			result = "bound"
			return boundFromClaims(claims), nil
		}
		waitCh := s.cache.waitCh()
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.Error(codes.Canceled, ctx.Err().Error())
		case <-timer.C:
			return deferred("DeadlineExceeded", fmt.Sprintf("no warm capacity in pool %q before deadline", key)), nil
		case <-waitCh:
			timer.Stop()
		}
	}
}

func (s *activationServer) ReportDemand(_ context.Context, req *activationapi.DemandReport) (*activationapi.DemandAck, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "DemandReport is required")
	}
	key, err := parsePoolKey(req.Pool)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pool := s.cache.getPool(key)
	ack := &activationapi.DemandAck{}
	if pool == nil {
		// Unknown pool: advise strong backpressure so callers back off.
		ack.Backpressure = 100
		return ack, nil
	}
	warm := s.cache.warmCount(key)
	min := int(pool.Spec.Warm.Min)
	deficit := min - warm
	if deficit < 0 {
		deficit = 0
	}
	bp := int(req.Pending) + deficit
	if bp > 100 {
		bp = 100
	}
	ack.Backpressure = uint32(bp)
	return ack, nil
}

func boundFromClaims(claims []claim) *activationapi.ActivateResponse {
	endpoints := make([]*activationapi.Endpoint, 0, len(claims))
	for _, cl := range claims {
		endpoints = append(endpoints, &activationapi.Endpoint{
			Address: cl.slot.podIP,
			Port:    uint32(cl.slot.port),
		})
	}
	first := claims[0]
	return &activationapi.ActivateResponse{
		Result: &activationapi.ActivateResponse_Bound{
			Bound: &activationapi.Bound{
				Endpoints:      endpoints,
				BindGeneration: first.generation,
				SandboxId:      string(first.slot.uid),
			},
		},
	}
}

func deferred(reason, message string) *activationapi.ActivateResponse {
	return &activationapi.ActivateResponse{
		Result: &activationapi.ActivateResponse_Deferred{
			Deferred: &activationapi.Deferred{
				Reason:  reason,
				Message: message,
			},
		},
	}
}

// parsePool splits a "namespace/name" pool reference. Namespace-implied names
// are rejected: the SAR resource is namespaced, so the namespace must be explicit.
func parsePool(pool string) (namespace, name string, err error) {
	pool = strings.TrimSpace(pool)
	if pool == "" {
		return "", "", fmt.Errorf("pool is required (format namespace/name)")
	}
	ns, n, ok := strings.Cut(pool, "/")
	if !ok || ns == "" || n == "" || strings.Contains(n, "/") {
		return "", "", fmt.Errorf("pool %q must be namespace/name", pool)
	}
	return ns, n, nil
}

func parsePoolKey(pool string) (string, error) {
	ns, name, err := parsePool(pool)
	if err != nil {
		return "", err
	}
	return poolKey(ns, name), nil
}

func activateDeadline(ctx context.Context, req *activationapi.ActivateRequest, pool *activationv1alpha1.ActivationPool) time.Time {
	var d time.Duration
	switch {
	case req.DeadlineMs > 0:
		d = time.Duration(req.DeadlineMs) * time.Millisecond
	case pool.Spec.Activation.Deadline != nil && pool.Spec.Activation.Deadline.Duration > 0:
		d = pool.Spec.Activation.Deadline.Duration
	default:
		d = DefaultActivateDeadline
	}
	deadline := time.Now().Add(d)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

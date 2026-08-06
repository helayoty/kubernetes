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
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

const (
	// activateVerb is the custom SAR verb callers need on the target pool.
	// RBAC contract: grant `activate` on `activationpools` (activation.k8s.io),
	// namespaced to the pool. Both Activate and ReportDemand check it.
	activateVerb = "activate"
	// activationGroup / activationPoolsResource identify the SAR resource.
	activationGroup         = "activation.k8s.io"
	activationVersion       = "v1alpha1"
	activationPoolsResource = "activationpools"
)

// poolIdentifier is implemented by request messages that target a pool. The
// generated ActivateRequest / DemandReport both expose GetPool().
type poolIdentifier interface {
	GetPool() string
}

// authInterceptor enforces delegated authn (TokenReview / client-cert) and SAR
// authz on every unary RPC. There is no anonymous/loopback bypass: the Phase 0
// exit criterion requires the bind path to be authorized.
type authInterceptor struct {
	authn authenticator.Request
	authz authorizer.Authorizer
}

func newAuthInterceptor(authn authenticator.Request, authz authorizer.Authorizer) *authInterceptor {
	return &authInterceptor{authn: authn, authz: authz}
}

func (a *authInterceptor) unary(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	u, err := a.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if err := a.authorize(ctx, u, req); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// authenticate bridges the gRPC context into the apiserver's request
// authenticator by synthesizing a minimal *http.Request: the bearer token from
// metadata drives TokenReview, and the peer TLS state drives client-cert x509.
// No in-tree component serves gRPC over delegated auth, so this bridge is the
// seam; it deliberately reuses AuthenticateRequest rather than reimplementing
// TokenReview.
func (a *authInterceptor) authenticate(ctx context.Context) (user.Info, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "building auth request: %v", err)
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("authorization"); len(vals) > 0 {
			r.Header.Set("Authorization", vals[0])
		}
	}
	if p, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			state := tlsInfo.State
			r.TLS = &state
		}
	}

	resp, ok, err := a.authn.AuthenticateRequest(r)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
	}
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "authentication required (provide a bearer token or client certificate)")
	}
	return resp.User, nil
}

// authorize runs a SubjectAccessReview for `activate` on the target pool. The
// pool namespace/name come from the request, so callers can be granted access
// per-pool via namespaced RBAC.
func (a *authInterceptor) authorize(ctx context.Context, u user.Info, req interface{}) error {
	pi, ok := req.(poolIdentifier)
	if !ok {
		return status.Error(codes.Internal, "request does not identify a pool")
	}
	namespace, name, err := parsePool(pi.GetPool())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	attrs := authorizer.AttributesRecord{
		User:            u,
		Verb:            activateVerb,
		APIGroup:        activationGroup,
		APIVersion:      activationVersion,
		Resource:        activationPoolsResource,
		Namespace:       namespace,
		Name:            name,
		ResourceRequest: true,
	}
	decision, reason, err := a.authz.Authorize(ctx, attrs)
	if err != nil {
		return status.Errorf(codes.Internal, "authorization error: %v", err)
	}
	if decision != authorizer.DecisionAllow {
		return status.Errorf(codes.PermissionDenied, "user %q is not allowed to %s %s %q: %s",
			u.GetName(), activateVerb, activationPoolsResource, pi.GetPool(), reason)
	}
	return nil
}

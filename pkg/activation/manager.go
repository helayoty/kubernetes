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

// Package activation implements the standalone activation bind data plane: a
// ready-set of warm capacity pods and a gRPC server for the Activate /
// ReportDemand contract (k8s.io/activation). It is hosted by
// cmd/kube-activation-manager, not the scheduler.
package activation

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	activationapi "k8s.io/activation/apis/v1alpha1"
	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/activation/metrics"
)

// ManagerOptions configures a Manager.
type ManagerOptions struct {
	// Client is used to build the pod + ActivationPool informers.
	Client kubernetes.Interface
	// SecureServing provides the already-bound listener and TLS cert for the
	// Activate gRPC server.
	SecureServing *genericapiserver.SecureServingInfo
	// Authenticator authenticates callers (delegated TokenReview / client-cert).
	Authenticator authenticator.Request
	// Authorizer authorizes callers (delegated SubjectAccessReview).
	Authorizer authorizer.Authorizer
	// ResyncPeriod for the shared informer factory. Zero disables resync.
	ResyncPeriod time.Duration
	// ClaimTTL is how long an abandoned in-memory claim stays claimed before
	// returning to the warm ready-set. Zero uses DefaultClaimTTL.
	ClaimTTL time.Duration
}

// Manager owns the informers, the ready-set/claim ledger, and the Activate gRPC
// server. Its lifecycle is driven by the leader: Run serves on the bound
// listener and blocks until the context is cancelled (lease lost or shutdown).
type Manager struct {
	client        kubernetes.Interface
	informers     informers.SharedInformerFactory
	podInformer   cache.SharedIndexInformer
	poolInformer  cache.SharedIndexInformer
	secureServing *genericapiserver.SecureServingInfo
	authn         authenticator.Request
	authz         authorizer.Authorizer
	cache         *poolCache
}

// NewManager builds a Manager, registers its informer event handlers onto the
// shared pod + ActivationPool informers, and wires the ready-set/claim ledger.
func NewManager(o ManagerOptions) (*Manager, error) {
	if o.Client == nil {
		return nil, fmt.Errorf("activation: Client is required")
	}
	if o.SecureServing == nil || o.SecureServing.Listener == nil {
		return nil, fmt.Errorf("activation: SecureServing with a bound listener is required")
	}
	if o.SecureServing.Cert == nil {
		return nil, fmt.Errorf("activation: SecureServing has no serving cert")
	}
	if o.Authenticator == nil {
		return nil, fmt.Errorf("activation: Authenticator is required (the bind path must be authenticated)")
	}
	if o.Authorizer == nil {
		return nil, fmt.Errorf("activation: Authorizer is required (the bind path must be authorized)")
	}
	metrics.Register()
	factory := informers.NewSharedInformerFactory(o.Client, o.ResyncPeriod)
	m := &Manager{
		client:        o.Client,
		informers:     factory,
		podInformer:   factory.Core().V1().Pods().Informer(),
		poolInformer:  factory.Activation().V1alpha1().ActivationPools().Informer(),
		secureServing: o.SecureServing,
		authn:         o.Authenticator,
		authz:         o.Authorizer,
		cache:         newPoolCache(o.ClaimTTL),
	}
	if err := m.registerHandlers(); err != nil {
		return nil, err
	}
	return m, nil
}

// registerHandlers wires pod + ActivationPool events into the ready-set. Adding
// handlers before the factory starts means the initial list replays as Adds,
// priming the cache without a separate re-List.
func (m *Manager) registerHandlers() error {
	if _, err := m.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*v1.Pod); ok {
				m.cache.observePod(pod)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if pod, ok := newObj.(*v1.Pod); ok {
				m.cache.observePod(pod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				pod, ok = tombstone.Obj.(*v1.Pod)
				if !ok {
					return
				}
			}
			m.cache.deletePod(pod)
		},
	}); err != nil {
		return fmt.Errorf("activation: pod informer handler: %w", err)
	}

	if _, err := m.poolInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pool, ok := obj.(*activationv1alpha1.ActivationPool); ok {
				m.cache.upsertPool(pool)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if pool, ok := newObj.(*activationv1alpha1.ActivationPool); ok {
				m.cache.upsertPool(pool)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pool, ok := obj.(*activationv1alpha1.ActivationPool)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				pool, ok = tombstone.Obj.(*activationv1alpha1.ActivationPool)
				if !ok {
					return
				}
			}
			m.cache.deletePool(pool.Namespace, pool.Name)
		},
	}); err != nil {
		return fmt.Errorf("activation: ActivationPool informer handler: %w", err)
	}
	return nil
}

// Run starts informers, waits for their initial sync, runs the claim TTL
// sweeper, and serves the Activate gRPC contract over TLS until ctx is
// cancelled.
func (m *Manager) Run(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	m.informers.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), m.podInformer.HasSynced, m.poolInformer.HasSynced) {
		return fmt.Errorf("activation: informers failed to sync (is activation.k8s.io installed and discoverable?)")
	}

	// Abandoned claims (client crashed mid-Activate) return to warm after TTL.
	go wait.UntilWithContext(ctx, func(context.Context) {
		if n := m.cache.releaseExpired(time.Now()); n > 0 {
			logger.V(4).Info("Released expired activation claims", "count", n)
		}
	}, time.Second)

	creds, err := m.serverCredentials(ctx)
	if err != nil {
		return err
	}
	interceptor := newAuthInterceptor(m.authn, m.authz)
	srv := grpc.NewServer(grpc.Creds(creds), grpc.ChainUnaryInterceptor(interceptor.unary))
	activationapi.RegisterActivationServer(srv, &activationServer{
		cache:  m.cache,
		logger: logger,
	})

	go func() {
		<-ctx.Done()
		logger.Info("Shutting down Activate gRPC server")
		srv.GracefulStop()
	}()

	logger.Info("Serving Activate gRPC", "address", m.secureServing.Listener.Addr().String())
	if err := srv.Serve(m.secureServing.Listener); err != nil && ctx.Err() == nil {
		return fmt.Errorf("activation: gRPC serve: %w", err)
	}
	return nil
}

// serverCredentials builds gRPC transport credentials backed by the serving
// cert from SecureServingInfo. The apiserver serving stack manages the cert as a
// dynamic provider, so prime it once and read the current pair per handshake
// (cheap, and picks up rotations without a restart).
func (m *Manager) serverCredentials(ctx context.Context) (credentials.TransportCredentials, error) {
	cert := m.secureServing.Cert
	if runner, ok := cert.(dynamiccertificates.ControllerRunner); ok {
		if err := runner.RunOnce(ctx); err != nil {
			return nil, fmt.Errorf("activation: priming serving cert: %w", err)
		}
	}
	tlsConfig := &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certPEM, keyPEM := cert.CurrentCertKeyContent()
			pair, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, err
			}
			return &pair, nil
		},
	}
	if m.secureServing.MinTLSVersion != 0 {
		tlsConfig.MinVersion = m.secureServing.MinTLSVersion
	}
	if len(m.secureServing.CipherSuites) != 0 {
		tlsConfig.CipherSuites = m.secureServing.CipherSuites
	}
	if len(m.secureServing.CurvePreferences) != 0 {
		tlsConfig.CurvePreferences = m.secureServing.CurvePreferences
	}
	return credentials.NewTLS(tlsConfig), nil
}

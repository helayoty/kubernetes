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

package options

import (
	"fmt"
	"net"
	"time"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	componentbaseconfig "k8s.io/component-base/config"
	componentbaseoptions "k8s.io/component-base/config/options"
	"k8s.io/kubernetes/pkg/cluster/ports"
	netutils "k8s.io/utils/net"
)

// componentName is the leader-election identity and lock name.
const componentName = "kube-activation-manager"

// Options holds everything needed to run kube-activation-manager.
type Options struct {
	// Master overrides the apiserver address from kubeconfig.
	Master string
	// ClientConnection bundles the client-side knobs (kubeconfig, content type,
	// QPS/burst), matching the kube-controller-manager pattern. A data-plane
	// server that drives informers benefits from tunable QPS/burst.
	ClientConnection componentbaseconfig.ClientConnectionConfiguration
	// SecureServing owns the bind address, port, and TLS material for the
	// Activate gRPC server. Reusing the apiserver serving stack gives us the
	// standard --bind-address/--secure-port/--tls-* flags and self-signed cert
	// bootstrap, and its ClientCA plumbing is what delegated authn (p0-authz)
	// will hang off of. Serve gRPC over the listener + cert it produces.
	SecureServing *apiserveroptions.SecureServingOptions
	// Authentication configures delegated authn (TokenReview + client-cert)
	// from the server's kubeconfig. The Activate bind path must be authenticated;
	// there is no loopback-without-auth exemption.
	Authentication *apiserveroptions.DelegatingAuthenticationOptions
	// Authorization configures delegated authz (SubjectAccessReview) for the
	// custom `activate` verb on activationpools.
	Authorization *apiserveroptions.DelegatingAuthorizationOptions
	// LeaderElection configures single-active leader election. The in-memory
	// claim ledger + generation minting are only correct with one active
	// writer, so leader election is on by default.
	LeaderElection componentbaseconfig.LeaderElectionConfiguration
}

// NewOptions returns Options with PoC defaults.
func NewOptions() *Options {
	secureServing := apiserveroptions.NewSecureServingOptions()
	// Empty CertDirectory keeps the self-signed pair in memory (same as
	// kube-scheduler) instead of writing it to disk.
	secureServing.ServerCert.CertDirectory = ""
	secureServing.ServerCert.PairName = componentName
	secureServing.BindPort = ports.KubeActivationManagerPort

	return &Options{
		SecureServing:  secureServing,
		Authentication: apiserveroptions.NewDelegatingAuthenticationOptions(),
		Authorization:  apiserveroptions.NewDelegatingAuthorizationOptions(),
		ClientConnection: componentbaseconfig.ClientConnectionConfiguration{
			QPS:   50,
			Burst: 100,
		},
		LeaderElection: componentbaseconfig.LeaderElectionConfiguration{
			LeaderElect:       true,
			LeaseDuration:     metav1.Duration{Duration: 15 * time.Second},
			RenewDeadline:     metav1.Duration{Duration: 10 * time.Second},
			RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},
			ResourceLock:      resourcelock.LeasesResourceLock,
			ResourceName:      componentName,
			ResourceNamespace: metav1.NamespaceSystem,
		},
	}
}

// Flags binds all options to the given FlagSet.
func (o *Options) Flags(fs *pflag.FlagSet) {
	fs.StringVar(&o.ClientConnection.Kubeconfig, "kubeconfig", o.ClientConnection.Kubeconfig, "Path to a kubeconfig with authorization and apiserver location; empty uses in-cluster config.")
	fs.StringVar(&o.Master, "master", o.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig).")
	fs.StringVar(&o.ClientConnection.ContentType, "kube-api-content-type", o.ClientConnection.ContentType, "Content type of requests sent to apiserver.")
	fs.Float32Var(&o.ClientConnection.QPS, "kube-api-qps", o.ClientConnection.QPS, "QPS to use while talking with kubernetes apiserver.")
	fs.Int32Var(&o.ClientConnection.Burst, "kube-api-burst", o.ClientConnection.Burst, "Burst to use while talking with kubernetes apiserver.")
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.Authorization.AddFlags(fs)
	componentbaseoptions.BindLeaderElectionFlags(&o.LeaderElection, fs)
}

// Validate checks option invariants.
func (o *Options) Validate() error {
	var errs []error
	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)
	errs = append(errs, o.Authorization.Validate()...)
	if o.LeaderElection.LeaderElect && o.LeaderElection.ResourceName == "" {
		errs = append(errs, fmt.Errorf("leader election is enabled but resource name is empty"))
	}
	return utilerrors.NewAggregate(errs)
}

// Auth builds the delegated authenticator + authorizer from the server's
// kubeconfig. It must run after the listener is bound (secureServing carries the
// clientCA plumbing that delegated authn attaches to the serving stack).
func (o *Options) Auth(secureServing *genericapiserver.SecureServingInfo) (authenticator.Request, authorizer.Authorizer, error) {
	var authnInfo genericapiserver.AuthenticationInfo
	if err := o.Authentication.ApplyTo(&authnInfo, secureServing, nil); err != nil {
		return nil, nil, fmt.Errorf("applying authentication options: %w", err)
	}
	var authzInfo genericapiserver.AuthorizationInfo
	if err := o.Authorization.ApplyTo(&authzInfo); err != nil {
		return nil, nil, fmt.Errorf("applying authorization options: %w", err)
	}
	return authnInfo.Authenticator, authzInfo.Authorizer, nil
}

// SecureServingInfo defaults a self-signed cert (if none was supplied) and binds
// the listener, returning the resolved serving info. Binding happens here rather
// than at flag-parse time so the caller can defer it to the leader-only serve
// path — followers must not hold the Activate port.
func (o *Options) SecureServingInfo() (*genericapiserver.SecureServingInfo, error) {
	if o.SecureServing == nil {
		return nil, fmt.Errorf("secure serving is not configured")
	}
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("generating self-signed cert: %w", err)
	}
	var info *genericapiserver.SecureServingInfo
	if err := o.SecureServing.ApplyTo(&info); err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("secure serving disabled (--secure-port is 0)")
	}
	return info, nil
}

// RESTConfig builds a *rest.Config from kubeconfig/master or in-cluster config,
// applying the client-connection knobs (content type, QPS, burst).
func (o *Options) RESTConfig() (*rest.Config, error) {
	cfg, err := clientcmd.BuildConfigFromFlags(o.Master, o.ClientConnection.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("building REST config: %w", err)
	}
	cfg.AcceptContentTypes = o.ClientConnection.AcceptContentTypes
	cfg.ContentType = o.ClientConnection.ContentType
	cfg.QPS = o.ClientConnection.QPS
	cfg.Burst = int(o.ClientConnection.Burst)
	return cfg, nil
}

// Client builds a clientset from the resolved REST config.
func (o *Options) Client() (kubernetes.Interface, error) {
	cfg, err := o.RESTConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building clientset: %w", err)
	}
	return client, nil
}

// ComponentName returns the leader-election identity/lock name.
func (o *Options) ComponentName() string {
	return componentName
}

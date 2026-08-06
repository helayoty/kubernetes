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

// Package app wires up the kube-activation-manager binary: client, leader
// election, and the activation Manager that serves the Activate gRPC contract.
package app

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/util/uuid"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-activation-manager/app/options"
	"k8s.io/kubernetes/pkg/activation"
)

// NewActivationManagerCommand builds the root cobra command.
func NewActivationManagerCommand() *cobra.Command {
	opts := options.NewOptions()

	cmd := &cobra.Command{
		Use: "kube-activation-manager",
		Long: `kube-activation-manager serves the synchronous activation contract
(Activate / ReportDemand) against a ready-set of warm capacity pods. It owns the
bind data plane out-of-process from the scheduler: Activate hands out
already-scheduled warm capacity and must not share a failure domain with
scheduling. Run leader-elected single-active; the in-memory claim ledger and
bind-generation minting require exactly one active writer.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()
			if err := opts.Validate(); err != nil {
				return err
			}
			return runCommand(opts)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	fs := cmd.Flags()
	verflag.AddFlags(fs)
	opts.Flags(fs)
	return cmd
}

func runCommand(opts *options.Options) error {
	ctx := genericapiserver.SetupSignalContext()
	client, err := opts.Client()
	if err != nil {
		return err
	}
	return Run(ctx, opts, client)
}

// Run starts the activation Manager, gated by single-active leader election.
// Only the leader binds the listener and serves; followers idle until they win
// the lease. This decouples bind availability from scheduler failover.
func Run(ctx context.Context, opts *options.Options, client kubernetes.Interface) error {
	logger := klog.FromContext(ctx)

	// serve binds the secure listener and runs the manager. It is invoked only on
	// the serve path (single instance, or leader) so followers never hold the
	// Activate port; SecureServing.ApplyTo binds eagerly, so we defer it here.
	serve := func(ctx context.Context) error {
		secureServing, err := opts.SecureServingInfo()
		if err != nil {
			return err
		}
		authn, authz, err := opts.Auth(secureServing)
		if err != nil {
			return err
		}
		mgr, err := activation.NewManager(activation.ManagerOptions{
			Client:        client,
			SecureServing: secureServing,
			Authenticator: authn,
			Authorizer:    authz,
		})
		if err != nil {
			return err
		}
		return mgr.Run(ctx)
	}

	if !opts.LeaderElection.LeaderElect {
		logger.Info("Leader election disabled; running single instance")
		return serve(ctx)
	}

	id, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("getting hostname for leader-election identity: %w", err)
	}
	id = id + "_" + string(uuid.NewUUID())

	lock, err := resourcelock.New(
		opts.LeaderElection.ResourceLock,
		opts.LeaderElection.ResourceNamespace,
		opts.LeaderElection.ResourceName,
		client.CoreV1(),
		client.CoordinationV1(),
		resourcelock.ResourceLockConfig{Identity: id},
	)
	if err != nil {
		return fmt.Errorf("creating leader-election lock: %w", err)
	}

	var runErr error
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   opts.LeaderElection.LeaseDuration.Duration,
		RenewDeadline:   opts.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:     opts.LeaderElection.RetryPeriod.Duration,
		Name:            opts.ComponentName(),
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.Info("Started leading; serving Activate", "identity", id)
				if err := serve(ctx); err != nil {
					runErr = err
					logger.Error(err, "Activation manager exited with error")
				}
			},
			OnStoppedLeading: func() {
				logger.Info("Stopped leading; no longer serving Activate", "identity", id)
			},
		},
	})
	return runErr
}

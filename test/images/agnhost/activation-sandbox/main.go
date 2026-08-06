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

// Package activationsandbox implements the Phase 0 activation sandbox fixture.
// It stands in for a real restore-capable runtime: it enforces bind-generation
// fencing (rejecting stale binds) and marks its own pod state=claimed on bind,
// which is the only apiserver write that turns an in-memory Activate claim into
// state the refill controller can observe (the Activate hot path never patches).
package activationsandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/spf13/cobra"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
)

// bindGenerationHeader carries the fencing generation on an activate request.
const bindGenerationHeader = "X-Bind-Generation"

// defaultPort mirrors the pool's default endpoint port (ActivationPool
// spec.endpointPort) so the fixture and server agree without importing the
// gRPC-heavy server package.
const defaultPort = 8080

var (
	port         int
	podName      string
	podNamespace string
)

// CmdActivationSandbox is used by agnhost Cobra.
var CmdActivationSandbox = &cobra.Command{
	Use:   "activation-sandbox",
	Short: "Serve the Phase 0 activation sandbox fixture (bind-generation fencing)",
	Long: `Serves an HTTP endpoint that stands in for a restore-capable activation runtime.

POST /activate with an "X-Bind-Generation" header binds the sandbox to that
generation. Generations are monotonic: a request carrying a generation older
than the highest one already seen is rejected with 409 Conflict (stale fence).
On an accepted bind the sandbox patches its own pod (state=claimed,
bind-generation, claimed-at) so the refill controller sees the warm deficit.

Meant to run inside a pod: pod identity comes from the downward API (env
POD_NAME/POD_NAMESPACE) and self-patching uses the in-cluster config.`,
	Args: cobra.MaximumNArgs(0),
	Run:  main,
}

func init() {
	CmdActivationSandbox.Flags().IntVar(&port, "port", defaultPort,
		"port to serve the activate endpoint on")
	CmdActivationSandbox.Flags().StringVar(&podName, "pod-name", os.Getenv("POD_NAME"),
		"name of this pod (defaults to the POD_NAME downward-API env var)")
	CmdActivationSandbox.Flags().StringVar(&podNamespace, "pod-namespace", os.Getenv("POD_NAMESPACE"),
		"namespace of this pod (defaults to the POD_NAMESPACE downward-API env var)")
}

func main(cmd *cobra.Command, args []string) {
	logs.InitLogs()
	defer logs.FlushLogs()

	sb := &sandbox{podName: podName, podNamespace: podNamespace}

	// The client is best-effort: without it the fixture still enforces fencing
	// in-memory, it just can't publish state=claimed. This keeps it runnable
	// outside a cluster for local checks.
	if cfg, err := rest.InClusterConfig(); err != nil {
		klog.Warningf("no in-cluster config (%v); running without self-patching", err)
	} else if client, err := kubernetes.NewForConfig(cfg); err != nil {
		klog.Warningf("building clientset failed (%v); running without self-patching", err)
	} else {
		sb.client = client
	}

	// Reconstruct the fence from the pod annotation so a sandbox restart does
	// not accept a generation the previous incarnation already fenced past.
	sb.seedGeneration(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/activate", sb.handleActivate)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/", sb.handleStatus)

	addr := fmt.Sprintf(":%d", port)
	klog.Infof("serving activation sandbox on %s (pod %s/%s)", addr, sb.podNamespace, sb.podName)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		klog.Fatalf("serving failed: %v", err)
	}
}

type sandbox struct {
	client       kubernetes.Interface
	podName      string
	podNamespace string

	mu      sync.Mutex
	highest uint64
	bound   bool
}

// seedGeneration primes the in-memory fence from this pod's persisted
// bind-generation annotation, if any.
func (s *sandbox) seedGeneration(ctx context.Context) {
	if s.client == nil || s.podName == "" || s.podNamespace == "" {
		return
	}
	pod, err := s.client.CoreV1().Pods(s.podNamespace).Get(ctx, s.podName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("seeding generation: getting pod: %v", err)
		return
	}
	raw := pod.Annotations[activationv1alpha1.AnnotationBindGeneration]
	if raw == "" {
		return
	}
	gen, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		klog.Warningf("seeding generation: malformed %s=%q", activationv1alpha1.AnnotationBindGeneration, raw)
		return
	}
	s.mu.Lock()
	s.highest = gen
	s.mu.Unlock()
	klog.Infof("seeded bind generation from pod annotation: %d", gen)
}

func (s *sandbox) handleActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	raw := r.Header.Get(bindGenerationHeader)
	if raw == "" {
		http.Error(w, fmt.Sprintf("missing %s header", bindGenerationHeader), http.StatusBadRequest)
		return
	}
	gen, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid %s: %v", bindGenerationHeader, err), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if s.bound && gen < s.highest {
		current := s.highest
		s.mu.Unlock()
		// Stale fence: a newer generation has already bound this sandbox.
		http.Error(w, fmt.Sprintf("stale bind generation %d; current is %d", gen, current), http.StatusConflict)
		return
	}
	s.highest = gen
	s.bound = true
	s.mu.Unlock()

	if err := s.markClaimed(r.Context(), gen); err != nil {
		// The fence is already advanced in memory; a failed patch only means the
		// controller may not see the claim yet. Surface it so callers can retry.
		klog.Warningf("marking pod claimed: %v", err)
		http.Error(w, fmt.Sprintf("bound generation %d but failed to persist claim: %v", gen, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"sandbox":    s.podName,
		"generation": gen,
	})
}

func (s *sandbox) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	gen, bound := s.highest, s.bound
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"sandbox":    s.podName,
		"namespace":  s.podNamespace,
		"generation": gen,
		"bound":      bound,
	})
}

// markClaimed patches this pod so the refill controller sees a warm deficit.
func (s *sandbox) markClaimed(ctx context.Context, gen uint64) error {
	if s.client == nil {
		return nil
	}
	if s.podName == "" || s.podNamespace == "" {
		return fmt.Errorf("pod identity unknown (set POD_NAME/POD_NAMESPACE via the downward API)")
	}
	annotations := map[string]string{
		activationv1alpha1.AnnotationState:          activationv1alpha1.StateClaimed,
		activationv1alpha1.AnnotationBindGeneration: strconv.FormatUint(gen, 10),
		activationv1alpha1.AnnotationClaimedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
	if err != nil {
		return err
	}
	_, err = s.client.CoreV1().Pods(s.podNamespace).Patch(ctx, s.podName, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

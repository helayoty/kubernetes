# Phase 0 activation PoC — local-up demo

A manual harness that proves the Phase 0 exit criteria of the Synchronous
Activation Contract against a local fork. It is **not** part of CI and builds
nothing; it drives an already-running cluster.

## What it proves

| # | Check       | Exit criterion |
|---|-------------|----------------|
| 1 | warm-hit    | `Activate` returns `Bound` from pre-warmed capacity |
| 2 | SAR-deny    | a caller without the `activate` verb is rejected |
| 3 | miss        | an oversized `Activate` returns `Deferred` (no bind) |
| 4 | fence       | the sandbox rejects a stale `bind_generation` (HTTP 409) |
| 5 | isolation   | an `Activate` burst does not move scheduler attempt counters |
| 6 | durability  | restarting the manager keeps `bind_generation` monotonic (reconstructed from pod annotations, not reset) |

A manual add-on (killing `kube-scheduler` and re-running check 1) demonstrates
that the bind path is independent of the scheduler.

## Prerequisites

1. **Tools on PATH:** `kubectl`, `grpcurl`, `jq`, `curl`.

2. **Binaries built** (no run here — build once):

   ```sh
   make WHAT=cmd/kube-activation-manager
   ```

3. **agnhost image with the `activation-sandbox` subcommand.** The published
   agnhost does not have it yet; build and make it loadable by the cluster:

   ```sh
   # from test/images/agnhost — bump VERSION first if you iterate
   make WHAT=test/images/agnhost   # or your local image-build flow
   docker tag <built-agnhost> agnhost:activation-demo
   ```

   Override the image the manifests use with `AGNHOST_IMAGE=<ref>` (see below).

4. **A fork brought up with the ActivationPool feature enabled.** The apiserver
   must both *serve* the built-in `activation.k8s.io/v1alpha1` API group
   (`RUNTIME_CONFIG`) and have the `ActivationPool` feature gate on
   (`FEATURE_GATES`). Both are required: the REST storage is gated on the
   feature, so runtime-config alone yields an empty group (`NotFound`).

   ```sh
   FEATURE_GATES="ActivationPool=true" \
   RUNTIME_CONFIG="activation.k8s.io/v1alpha1=true" \
     hack/local-up-cluster.sh
   ```

   `FEATURE_GATES` is applied by local-up to the apiserver, controller-manager,
   scheduler, and kubelet, so you do **not** need `KUBE_CONTROLLER_MANAGER_FLAGS`
   for the gate — adding it produces a duplicate `--feature-gates` flag.

   The `activationpool-controller` (KCM) reconciles warm capacity once the pool
   is applied. The `kube-activation-manager` (the Activate gRPC server) is
   started by the demo script itself, not by local-up.

## Run

```sh
# KUBECONFIG defaults to the local-up admin config.
export KUBECONFIG=/var/run/kubernetes/admin.kubeconfig

# Optionally point at your locally-built agnhost image.
export AGNHOST_IMAGE=agnhost:activation-demo

staging/src/k8s.io/activation/examples/localup-demo/run-demo.sh
```

The script applies `manifests.yaml`, starts the manager (logs to
`/tmp/kube-activation-manager.log`), waits for warm capacity, then runs the six
checks and prints `PASS`/`FAIL` per check. It stops the manager on exit.

### Useful overrides

| Env | Default | Purpose |
|-----|---------|---------|
| `KUBECONFIG` | `/var/run/kubernetes/admin.kubeconfig` | cluster access |
| `AGNHOST_IMAGE` | `agnhost:activation-demo` | sandbox image the PodTemplate uses |
| `MANAGER_BIN` | `_output/bin/kube-activation-manager` | manager binary |
| `MANAGER_ADDR` / `MANAGER_PORT` | `localhost:10269` / `10269` | manager gRPC endpoint |
| `SCHED_METRICS` | `https://localhost:10259/metrics` | scheduler metrics for the isolation check |

## Files

- `manifests.yaml` — namespace, RBAC (sandbox self-patch; authorized
  `activate-client`; unauthorized `deny-client`), the `PodTemplate` running
  `agnhost activation-sandbox`, and the `ActivationPool`.
- `run-demo.sh` — the driver described above.
- `FLOW.md` — mermaid diagrams of the demo run and the data-plane path.

## Notes / limits

- The isolation check reads `scheduler_schedule_attempts_total`; if scheduler
  metrics are not reachable it degrades to a manual reminder rather than failing.
- The fence check needs a pod already marked `state=claimed`, which the warm-hit
  check produces; on a cold namespace it is skipped with a note.
- Everything is scoped to the `activation-demo` namespace; `kubectl delete ns
  activation-demo` tears the demo down.

## Troubleshooting

Real problems hit bringing this up on a dev box, and how each was diagnosed and
fixed. Most are `local-up`/environment issues, not the activation code itself.

### `FAIL missing required tool: grpcurl` (or `kind: command not found`)

`go install` drops binaries in `$(go env GOPATH)/bin` (`~/go/bin`), which is
often not on `PATH`. The tool is installed; the shell just can't find it.

```sh
export PATH="$(go env GOPATH)/bin:$PATH"   # add to ~/.zshrc to make it stick
```

### `FAIL activation.k8s.io/v1alpha1 not served` even though the cluster is up

Two independent switches gate the API; check both on the *running* apiserver:

```sh
pgrep -af kube-apiserver | grep -o 'runtime-config=[^ ]*'   # want activation.k8s.io/v1alpha1=true
pgrep -af kube-apiserver | grep -o 'feature-gates=[^ ]*'    # must include ActivationPool=true
```

If `feature-gates` shows only `AllAlpha=false`, you started local-up without
`FEATURE_GATES` (its default). The REST storage returns nothing when the gate is
off, so the group 404s. Restart with **both** `FEATURE_GATES` and
`RUNTIME_CONFIG` set (see prerequisite 4).

### Node never becomes `Ready`; kubelet log shows `system:anonymous`

Symptom in `/tmp/kubelet.log`:

```
"Unable to register node ..." err="... User \"system:anonymous\" cannot get resource \"nodes\" ..."
```

The kubelet is presenting a client cert the apiserver's `client-ca` doesn't
trust, so it's treated as anonymous and can't register. Two common causes:

1. **A stale, root-owned cert dir.** A previous `local-up` run left
   `/var/run/kubernetes` owned by root; a later user-run couldn't overwrite it
   (you'll see `chown: ... Operation not permitted` during startup), leaving a
   mixed cert set from two CAs. Wipe it and restart:

   ```sh
   sudo rm -rf /var/run/kubernetes
   ```

2. **An orphaned old apiserver squatting on `:6443`.** See the next entry.

### Ctrl-C doesn't clean up; stale processes pile up across runs

`local-up` starts the apiserver/kubelet via `sudo`, so Ctrl-C on the script
kills only the foreground bash — the **root-owned** children keep running and
one old apiserver keeps `:6443`. New runs then stack on top (you'll see multiple
apiservers/kubelets). Always tear down explicitly and verify zero before
restarting:

```sh
sudo pkill -9 -f 'hack/local-up-cluster.sh'
sudo pkill -9 -f 'kube-apiserver|kubelet|kube-scheduler|kube-controller-manager|kube-proxy'
pgrep -af 'kube-apiserver|kubelet|kube-scheduler|kube-controller-manager|kube-proxy'   # MUST be empty
```

Tip: `ps -eo pid,ppid,user,etime,comm` distinguishes root orphans (old runs)
from your current run. If `--provider-id=kind://...` shows up, those are a
`kind` cluster in Docker, unrelated to `local-up` — remove it separately with
`kind delete cluster --name <name>`.

### apiserver won't start: `/tmp/kube-apiserver-audit.log` permission denied

A prior run as root created the audit log; the new apiserver can't write it.

```sh
sudo rm -f /tmp/kube-apiserver-audit.log
```

### Build fails with CGO / cross-compiler errors (`aarch64-linux-gnu-gcc: not found`)

`KUBE_BUILD_PLATFORMS` / `PLATFORM` were exported (e.g. in `~/.zshrc`) pinning an
`arm64` target, so an `amd64` host tries to cross-compile CGO binaries. Unset
them for native builds:

```sh
unset KUBE_BUILD_PLATFORMS PLATFORM
```

For a single statically-linked binary that otherwise pulls in CGO (e.g. building
agnhost for the sandbox image), force static without editing shared files:

```sh
KUBE_STATIC_OVERRIDES="agnhost" make WHAT=test/images/agnhost
```

### Fence check fails with `valid=000, stale=000` (sandbox pods CrashLoopBackOff)

`000` is curl's "no HTTP response" — the sandbox isn't listening. Check the pods:

```sh
kubectl -n activation-demo get pods -l activation.k8s.io/pool=demo
kubectl -n activation-demo logs <pod>          # and --previous
```

If the log says:

```
exec /agnhost: no such file or directory
```

the agnhost binary in the image is the wrong architecture **or** dynamically
linked against a loader the (distroless/alpine) base doesn't ship. A host build
defaults to CGO/dynamic; build it static instead (no `make` needed, which would
also try to overwrite the running kubelet/kube-proxy → `Text file busy`):

```sh
unset KUBE_BUILD_PLATFORMS PLATFORM
CGO_ENABLED=0 go build -o _output/bin/agnhost ./test/images/agnhost
file _output/bin/agnhost                        # must say "statically linked"
```

Then repackage the image and reload it into the node's containerd. Because
`imagePullPolicy: IfNotPresent` and the refill controller recreates warm pods
within ~2s, a plain `delete pods` lets the controller re-pin the stale tag
before you can replace it (and `ctr images rm` then fails "in use"). Delete the
pool first so nothing recreates, then swap the image:

```sh
work="$(mktemp -d)"; cp _output/bin/agnhost "$work/agnhost"
printf 'FROM alpine:3.20\nCOPY agnhost /agnhost\nENTRYPOINT ["/agnhost"]\n' >"$work/Dockerfile"
docker build -t agnhost:activation-demo "$work" && rm -rf "$work"

kubectl -n activation-demo delete activationpool demo --ignore-not-found
kubectl -n activation-demo delete pods -l activation.k8s.io/pool=demo --wait=true
sudo ctr -n k8s.io images rm docker.io/library/agnhost:activation-demo
docker save agnhost:activation-demo | sudo ctr -n k8s.io images import -
sudo ctr -n k8s.io images ls | grep agnhost     # confirm; digest should change

# Re-run the demo; it reapplies the pool, so fresh pods pull the new image.
./staging/src/k8s.io/activation/examples/localup-demo/run-demo.sh
```

If pods still `exec ... no such file or directory` after the swap and
`ctr -n k8s.io images ls` shows the new image, the kubelet is using a different
containerd than `ctr -n k8s.io`; check its `--container-runtime-endpoint` and
import into that socket's store.

### `etcd` not found when local-up starts

Install the pinned etcd and put it on `PATH`:

```sh
hack/install-etcd.sh
export PATH="$(pwd)/third_party/etcd:${PATH}"
```

### Clean restart recipe (the reliable sequence)

When in doubt, full teardown → wipe state → restart with the gates:

```sh
sudo pkill -9 -f 'hack/local-up-cluster.sh'
sudo pkill -9 -f 'kube-apiserver|kubelet|kube-scheduler|kube-controller-manager|kube-proxy'
pgrep -af 'kube-apiserver|kubelet|kube-scheduler|kube-controller-manager|kube-proxy'   # confirm empty
sudo rm -rf /var/run/kubernetes
sudo rm -f /tmp/kube-apiserver-audit.log

export PATH="$(go env GOPATH)/bin:$PATH"
unset KUBE_BUILD_PLATFORMS PLATFORM
FEATURE_GATES="ActivationPool=true" \
RUNTIME_CONFIG="activation.k8s.io/v1alpha1=true" \
  hack/local-up-cluster.sh
```

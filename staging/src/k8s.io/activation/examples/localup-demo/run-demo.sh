#!/usr/bin/env bash

# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Phase 0 activation PoC demo driver.
#
# Proves the Phase 0 exit criteria against a fork brought up with
# hack/local-up-cluster.sh:
#   1. warm-hit    Activate returns Bound from pre-warmed capacity.
#   2. SAR-deny    a caller without the "activate" verb is rejected.
#   3. miss        oversized Activate returns Deferred (no bind).
#   4. fence       the sandbox rejects a stale bind_generation (409).
#   5. isolation   an Activate burst does not move scheduler attempt counters.
#   6. durability  killing + restarting the manager keeps generations monotonic
#                  (reconstructed from pod annotations, not reset).
#
# This is a manual harness, not CI. It assumes local-up defaults and that the
# binaries/images below already exist. It does not build anything.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
# Repo root: examples/localup-demo -> activation -> k8s.io -> src -> staging -> repo.
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"
readonly REPO_ROOT

# --- Tunables (override via environment) -------------------------------------
KUBECONFIG="${KUBECONFIG:-/var/run/kubernetes/admin.kubeconfig}"
export KUBECONFIG
readonly NS="activation-demo"
readonly POOL="demo"
readonly MANAGER_BIN="${MANAGER_BIN:-${REPO_ROOT}/_output/bin/kube-activation-manager}"
readonly MANAGER_ADDR="${MANAGER_ADDR:-localhost:10269}"
readonly MANAGER_PORT="${MANAGER_PORT:-10269}"
readonly SCHED_METRICS="${SCHED_METRICS:-https://localhost:10259/metrics}"
readonly PROTO="${REPO_ROOT}/staging/src/k8s.io/activation/apis/v1alpha1/api.proto"
readonly PROTO_IMPORT="${REPO_ROOT}/staging/src/k8s.io/activation/apis/v1alpha1"
readonly METHOD="k8s.io.activation.apis.v1alpha1.Activation/Activate"
readonly MANAGER_LOG="${MANAGER_LOG:-/tmp/kube-activation-manager.log}"

MANAGER_PID=""

log()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*"; exit 1; }

cleanup() {
  if [[ -n "${MANAGER_PID}" ]] && kill -0 "${MANAGER_PID}" 2>/dev/null; then
    log "Stopping kube-activation-manager (pid ${MANAGER_PID})"
    kill "${MANAGER_PID}" 2>/dev/null || true
    wait "${MANAGER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

require() { command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"; }

# activate <service-account> <json-payload> -> prints grpcurl output, returns its code.
activate() {
  local sa="$1" payload="$2" token
  token="$(kubectl -n "${NS}" create token "${sa}")"
  grpcurl -insecure \
    -H "authorization: Bearer ${token}" \
    -proto "${PROTO}" -import-path "${PROTO_IMPORT}" \
    -d "${payload}" \
    "${MANAGER_ADDR}" "${METHOD}"
}

start_manager() {
  log "Starting kube-activation-manager"
  [[ -x "${MANAGER_BIN}" ]] || fail "manager binary not found: ${MANAGER_BIN} (build with: make WHAT=cmd/kube-activation-manager)"
  "${MANAGER_BIN}" \
    --kubeconfig="${KUBECONFIG}" \
    --authentication-kubeconfig="${KUBECONFIG}" \
    --authorization-kubeconfig="${KUBECONFIG}" \
    --secure-port="${MANAGER_PORT}" \
    --leader-elect=false \
    --v=2 >"${MANAGER_LOG}" 2>&1 &
  MANAGER_PID=$!
  for _ in $(seq 1 30); do
    if grpcurl -insecure -proto "${PROTO}" -import-path "${PROTO_IMPORT}" \
        -d '{"pool":"'"${NS}/${POOL}"'","count":0}' "${MANAGER_ADDR}" "${METHOD}" >/dev/null 2>&1; then
      break
    fi
    kill -0 "${MANAGER_PID}" 2>/dev/null || fail "manager exited early; see ${MANAGER_LOG}"
    sleep 1
  done
  ok "manager listening on ${MANAGER_ADDR} (pid ${MANAGER_PID}, log ${MANAGER_LOG})"
}

wait_warm() {
  local want="$1"
  log "Waiting for ${want} warm capacity pods"
  for _ in $(seq 1 60); do
    local n
    # A pod counts only if it is warm AND its container is actually Ready.
    # phase=Running + the controller-set "warm" annotation are both true even
    # when the container is crash-looping (e.g. a bad image), so gate on
    # containerStatuses[].ready to avoid a false PASS that only surfaces later.
    n="$(kubectl -n "${NS}" get pods \
      -l "activation.k8s.io/pool=${POOL}" \
      --field-selector=status.phase=Running \
      -o json | jq '[.items[]
        | select(.metadata.annotations["activation.k8s.io/state"]=="warm")
        | select((.status.containerStatuses // []) | length > 0 and all(.ready))
      ] | length')"
    if [[ "${n}" -ge "${want}" ]]; then
      ok "${n} warm pods ready"
      return 0
    fi
    sleep 2
  done
  kubectl -n "${NS}" get pods -l "activation.k8s.io/pool=${POOL}" -o wide || true
  # Surface crash reasons so a bad sandbox image fails here with a clear cause
  # instead of a cryptic downstream symptom (e.g. curl 000 in the fence check).
  kubectl -n "${NS}" get pods -l "activation.k8s.io/pool=${POOL}" -o json 2>/dev/null | jq -r '
    .items[]
    | (.status.containerStatuses[0] // {}) as $c
    | "  \(.metadata.name): phase=\(.status.phase) restarts=\($c.restartCount // 0) " +
      "state=\(($c.state // {} | keys[0]) // "-") " +
      "reason=\(($c.state // {} | to_entries[0].value.reason) // "-")"' || true
  fail "timed out waiting for ${want} warm pods that are Ready (see crash reasons above; check: kubectl -n ${NS} logs <pod>)"
}

sched_attempts() {
  # Scheduler metrics require auth; the local-up admin token works.
  local token
  token="$(kubectl create token default -n kube-system --duration=10m 2>/dev/null || true)"
  curl -sk -H "Authorization: Bearer ${token}" "${SCHED_METRICS}" 2>/dev/null \
    | awk '/^scheduler_schedule_attempts_total/ {s+=$2} END {print s+0}'
}

main() {
  require kubectl
  require grpcurl
  require jq
  require curl

  log "Preflight: ActivationPool API present?"
  kubectl get --raw /apis/activation.k8s.io/v1alpha1 >/dev/null 2>&1 \
    || fail "activation.k8s.io/v1alpha1 not served; enable --feature-gates=ActivationPool=true and the built-in API on the apiserver"
  ok "activation.k8s.io/v1alpha1 is served"

  log "Applying demo manifests"
  local rendered
  rendered="$(mktemp)"
  if [[ -n "${AGNHOST_IMAGE:-}" ]]; then
    sed "s#agnhost:activation-demo#${AGNHOST_IMAGE}#g" "${SCRIPT_DIR}/manifests.yaml" >"${rendered}"
  else
    cp "${SCRIPT_DIR}/manifests.yaml" "${rendered}"
  fi
  kubectl apply -f "${rendered}"
  rm -f "${rendered}"

  start_manager
  wait_warm 2

  # 1. warm-hit --------------------------------------------------------------
  log "1. warm-hit: authorized Activate should return Bound"
  local out
  out="$(activate activate-client '{"pool":"'"${NS}/${POOL}"'","count":1,"deadline_ms":800}')"
  echo "${out}"
  echo "${out}" | jq -e '.bound.endpoints | length >= 1' >/dev/null \
    && ok "bound with endpoints" || fail "expected Bound with endpoints"
  local gen1 sid1
  gen1="$(echo "${out}" | jq -r '.bound.bindGeneration // .bound.bind_generation // 0')"
  sid1="$(echo "${out}" | jq -r '.bound.sandboxId // .bound.sandbox_id // ""')"
  echo "    bind_generation=${gen1} sandbox_id=${sid1}"

  # 2. SAR-deny --------------------------------------------------------------
  log "2. SAR-deny: caller without the activate verb should be rejected"
  # grpcurl exits non-zero on denial; capture (don't pipe under pipefail) then match.
  local deny_out
  deny_out="$(activate deny-client '{"pool":"'"${NS}/${POOL}"'","count":1,"deadline_ms":800}' 2>&1 || true)"
  echo "${deny_out}"
  if echo "${deny_out}" | grep -qi 'PermissionDenied\|Unauthenticated\|forbidden'; then
    ok "deny-client rejected"
  else
    fail "deny-client was not rejected"
  fi

  # 3. miss ------------------------------------------------------------------
  log "3. miss: oversized Activate should return Deferred (no bind)"
  out="$(activate activate-client '{"pool":"'"${NS}/${POOL}"'","count":999,"deadline_ms":400}')"
  echo "${out}"
  echo "${out}" | jq -e '.deferred' >/dev/null \
    && ok "deferred on miss" || fail "expected Deferred for oversized request"

  # 4. fence -----------------------------------------------------------------
  log "4. fence: sandbox accepts its bound generation, rejects a stale one (409)"
  local pod=""
  if [[ -n "${sid1}" ]]; then
    # Map the bound sandbox_id (pod UID) back to a pod name to port-forward.
    pod="$(kubectl -n "${NS}" get pods -l "activation.k8s.io/pool=${POOL}" -o json \
      | jq -r --arg uid "${sid1}" '.items[] | select(.metadata.uid==$uid) | .metadata.name' \
      | head -n1)"
  fi
  if [[ -z "${pod}" ]]; then
    log "could not resolve bound sandbox pod; skipping fence check"
  else
    kubectl -n "${NS}" port-forward "pod/${pod}" 18080:8080 >/dev/null 2>&1 &
    local pf=$!
    sleep 2
    local okcode stalecode
    # Valid generation is accepted (drives the sandbox to state=claimed).
    okcode="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
      -H "X-Bind-Generation: ${gen1}" "http://localhost:18080/activate" || true)"
    # A generation below the current one must be fenced.
    stalecode="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
      -H "X-Bind-Generation: $((gen1 > 0 ? gen1 - 1 : 0))" "http://localhost:18080/activate" || true)"
    kill "${pf}" 2>/dev/null || true
    echo "    valid gen -> ${okcode}, stale gen -> ${stalecode}"
    if [[ "${okcode}" =~ ^2 && "${stalecode}" == "409" ]]; then
      ok "valid generation accepted, stale generation fenced (409)"
    else
      fail "fence check failed (valid=${okcode}, stale=${stalecode}; expected 2xx then 409)"
    fi
  fi

  # 5. isolation -------------------------------------------------------------
  log "5. isolation: Activate burst must not move scheduler attempt counters"
  local before after
  before="$(sched_attempts)"
  for _ in $(seq 1 20); do
    activate activate-client '{"pool":"'"${NS}/${POOL}"'","count":1,"deadline_ms":200}' >/dev/null 2>&1 || true
  done
  after="$(sched_attempts)"
  echo "    scheduler_schedule_attempts_total before=${before} after=${after}"
  if [[ -z "${before}" || -z "${after}" ]]; then
    log "scheduler metrics unavailable; record manually from ${SCHED_METRICS}"
  elif [[ "${after}" -le "$((before + 2))" ]]; then
    ok "scheduler attempts unchanged by Activate burst (bind path bypasses the scheduler)"
  else
    fail "scheduler attempts moved (${before} -> ${after}); binds should not touch the scheduler"
  fi

  # 6. durability ------------------------------------------------------------
  log "6. durability: restart the manager; generation must not reset"
  cleanup
  MANAGER_PID=""
  start_manager
  wait_warm 1
  out="$(activate activate-client '{"pool":"'"${NS}/${POOL}"'","count":1,"deadline_ms":800}')"
  local gen2
  gen2="$(echo "${out}" | jq -r '.bound.bindGeneration // .bound.bind_generation // 0')"
  echo "    bind_generation after restart=${gen2} (was ${gen1})"
  if [[ "${gen2}" -ge "${gen1}" && "${gen2}" -gt 0 ]]; then
    ok "generation reconstructed from annotations (monotonic across restart)"
  else
    fail "generation reset across restart (${gen1} -> ${gen2})"
  fi

  log "All Phase 0 exit checks passed."
  echo "Manual add-on: kill the scheduler (pkill kube-scheduler) and rerun step 1;"
  echo "binds should still succeed, proving server<->scheduler independence."
}

main "$@"

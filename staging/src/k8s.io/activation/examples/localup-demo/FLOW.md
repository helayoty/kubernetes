# Phase 0 activation PoC — demo flow

Visual companion to [`README.md`](./README.md) and [`run-demo.sh`](./run-demo.sh).
Two views: the demo run (setup + the six exit checks) and the data-plane path
those checks exercise.

## Demo run

```mermaid
flowchart TD
    subgraph SETUP[Setup]
        P0["Preflight: kubectl get --raw /apis/activation.k8s.io/v1alpha1<br/>(feature gate + runtime-config both on)"]
        P1["kubectl apply manifests.yaml<br/>ns • RBAC • PodTemplate • ActivationPool(demo)"]
        P2["KCM activationpool-controller<br/>reconciles warm deficit → creates N warm pods<br/>(agnhost activation-sandbox)"]
        P3["start kube-activation-manager<br/>(Activate gRPC server, :10269)"]
        P4["wait_warm: pods warm AND container Ready"]
        P0 --> P1 --> P2 --> P3 --> P4
    end

    P4 --> C1

    subgraph CHECKS[Phase 0 exit checks]
        C1["1. warm-hit<br/>activate-client → grpcurl Activate(count=1)"]
        C1R{{"Bound: endpoints + bind_generation=1 + sandbox_id"}}
        C1 --> C1R

        C2["2. SAR-deny<br/>deny-client → Activate"]
        C2R{{"authz TokenReview+SAR → PermissionDenied"}}
        C1R --> C2 --> C2R

        C3["3. miss<br/>activate-client → Activate(count=999, deadline 400ms)"]
        C3R{{"Deferred: DeadlineExceeded (no bind)"}}
        C2R --> C3 --> C3R

        C4["4. fence<br/>port-forward sandbox → POST /activate"]
        C4R{{"gen=N → 2xx (state=claimed)<br/>gen=N-1 → 409 Conflict"}}
        C3R --> C4 --> C4R

        C5["5. isolation<br/>burst 20× Activate"]
        C5R{{"scheduler_schedule_attempts_total unchanged<br/>(bind path bypasses scheduler)"}}
        C4R --> C5 --> C5R

        C6["6. durability<br/>kill + restart manager, Activate again"]
        C6R{{"bind_generation ≥ prior, monotonic<br/>(reconstructed from pod annotations)"}}
        C5R --> C6 --> C6R
    end

    C6R --> DONE["All Phase 0 exit checks passed"]
```

## Data-plane path (checks 1, 4, 6)

The write-free hot path, plus the sandbox's self-write on bind.

```mermaid
sequenceDiagram
    participant Cli as activate-client (grpcurl)
    participant Mgr as kube-activation-manager
    participant API as kube-apiserver
    participant SB as sandbox pod (agnhost)
    participant KCM as activationpool-controller

    Note over Mgr: in-memory ready-set + claim ledger<br/>(hot path never writes to API)
    Cli->>Mgr: Activate(pool, count)
    Mgr->>Mgr: authz (TokenReview + SAR verb=activate)
    Mgr->>Mgr: pick warm slot, bump bind_generation
    Mgr-->>Cli: Bound(endpoints, bind_generation, sandbox_id)

    Cli->>SB: POST /activate (X-Bind-Generation)
    alt generation >= highest seen
        SB->>API: patch self → state=claimed, bind-generation, claimed-at
        SB-->>Cli: 2xx
        API-->>KCM: warm deficit observed → refill (new warm pod)
    else stale generation
        SB-->>Cli: 409 Conflict (fenced)
    end

    Note over Mgr,API: on manager restart, generation is rebuilt<br/>from pod annotations → stays monotonic (check 6)
```

## Real-world use case: on-demand warm sandboxes

The demo is deliberately abstract (agnhost stands in for the runtime). Mapped to
a real system — a service that hands each request its own isolated, pre-warmed
sandbox (an AI-agent/code-exec sandbox, a GPU inference slot, a FaaS worker)
with sub-second start — the two lanes look like this:

```mermaid
flowchart LR
    subgraph ASYNC["Async lane — ahead of time (throughput-bound)"]
        OP["operator sets warm.min=N"]
        CTRL["activationpool-controller<br/>creates warm sandboxes"]
        SCHED["kube-scheduler places warm pods<br/>(device attach • checkpoint locality)"]
        WARM["pool of warm, restore-ready sandboxes"]
        OP --> CTRL --> SCHED --> WARM
    end

    subgraph HOT["Hot path — per request (latency-critical)"]
        REQ["user request"]
        ROUTER["router / front door"]
        MGR["kube-activation-manager<br/>in-memory bind, no scheduler, no API write"]
        SB["warm sandbox<br/>fence + serve traffic"]
        REQ --> ROUTER -->|Activate pool| MGR -->|Bound: endpoints| ROUTER
        ROUTER -->|route work| SB
    end

    WARM -. warm slots .-> MGR
    SB -. state=claimed .-> CTRL
    MGR -.->|pool exhausted → Deferred| ROUTER

    classDef async fill:#eef,stroke:#66a;
    classDef hot fill:#efe,stroke:#6a6;
    class OP,CTRL,SCHED,WARM async;
    class REQ,ROUTER,MGR,SB hot;
```

The point: the **expensive** decision (where to place capacity) runs in the async
lane via the scheduler; the **latency-critical** decision (which warm slot to
hand this request) runs in the hot path with an in-memory bind. They are
decoupled — binding thousands of requests never touches `kube-scheduler`.

| Demo (PoC) | Real system |
|---|---|
| `ActivationPool` `warm.min/max` | Hot-pool size: your latency-vs-cost dial per sandbox profile |
| warm `agnhost` pod | Restore-ready sandbox (snapshot/CRIU/Firecracker, model preloaded) |
| `Activate` → `Bound` | Router's "give me a sandbox now" → endpoints to send traffic to |
| `Deferred / DeadlineExceeded` | Pool exhausted in time budget → caller fails fast / falls back |
| SAR `verb=activate` deny | Multi-tenant authz at the bind |
| `bind_generation` fence | Anti–split-brain when slots are reused or the manager restarts |
| `state=claimed` → refill | Replacement warmed the instant a slot is taken |
| scheduler counters flat | Bind throughput independent of scheduler load |

Still fake in Phase 0: the runtime itself (agnhost only fences + self-patches),
checkpoint-locality/preemption scoring (pending P1), device/GPU attach, quotas,
health/eviction, and HA of the claim ledger.

## Two things the diagrams make explicit

- **The hot path (Activate) never writes to the apiserver.** The manager answers
  from in-memory state; the only API write is the sandbox marking *itself*
  `claimed`, which is what lets the controller observe the deficit and refill.
  That decoupling is what check 5 (isolation) proves.
- **`bind_generation` is the fence.** It is monotonic per slot and reconstructed
  from pod annotations on manager restart, which is what checks 4 (fence) and
  6 (durability) lean on.

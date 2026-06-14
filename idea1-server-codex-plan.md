# Idea 1 Server Implementation Plan: Tenant-aware Transfer QoS for Mooncake

This plan is intended to be used by Codex running on the Linux server where the
Mooncake repository is located at `~/Mooncake`.

## 0. Goal

Implement and evaluate a minimal tenant-aware transfer QoS path in Mooncake.

The research question is not simply "short Get should bypass long Put". That
can also happen in a single-tenant system. The multi-tenant framing is:

> In a shared disaggregated KV cache pool, a bulk Put workload from one tenant
> should not destroy the tail latency of latency-sensitive Get requests from
> another tenant, while still allowing the bulk tenant to make fair progress.

The implementation should therefore demonstrate:

- Cross-tenant interference exists in the current baseline.
- Large Put chunking is necessary because a single large submitted transfer
  cannot be preempted after it enters the Transfer Engine.
- Per-tenant scheduling plus op-aware priority can reduce Get tail latency.
- Token bucket / weighted deficit scheduling can preserve fair tenant shares.

## 1. Branch And Safety

Work in the server repository:

```bash
cd ~/Mooncake
git status --short --branch
git checkout main
git pull --ff-only
git checkout -b feat/tenant-aware-transfer-qos
```

Before editing, inspect whether there are uncommitted user changes. Do not
revert unrelated changes.

## 2. Existing Mooncake Code Points

Start by reading these files and confirming the paths on the server branch:

- `mooncake-store/src/client_service.cpp`
- `mooncake-store/src/transfer_task.cpp`
- `mooncake-store/include/client_service.h`
- `mooncake-store/tests/CMakeLists.txt`

Important current path:

- `Client::Get(...)` finds a replica and calls `TransferRead(...)`.
- `Client::Put(...)` calls `PutStart(...)`, then calls `TransferWrite(...)`.
- `Client::TransferData(...)` submits a transfer through `TransferSubmitter`.
- `TransferSubmitter::submitTransfer(...)` allocates a batch ID and calls
  `engine_.submitTransfer(...)`.
- Batch Get / Batch Put submit groups of `TransferRequest`.

Expected implication:

- Mooncake has batching, slicing, metrics, local memcpy, hot cache, and Transfer
  Engine optimizations.
- The Store path does not appear to have a tenant-aware, operation-aware queue
  that can decide cross-tenant order before transfer submission.

## 3. Baseline Experiments First

Before integrating the scheduler, create a small benchmark or extend an existing
client test so it can generate the following workloads.

### 3.1 Single-tenant HOL baseline

Purpose: verify whether long Put affects short Get even without multi-tenancy.
This is only motivation, not the main contribution.

Workload:

- Tenant: `default`
- Background: repeated large Put, for example 256 MB or 1 GB objects.
- Foreground: repeated small Get, for example 1 MB or 4 MB objects.

Metrics:

- Get p50, p95, p99 latency
- Put throughput
- Total transfer bandwidth

Expected result:

- If long Put monopolizes the transfer path, Get p99 rises.

### 3.2 Two-tenant interference baseline

Purpose: prove the multi-tenant problem.

Workload:

- Tenant A: repeated large Put / batch Put.
- Tenant B: repeated small latency-sensitive Get.

Metrics:

- Tenant B Get p50, p95, p99, p999 latency
- Tenant B SLO violation rate, e.g. Get latency > 5 ms or > 10 ms
- Tenant A Put throughput
- Aggregate throughput

Expected result:

- Tenant A bulk Put worsens Tenant B Get tail latency.

### 3.3 Baseline output format

Write CSV or JSON results under a local result directory, for example:

```text
qos_results/
  baseline_single_tenant.csv
  baseline_two_tenant.csv
  README.md
```

Do not commit large binary logs.

## 4. Minimal Scheduler Design

Add a Store-side scheduler before transfers are submitted to the Transfer
Engine. Keep the first implementation narrow and testable.

### 4.1 Request model

Each queued transfer request should carry:

```text
tenant_id
operation_type: Get / Put / Copy / Background
object_key
bytes
slices or transfer descriptor
enqueue_time
priority_class
```

Initial priority:

- Get: high priority
- Put: normal priority
- Copy / background migration: low priority

### 4.2 Scheduling policy

Use a conservative first policy:

```text
per-tenant queues
+ high/normal/background subqueues
+ large Put chunking
+ weighted deficit round robin across tenants
+ token bucket pacing per tenant
+ starvation guard for old normal/background requests
```

Key rule:

- Get priority is applied within and across tenant scheduling.
- Tenant weights and token buckets prevent one tenant's high-rate Get workload
  from starving all other tenants.

### 4.3 Chunking

Chunking is mandatory for large Put. Without chunking, priority cannot help once
a large transfer has already been submitted as one big Transfer Engine batch.

Start with configurable chunk sizes:

- 64 MB
- 16 MB
- 4 MB

Default candidate:

```text
MOONCAKE_QOS_CHUNK_BYTES=16777216
```

## 5. Implementation Phases

### Phase A: Standalone scheduler unit

Add a standalone scheduler class first, without wiring it into the full Store
data path.

Suggested files:

```text
mooncake-store/include/tenant_qos_scheduler.h
mooncake-store/tests/tenant_qos_scheduler_test.cpp
```

Unit tests:

- Splits large Put into chunks.
- Prioritizes short Get over queued Put chunks.
- Weighted deficit scheduling gives configured tenant share.
- Token bucket delays a tenant until refill.
- Starvation guard allows old Put / background work to progress.

This phase can be completed before any RDMA/GPU test.

### Phase B: Local integration point

Add a minimal integration layer around transfer submission.

Candidate hook points:

- `Client::TransferData(...)`
- `Client::TransferRead(...)`
- `Client::TransferWrite(...)`
- `Client::SubmitTransfers(...)` for batch Put
- `TransferSubmitter::submit(...)` / `submit_batch(...)`

Preferred first integration:

- Keep existing direct path as default.
- Add a config/env flag to enable QoS scheduling.
- When disabled, behavior should be unchanged.

Suggested flags:

```text
MOONCAKE_ENABLE_TENANT_QOS=false
MOONCAKE_QOS_CHUNK_BYTES=16777216
MOONCAKE_QOS_DEFAULT_WEIGHT=1
MOONCAKE_QOS_DEFAULT_REFILL_BYTES_PER_MS=0
MOONCAKE_QOS_DEFAULT_BUCKET_BYTES=0
```

Implementation rule:

- Avoid invasive rewrites of `Client::Get` and `Client::Put`.
- Keep scheduler logic isolated so the baseline path remains easy to compare.

### Phase C: Tenant config

Start with environment-based or static config. Do not design a full control
plane yet.

Example:

```text
MOONCAKE_QOS_TENANTS=gold:4,silver:1
MOONCAKE_QOS_REFILL_BYTES_PER_MS=gold:8388608,silver:2097152
MOONCAKE_QOS_BUCKET_BYTES=gold:67108864,silver:16777216
```

If parsing this becomes distracting, hard-code a test config behind an
experimental flag and document it clearly.

### Phase D: End-to-end benchmark

Build a benchmark that can run:

```text
policy=baseline
policy=get_priority_only
policy=tenant_rr
policy=tenant_qos_chunked
```

Required experiment matrix:

| Experiment | Tenant A | Tenant B | Purpose |
| --- | --- | --- | --- |
| E1 | none | small Get | unloaded reference |
| E2 | large Put | small Get | cross-tenant interference |
| E3 | large Put | small Get | QoS improvement |
| E4 | large Put | large Put | fairness / share validation |
| E5 | many small Get | normal Put | starvation check |

Primary metrics:

- Tenant B Get p50/p95/p99/p999
- Tenant B SLO violation rate
- Tenant A Put throughput
- Aggregate throughput
- Tenant bandwidth share
- Scheduler iterations / chunks submitted

## 6. Build And Test Commands

Exact flags may need adjustment for the server environment.

Start with:

```bash
cd ~/Mooncake
cmake -S . -B build-qos -G Ninja \
  -DWITH_TE=ON \
  -DWITH_STORE=ON \
  -DWITH_STORE_RUST=OFF \
  -DWITH_P2P_STORE=OFF \
  -DUSE_CUDA=ON \
  -DBUILD_UNIT_TESTS=ON \
  -DBUILD_EXAMPLES=OFF \
  -DBUILD_BENCHMARK=OFF \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo

cmake --build build-qos --target tenant_qos_scheduler_test
ctest --test-dir build-qos -R tenant_qos_scheduler_test --output-on-failure
```

Then build the smallest existing client/store tests needed to validate the
integration point. Avoid attempting the entire test suite first.

## 7. Success Criteria

Minimum acceptable result:

- Scheduler unit tests pass.
- QoS path is disabled by default.
- Baseline direct path remains unchanged.
- Two-tenant benchmark shows baseline interference.
- QoS policy reduces Tenant B Get p99 latency under Tenant A large Put load.
- Tenant A still makes measurable Put progress.

Stronger result:

- Chunk size sweep shows a clear latency/overhead trade-off.
- Weighted tenant policy produces approximately configured bandwidth shares.
- Get-priority-only baseline is worse than tenant-aware QoS under mixed tenants.

## 8. Things To Avoid

- Do not claim generic token bucket or WRR is the novelty.
- Do not frame the contribution as only "Get before Put".
- Do not introduce a large control plane before the data-path result is proven.
- Do not optimize only the single-tenant case and call it multi-tenant isolation.
- Do not break existing non-QoS behavior.

## 9. Final Deliverables

Produce:

- Code changes on `feat/tenant-aware-transfer-qos`.
- Unit tests for the scheduler.
- A short benchmark README explaining how to reproduce results.
- CSV/JSON benchmark outputs.
- A brief result summary with:
  - baseline two-tenant p99 Get latency;
  - QoS two-tenant p99 Get latency;
  - Put throughput before/after;
  - tenant bandwidth shares.

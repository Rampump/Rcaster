# RCaster Operational Completeness Plan

**Goal:** Close the remaining implementation gaps found while auditing the
current source against `rust-rewrite-plan.md`, without connecting to, querying,
or modifying the four soak servers.

**Global constraints:**

- Do not push to either Git remote.
- Do not commit the user's modified `README.md`.
- Do not touch the four soak servers or their supporting etcd/coordinator.
- Keep operational state in etcd and node-local health in memory; never add
  per-packet etcd traffic.

## Audit Findings

The completed remediation covered source admission, late STR refresh,
cross-Caster STR cache, NEAR, Source Health, management views, protocol bounds,
and large account seeding. A second audit of sections 12.5, 12.9 and 12.10
found these remaining gaps:

1. Prometheus did not expose all currently observable Source Health and slow
   subscriber signals.
2. Batch operations only supported `config.rollback`; users, mountpoints and
   sessions could not be changed through a previewed long operation.
3. Source history events were not exposed as a bounded local event interface.
4. Public/admin browser flows still lacked a repeatable browser-level test.
5. Long-duration soak and real multi-node upgrade/recovery acceptance remain
   external environment work.

## Task 11: Expand Operational Metrics

**Status:** In progress.

- [x] Expose active Source Health warning sources by fixed reason.
- [x] Expose the maximum live-source packet age.
- [x] Count station ID changes and base-position jumps.
- [x] Count subscribers disconnected for exceeding their broadcast backlog.
- [x] Add health endpoint and Hub regression tests.
- [x] Add public monitor/viewer/busy counters.
- [x] Add relay STARTING/failure/idle-close counters.
- [ ] Add relay slow-consumer counters.
- [ ] Add parser queue/drop/framing/resynchronization counters.
- [ ] Add etcd watch-lag, admission-latency and lease-remaining gauges.

Verification:

```powershell
wsl.exe --cd /mnt/e/GitHub/Rcaster -d Debian --user rtk -- bash -lc "cargo test -p rcaster-service --test health --test mountpoint_hub --locked && cargo clippy -p rcaster-service --all-features --all-targets --locked -- -D warnings"
```

## Task 12: Previewed Batch Operations

**Status:** In progress.

Implement the section 12.9 state machine for:

- [x] `users.disable` preview with target user/mountpoint/source/session counts.
- [x] Preview digest covering user, mountpoint, source-generation and session
  target revisions.
- [x] Revalidate the digest before starting and return 409 on drift.
- [x] Return 202 immediately and process targets in a background task.
- [x] Persist queued/running/succeeded/partial/failed/cancelled state and
  per-target results under `runtime/operations/{operation_id}`.
- [x] Idempotent already-disabled handling and revision-CAS user updates.
- [x] Require `admin:write` plus `operations:write`.
- [x] Allow only the creator or `cluster:admin` scope to cancel.
- [ ] Bind operation records to a coordinator lease and adopt them after a
  coordinator restart.
- [x] Add `users.delete`.
- [x] Add `mountpoints.disable`.
- [x] Add owner migration.
- [x] Add `sessions.drop`.
- [x] Add operation retry for failed targets.
- [x] Add dashboard preview/start forms and progress display.

Verification:

```powershell
wsl.exe --cd /mnt/e/GitHub/Rcaster -d Debian --user rtk -- bash -lc "cargo test -p rcaster-service --test admin_api --locked"
```

## Task 13: Source Event Interface

**Status:** Pending.

- [ ] Define a bounded local JSONL source event sink.
- [ ] Emit accepted, rejected, disconnected, owner-changed and health-alert
  events with source generation and connection ID.
- [ ] Expose a paginated read-only admin event query.
- [ ] Rotate and retain events independently from business etcd state.
- [ ] Verify that no event write occurs on the packet hot path.

## Task 14: Browser-Level Web Verification

**Status:** Pending.

- [ ] Start an isolated local standalone and clustered test instance.
- [ ] Exercise admin login, source filtering, health, history, config and
  operation lookup.
- [ ] Exercise public directory, map, login, ticket and monitor WebSocket.
- [ ] Capture console errors and failed network requests.
- [ ] Keep the test independent of the soak deployment.

## Task 15: Final External Acceptance

**Status:** Blocked by explicitly protected external environments.

- [ ] Complete the 72-hour soak without touching the current frozen run.
- [ ] Execute a signed rolling upgrade and rollback across four real nodes.
- [ ] Obtain a traceable real CMR/CMR+ capture.
- [ ] Validate additional receiver and commercial-client models.
- [ ] Validate production DNS/GeoDNS/LB and OCSP behavior.

These items must remain marked pending or partial until their real environment
evidence exists.

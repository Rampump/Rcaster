# Coordinator Admission Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove a single-owner batched admission coordinator can sustain 1000 client/NAT admissions per second while preserving exact quotas and owner fencing.

**Architecture:** One etcd lease-backed coordinator owns all client admission writes for a cluster. Callers send requests through an in-process channel. The coordinator aggregates requests for a short window, performs linear count queries concurrently, allocates available user/IP slots, and commits all accepted sessions in one etcd transaction conditioned on config revision and owner identity.

**Tech Stack:** Rust 2024, Tokio, etcd-client 0.21, etcd 3.5.

**Spec:** `rust-rewrite-plan.md` sections 6.4, 6.10, 15.2 and the 2026-11-22 admission spike report.

## Global Constraints

- Only one owner may commit admission for a cluster.
- The owner key is lease-bound and checked in every batch transaction.
- Batch allocation is serial; quota decisions never race inside a process.
- Session counts are exact linearizable count-only reads.
- Accepted sessions and IP indexes commit with config and owner revision comparisons.
- Owner keepalive failure fences new admissions.
- This prototype covers Client admission; Source remains on the revision-admission path until C supports source batching.

---

### Task 1: Implement the Coordinator

**Files:**
- Create: `crates/rcaster-service/src/coordinator_admission.rs`
- Modify: `crates/rcaster-service/src/admission.rs`
- Modify: `crates/rcaster-service/src/lib.rs`

**Interfaces:**
- Produces: `AdmissionCoordinator::start(Client, cluster_id, CoordinatorOptions)`.
- Produces: `AdmissionCoordinator::admit_client(lease_id, ClientAdmissionRequest)`.
- Produces: `AdmissionCoordinator::shutdown()`.

- [x] Acquire a lease-bound owner key and start keepalive.
- [x] Aggregate requests by config revision and batch window.
- [x] Fetch user/IP counts concurrently and allocate slots in batch order.
- [x] Commit accepted sessions in one owner/config-conditioned transaction.

### Task 2: Verify Correctness and Takeover

**Files:**
- Test: `crates/rcaster-service/tests/coordinator_admission.rs`

- [x] Race 100 casters for one final slot.
- [x] Verify Source sessions do not consume coordinator IP quota.
- [x] Reject stale config without writes.
- [x] Verify owner exclusivity and takeover after lease expiry.

### Task 3: Measure Contended Workloads

**Files:**
- Modify: `crates/rcaster-service/examples/admission_spike.rs`
- Modify: `docs/superpowers/reports/2026-11-22-etcd-admission-spike.md`

- [x] Add `--admission coordinator`.
- [x] Measure independent users at 1000/s.
- [x] Measure one hot user at 1000/s.
- [x] Measure shared NAT IP at 1000/s.
- [x] Measure a 10k-session prefix.
- [x] Record C as the selected contention architecture.

### Task 4: Verify the Gate

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update `README.md` with the coordinator result.

# Revision Admission Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace user/IP admission locks with per-user and per-IP etcd record revision CAS while preserving exact quotas, stale-config rejection, and atomic source claims.

**Architecture:** A stable JSON record exists under each admission scope. Every successful admission rewrites the record, changing its etcd `mod_revision`. Admission reads the record revision and linear session count, then commits session writes only if both the user record and optional IP record revisions are unchanged. The record is a serialization gate, not a cached count, so lease expiry cannot leave a stale quota.

**Tech Stack:** Rust 2024, Tokio, etcd-client 0.21, serde, etcd 3.5.

**Spec:** `rust-rewrite-plan.md` sections 6.4, 6.10, 15.2, and the 2026-11-22 admission spike report.

## Global Constraints

- User scope is always checked before optional IP scope.
- The admission record stores no authoritative session count.
- Session counts come from linearizable count-only reads.
- Every commit compares record `mod_revision`, config revision, and any source-claim generation/revision guard.
- A CAS conflict retries from fresh record and session reads.
- A stale config returns `ConfigStale` without online writes.
- A source claim and owner session remain in one transaction.
- Record keys are persistent and require no lease cleanup; abandoned records are harmless serialization tokens.

---

### Task 1: Add Revision Admission Records

**Files:**
- Create: `crates/rcaster-service/src/revision_admission.rs`
- Modify: `crates/rcaster-service/src/lib.rs`
- Modify: `crates/rcaster-service/src/admission.rs`

**Interfaces:**
- Produces: `admit_client_revision(&mut Client, i64, &str, ClientAdmissionRequest) -> Result<ClientPermit, AdmissionError>`.
- Produces: `admit_source_revision(&mut Client, i64, &str, SourceAdmissionRequest) -> Result<SourcePermit, AdmissionError>`.
- Adds `AdmissionError::AdmissionConflict`.

- [x] Add unit tests for user and IP record keys.
- [x] Run the tests and confirm the module API is missing.
- [x] Implement record load, create-or-CAS compare, record rewrite, and retry.

### Task 2: Commit Client and Source Admission by Revision

**Files:**
- Modify: `crates/rcaster-service/src/revision_admission.rs`
- Test: `crates/rcaster-service/tests/revision_admission.rs`

- [x] Add a test where Client and Source race for one final user slot; exactly one succeeds and exactly one session exists.
- [x] Add a test with 100 concurrent revisions for one final slot; exactly one succeeds.
- [x] Add a test where user admission and shared NAT IP admission both serialize correctly.
- [x] Add stale-config rejection and source-claim rollback tests.
- [x] Implement client session commit using user and IP record CAS.
- [x] Implement source claim commit using the user record CAS plus the existing source claim guards.
- [x] Run all revision-admission tests and confirm they pass.

### Task 3: Compare Lock and Revision Throughput

**Files:**
- Modify: `crates/rcaster-service/examples/admission_spike.rs`
- Modify: `docs/superpowers/reports/2026-11-22-etcd-admission-spike.md`

- [x] Add `--admission lock|revision` to the spike driver.
- [x] Re-run unique-user 1000/s, hot-user 500/1000/s, shared NAT IP 500/s, and 10k-session probes with revision admission.
- [x] Record failures, retries, achieved throughput, p50/p95/p99, and resource behavior.
- [x] Decide whether B is sufficient or C must be designed next.

### Task 4: Verify the Gate

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update `README.md` with the revision-admission result.

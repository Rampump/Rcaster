# User Admission Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that concurrent client and source admissions cannot exceed a user's cluster-wide connection limit and cannot create half-committed online state.

**Architecture:** `admission` owns user/IP lock identities and session index writes. User admission locks serialize counting and commit for one user. Client admission writes client and IP session keys conditionally; source admission extends the existing source-claim transaction with the owner session and lock comparison.

**Tech Stack:** Rust 2024, Tokio, etcd-client 0.21, rand, serde, serde_json, etcd 3.5.

**Spec:** `rust-rewrite-plan.md` sections 6.4, 6.5, 6.6, and 19.2.

## Global Constraints

- Lock order is always user then IP.
- A lock identity is the holder key value, lease ID, and key revision; an in-memory boolean is insufficient.
- Lock values contain 256 random bits and are never logged.
- Session counts are linear reads of live index keys, never a persistent count key.
- Client admission writes user and IP indexes in one transaction.
- Source admission writes the user session, source claim, generation, and source record in one transaction.
- Source connections count toward the owner user limit but never create or consume an IP session.
- Any failed lock comparison rejects the admission; no fallback write is allowed.

---

### Task 1: Add Lock and Session Admission Primitives

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/rcaster-service/Cargo.toml`
- Create: `crates/rcaster-service/src/admission.rs`
- Modify: `crates/rcaster-service/src/lib.rs`
- Test: unit tests in `crates/rcaster-service/src/admission.rs`

**Interfaces:**
- Produces: `try_acquire_user_lock(&mut Client, i64, &str, &str) -> Result<UserLock, AdmissionError>`.
- Produces: `try_acquire_ip_lock(&mut Client, i64, &str, &str) -> Result<IpLock, AdmissionError>`.
- Produces: `read_user_session_count(&mut Client, &str, &str) -> Result<usize, AdmissionError>`.
- Produces: `read_ip_session_count(&mut Client, &str, &str) -> Result<usize, AdmissionError>`.
- Produces: `SessionRecord` with explicit `client` and `source` variants.

- [x] Write tests for lock keys and session keys escaping their etcd key segment.
- [x] Run `cargo test -p rcaster-service admission::tests` and confirm the key validation tests fail.
- [x] Implement key validation and random 256-bit lock owner tokens.
- [x] Run `cargo test -p rcaster-service admission::tests` and confirm the tests pass.

### Task 2: Commit Client and Source Sessions Atomically

**Files:**
- Modify: `crates/rcaster-service/src/source_claim.rs`
- Modify: `crates/rcaster-service/src/admission.rs`
- Test: integration tests in `crates/rcaster-service/tests/user_admission.rs`

**Interfaces:**
- Produces: `admit_client(&mut Client, i64, &str, UserLock, ClientAdmissionRequest) -> Result<ClientPermit, AdmissionError>`.
- Produces: `admit_source(&mut Client, i64, &str, UserLock, SourceAdmissionRequest) -> Result<SourcePermit, AdmissionError>`.
- Modify: `claim_source` to delegate to an internal transaction helper that accepts lock comparisons and extra session operations.

- [x] Write an integration test where one client and one source race for the final user slot; assert exactly one succeeds and one user session exists.
- [x] Run `cargo test -p rcaster-service --test user_admission` and confirm it fails because admission APIs do not exist.
- [x] Implement client transaction writes with user/IP lock comparisons.
- [x] Implement source admission by adding the owner session put and user lock comparison to the claim transaction.
- [x] Run the final-slot race test and confirm it passes.

### Task 3: Prove Lock Expiry and Transaction Failure Safety

**Files:**
- Modify: `crates/rcaster-service/tests/user_admission.rs`

- [x] Add a test where caster A holds a lock for longer than its lease, caster B completes admission, and A's stale commit fails.
- [x] Add a test proving a source claim failure writes no owner session.
- [x] Verify a failed source claim caused by an already-claimed mountpoint leaves the user session count at zero.

- [x] Implement lock release and stale-lock error handling.
- [x] Run the expiry and rollback tests and confirm they pass.

### Task 4: Prove Cluster Limits and IP Isolation

**Files:**
- Modify: `crates/rcaster-service/tests/user_admission.rs`

- [x] Add a test with ten concurrent client admissions for one user and a limit of three; assert exactly three sessions remain.
- [x] Add a test where a source and a client use the same IP and IP limit one; assert the source does not consume the IP slot and the next client is rejected.
- [x] Run all integration tests and confirm they pass.

### Task 5: Verify the Gate

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update `README.md` with the user-admission prototype status.

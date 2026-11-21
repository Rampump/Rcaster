# Configuration Revision Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that Client and Source admission cannot commit with a stale Caster Config snapshot and resume after loading the current revision.

**Architecture:** A minimal `CasterConfig` record is stored at `config/caster`. `ConfigSnapshot` retains its generation and etcd revision. Client and Source admission transactions compare that revision before writing any session or source state.

**Tech Stack:** Rust 2024, Tokio, etcd-client 0.21, serde, serde_json, etcd 3.5.

**Spec:** `rust-rewrite-plan.md` sections 5.2, 5.5, 5.6, 6.10, and 19.3.

## Global Constraints

- Caster Config has a monotonically increasing generation.
- `ConfigSnapshot` stores the etcd `mod_revision` used to authorize admission.
- Security admission compares the current `config/caster` revision in the same transaction that creates a session or source.
- A stale config snapshot returns a dedicated retryable error and writes no online state.
- Reloading the current config revision restores admission.
- Sourcetable and NEAR eventual consistency are outside this prototype; security admission never relies on stale config.

---

### Task 1: Add Config Snapshot Storage

**Files:**
- Create: `crates/rcaster-service/src/config.rs`
- Modify: `crates/rcaster-service/src/lib.rs`
- Test: unit tests in `crates/rcaster-service/src/config.rs`

**Interfaces:**
- Produces: `CasterConfig { schema_version: u8, generation: u64 }`.
- Produces: `ConfigSnapshot { config: CasterConfig, revision: i64 }`.
- Produces: `publish_config(&mut Client, &str, CasterConfig) -> Result<ConfigSnapshot, ConfigError>`.
- Produces: `load_config_snapshot(&mut Client, &str) -> Result<ConfigSnapshot, ConfigError>`.

- [x] Add unit tests for config key validation and invalid stored JSON.
- [x] Run `cargo test -p rcaster-service config::tests` and confirm the API is missing.
- [x] Implement config publish and load using the `config/caster` key.
- [x] Run the config unit tests and confirm they pass.

### Task 2: Guard Client and Source Admission with Config Revision

**Files:**
- Modify: `crates/rcaster-service/src/admission.rs`
- Modify: `crates/rcaster-service/tests/user_admission.rs`
- Test: `crates/rcaster-service/tests/config_revision.rs`

**Interfaces:**
- Modify: `ClientAdmissionRequest::new(..., ConfigSnapshot)`.
- Modify: `SourceAdmissionRequest::new(..., ConfigSnapshot)`.
- Produces: `AdmissionError::ConfigStale`.
- Adds the `config/caster` `mod_revision` comparison to every client and source admission transaction.

- [x] Write a client test that publishes generation 1, updates to generation 2, rejects admission with snapshot 1, verifies no session, reloads snapshot 2, and admits successfully.
- [x] Write a source test with the same stale/current transition and verify no claim or owner session remains after rejection.
- [x] Run `cargo test -p rcaster-service --test config_revision` and confirm both tests fail.
- [x] Add config revision comparisons and stale-error mapping to client admission.
- [x] Add config revision comparison and stale-error mapping to source admission.
- [x] Run the config revision integration tests and confirm they pass.

### Task 3: Regress the Complete Gate

- [x] Update all user-admission tests to supply an explicit current ConfigSnapshot.
- [x] Verify a duplicate or lower Caster Config generation is rejected.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update `README.md` with the configuration revision prototype status.

# Source Fencing Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the Gate 1 source-fencing invariant against a real etcd instance before implementing the complete caster.

**Architecture:** `rcaster-core` owns the in-process `FenceGuard`. `rcaster-service` owns etcd source-claim persistence and conditional publish/cleanup operations. An integration test uses two leases to prove that a stale caster cannot publish after another caster takes over.

**Tech Stack:** Rust 2024, Tokio, etcd-client 0.21, serde, serde_json, Docker Compose with etcd 3.5.

**Spec:** `rust-rewrite-plan.md`

## Global Constraints

- `source-generations/{mount}` is persistent and monotonically increasing.
- A source claim can only be created while no claim exists for that mountpoint.
- Claim, next generation, and `VALIDATING` source record are written in one etcd transaction.
- Every claim update or cleanup compares the claim revision, generation, connection ID, and runtime epoch.
- A stale connection can close only its own socket; it cannot overwrite or delete a newer source.
- No source packet is dispatched after the lease deadline or with a stale local generation.
- The prototype models the cooperative-node guarantee only; it does not claim Byzantine or external-ingress fencing.

---

### Task 1: Bootstrap the Workspace and Guard Logic

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `crates/rcaster-core/Cargo.toml`
- Create: `crates/rcaster-core/src/lib.rs`
- Test: unit tests in `crates/rcaster-core/src/lib.rs`

**Interfaces:**
- Produces: `rcaster_core::FenceGuard::new(deadline: Instant, generation: u64)`.
- Produces: `FenceGuard::allow_dispatch(now: Instant, generation: u64) -> bool`.
- Produces: `FenceGuard::is_fenced() -> bool`.

- [x] Write unit tests for valid dispatch, stale generation, and expired deadline.
- [x] Run `cargo test -p rcaster-core` and confirm the tests fail because `FenceGuard` does not exist.
- [x] Implement the minimal guard with permanent local fencing after a failed check.
- [x] Run `cargo test -p rcaster-core` and confirm all guard tests pass.

### Task 2: Implement Atomic Source Claims in etcd

**Files:**
- Create: `compose.yaml`
- Create: `crates/rcaster-service/Cargo.toml`
- Create: `crates/rcaster-service/src/lib.rs`
- Create: `crates/rcaster-service/src/source_claim.rs`
- Test: integration tests in `crates/rcaster-service/tests/source_fencing.rs`

**Interfaces:**
- Produces: `SourceClaimRequest` containing mountpoint, caster, connection, runtime epoch, relay endpoint, effective user, and creation timestamp.
- Produces: `SourceClaim` containing generation and fencing identity.
- Produces: `ClaimedSource` containing the claim and revision pins.
- Produces: `claim_source(&mut Client, i64, &str, SourceClaimRequest) -> Result<ClaimedSource, ClaimError>`.
- Produces: `publish_active(&mut Client, &ClaimedSource) -> Result<(), ClaimError>`.
- Produces: `release_source(&mut Client, &ClaimedSource) -> Result<(), ClaimError>`.

- [x] Write an integration test that starts from the configured etcd endpoint and proves a second claim is rejected while the first lease is alive.
- [x] Run `cargo test -p rcaster-service --test source_fencing` and confirm it fails because the claim API does not exist.
- [x] Implement claim creation with generation CAS and a single conditional transaction.
- [x] Run the live-claim test and confirm it passes.
- [x] Add the takeover test: revoke caster A's lease, claim from caster B, then prove A cannot publish or clean up B's source.
- [x] Run the takeover test against the conditional publish and cleanup operations.
- [x] Implement conditional `ACTIVE` publish and release.
- [x] Run all `rcaster-service` integration tests and confirm they pass.

### Task 3: Verify the Gate

**Files:**
- Modify: `README.md` (only if the final commands cannot stand alone in the plan)

- [x] Start etcd with Compose or an equivalent local process.
- [x] Wait for etcd health to pass.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Record the observed result of the Gate 1 plan step for source fencing.

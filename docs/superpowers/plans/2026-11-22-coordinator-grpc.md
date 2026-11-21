# Coordinator gRPC Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the batched admission coordinator through a typed gRPC API so Caster processes can use it as an independent service.

**Architecture:** A protobuf service carries client-admission identity, limits, and config revision. The server wraps the existing `AdmissionCoordinator`; the client converts typed Rust requests and errors. This milestone runs insecure loopback for correctness only; mTLS and service-token authorization are the next security milestone.

**Tech Stack:** Rust 2024, tonic 0.14, prost 0.14, Tokio, etcd 3.5.

**Spec:** `rust-rewrite-plan.md` section 6.4, section 15.2, and the coordinator admission report.

## Global Constraints

- The gRPC API accepts only client admission in this milestone.
- Expected quota/config errors remain typed application responses, not transport failures.
- The server does not own session state independently; it delegates to `AdmissionCoordinator`.
- Requests include config generation and etcd revision so the coordinator can still reject stale policy.
- TLS, service identity, authorization, and rate limiting are explicitly deferred.

---

### Task 1: Define the Protobuf Contract

**Files:**
- Create: `proto/admission_coordinator.proto`
- Create: `crates/rcaster-service/build.rs`
- Modify: `Cargo.toml`
- Modify: `crates/rcaster-service/Cargo.toml`

- [x] Define `AdmitClient` request/response messages and a typed error code.
- [x] Generate tonic client and server code in the build script.

### Task 2: Implement Server and Client Adapters

**Files:**
- Create: `crates/rcaster-service/src/coordinator_grpc.rs`
- Modify: `crates/rcaster-service/src/lib.rs`

- [x] Convert protobuf requests to `ClientAdmissionRequest`.
- [x] Delegate to `AdmissionCoordinator::admit_client`.
- [x] Convert expected admission errors into structured gRPC responses.
- [x] Provide a cloneable `GrpcAdmissionClient`.

### Task 3: Verify Transport Behavior

**Files:**
- Test: `crates/rcaster-service/tests/coordinator_grpc.rs`

- [x] Start a loopback gRPC server backed by a real coordinator and etcd.
- [x] Admit one client through gRPC and verify the etcd session exists.
- [x] Verify the final slot returns `USER_QUOTA_EXCEEDED` as an application response.
- [x] Verify stale config returns `CONFIG_STALE`.

### Task 4: Verify the Gate

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update `README.md` with the gRPC milestone.

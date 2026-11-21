# Coordinator mTLS and Service Token Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Require mutual TLS and a cluster-scoped `admission:write` service token before any admission request reaches the coordinator.

**Architecture:** Tonic verifies server and client certificates against an internal CA. A second metadata interceptor validates a constant-time pre-shared service token, cluster ID, and required scope. Expected application errors remain response messages; authentication and authorization failures remain gRPC statuses.

**Tech Stack:** Rust 2024, tonic 0.14 TLS-ring, rustls, subtle, rcgen test fixtures, etcd 3.5.

**Spec:** `rust-rewrite-plan.md` sections 2.4, 14, 15.2, and internal service identity requirements.

## Global Constraints

- Server TLS requires a client certificate signed by the configured internal CA.
- `admission:write` is mandatory for the admission RPC.
- Tokens are cluster-scoped and compared without early-exit string equality.
- Token values and private keys are never logged.
- Authentication occurs before the request enters `AdmissionCoordinator`.
- Test certificates are generated in memory and are not committed as reusable credentials.

---

### Task 1: Add TLS and Token Policy

- [x] Add server/client TLS option types.
- [x] Add cluster-scoped service tokens and constant-time comparison.
- [x] Add client metadata injection and server authorization interceptor.

### Task 2: Verify Security Boundaries

- [x] Verify a valid client certificate and `admission:write` token can admit.
- [x] Verify an invalid token returns `Unauthenticated`.
- [x] Verify missing scope and wrong cluster return `PermissionDenied`.
- [x] Verify a TLS client without a client certificate cannot complete admission.
- [x] Verify rejected requests create no session.

### Task 3: Verify the Gate

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update `README.md` and the admission report.

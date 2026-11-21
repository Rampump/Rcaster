# Admission Token Rotation and Rate Limit Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rotate admission service tokens without restarting the gRPC service and enforce an optional per-token request rate.

**Architecture:** `AdmissionAccessPolicy` clones share a locked policy snapshot. Token replacement is atomic from RPC perspective: each request uses one snapshot. Each service token can own a token bucket shared by client connections using that token.

**Tech Stack:** Rust 2024, standard synchronization primitives, tonic, etcd.

## Global Constraints

- Rotation removes old tokens immediately for new RPCs.
- Tokens remain cluster-scoped and require `admission:write`.
- Token comparison remains constant-time.
- Rate limits are per token, not per connection.
- A rate-limit rejection creates no session or source claim.
- Token values and bucket internals are not logged.

---

### Task 1: Add Shared Rotatable Policy

- [x] Replace copied token vectors with an `Arc<RwLock<...>>`.
- [x] Add `replace_tokens` and prove old/new token behavior.

### Task 2: Add Token Buckets

- [x] Add optional per-token rate and burst configuration.
- [x] Reject exhausted buckets with gRPC `ResourceExhausted`.
- [x] Map the client result to a typed admission error.

### Task 3: Verify

- [x] Test old-token denial and new-token admission after live rotation.
- [x] Test rate limit rejection without online state changes.
- [x] Run format, Clippy, and full workspace tests.
- [x] Update README and admission report.

# Coordinator Source Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route Source claims through the same lease-fenced coordinator that owns client/NAT admission, preserving shared user quotas and atomic claim/generation/session writes.

**Architecture:** Extract source-claim transaction plans from the single-claim helper. Mixed coordinator batches allocate user slots across clients and sources, then append every accepted Source claim plan to the same owner/config-conditioned transaction as client sessions. A new `AdmitSource` RPC exposes the path remotely.

**Tech Stack:** Rust 2024, Tokio, etcd-client, tonic, etcd 3.5.

**Spec:** `rust-rewrite-plan.md` sections 6.4, 6.6, and 15.2.

## Global Constraints

- Client and Source admissions share one effective user limit path.
- Source does not consume IP quotas.
- Source claim, generation, owner session, and VALIDATING route record commit atomically.
- Duplicate mountpoints in one batch admit at most one source.
- Owner and config revision conditions apply to the entire transaction.
- Default batch size is reduced to 16 so batches of Source transaction operations remain bounded.

---

### Task 1: Extract Source Claim Plans

- [x] Add a prepared source-claim representation with conditions and operations.
- [x] Reuse it from the existing single-claim API.
- [x] Preserve claim retry and stale-guard behavior.

### Task 2: Add Mixed Coordinator Batching

- [x] Add `AdmissionCoordinator::admit_source`.
- [x] Allocate user slots across mixed Client/Source batch entries.
- [x] Prepare Source claims and append them to the batch transaction.
- [x] Reject duplicate mountpoints without consuming a user slot.
- [x] Return finalized Source generation from the transaction revision.

### Task 3: Expose Source over gRPC

- [x] Add `AdmitSource` protobuf request/response and source generation result.
- [x] Add server-side source request conversion and delegation.
- [x] Add cloneable client `admit_source`.

### Task 4: Verify

- [x] Test Client/Source final-slot competition.
- [x] Test multiple Source claims in one coordinator batch.
- [x] Test duplicate Source claim rejection.
- [x] Test Source admission through gRPC.
- [x] Re-run hot 1000/s after lowering batch size.
- [x] Run format, Clippy, and full workspace tests.

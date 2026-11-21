# Etcd Admission Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure the current etcd lock-based admission design at 100, 500, and 1000 offered admissions per second, including hot-user lock contention and single-member failure recovery.

**Architecture:** Add a reusable Rust example that drives `admit_client` at an open-loop target rate and records success, lock retries, failures, achieved throughput, and p50/p95/p99 latency. Run it against a three-member local etcd cluster. Keep spike-only process control outside the production library.

**Tech Stack:** Rust 2024, Tokio, etcd-client 0.21, three local etcd 3.5.21 processes.

**Spec:** `rust-rewrite-plan.md` sections 6.4, 15.2, Phase 0, and 19.2.

## Global Constraints

- Session quota reads are linearizable.
- Count queries must not transfer every session value.
- The benchmark uses the same `try_acquire_user_lock`, count, transaction, and release path as admission.
- Report offered and achieved rates separately; a slow system must not be reported as if it sustained the target rate.
- Record p50/p95/p99, lock-busy retries, errors, and elapsed time.
- Run unique-user and hot-user workloads.
- Preserve quorum failure recovery evidence instead of claiming three-node availability from a single-node run.

---

### Task 1: Optimize Session Count Queries

**Files:**
- Modify: `crates/rcaster-service/src/admission.rs`

**Interfaces:**
- Keeps `read_user_session_count` and `read_ip_session_count` linearizable.
- Uses etcd count-only range responses so large session prefixes do not return every value.

- [x] Add a regression test that fills multiple sessions and asserts exact counts for user and IP prefixes.
- [x] Change both count queries to count-only linearizable reads.
- [x] Run the existing user-admission tests and confirm counts and quota behavior remain correct.

### Task 2: Add the Admission Spike Driver

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/rcaster-service/Cargo.toml`
- Create: `crates/rcaster-service/examples/admission_spike.rs`

**Interfaces:**
- CLI: `--rate <u32> --seconds <u64> --mode unique|hot --concurrency <usize>`.
- Environment: `RCASTER_ETCD_ENDPOINTS` as a comma-separated endpoint list.
- Output: one machine-readable JSON object per run.

- [x] Implement pre-granted leases, fixed concurrency, and an open-loop arrival schedule.
- [x] Retry lock-busy errors with a bounded short delay and report the retry count.
- [x] Measure successful and failed admissions plus p50/p95/p99 latency.
- [x] Run one short smoke test against etcd.

### Task 3: Run the Three-Member Spike

**Files:**
- Create: `docs/superpowers/reports/2026-11-22-etcd-admission-spike.md`

- [x] Start three local etcd members and verify quorum health.
- [x] Run unique-user workloads at 100, 500, and 1000 offered admissions/s.
- [x] Run hot-user workloads at 100, 500, and 1000 offered admissions/s.
- [x] Run a 10,000-session prefix probe and a shared NAT IP probe.
- [x] Stop the leader, run a recovery probe, restart it, and confirm admission recovers.
- [x] Record hardware, topology, duration, throughput, p50/p95/p99, retries, and failures.
- [x] Choose the supported admission range and recommend A, B, or C for the next design gate.

### Task 4: Verify the Gate

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update `README.md` with the spike result summary.

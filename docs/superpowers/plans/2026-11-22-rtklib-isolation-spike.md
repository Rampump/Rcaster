# RTKLIB Isolation Spike Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove a versioned supervised decoder-worker boundary can survive crashes, hangs, oversized messages, and automatic restart without affecting the caller.

**Architecture:** A bounded JSON-lines protocol runs over child process stdin/stdout. The supervisor sends one request at a time with a timeout, enforces input/response byte limits, kills unhealthy workers, and starts a fresh process for the next request. A fixture worker stands in for the future RTKLIB C ABI worker so the failure behavior can be tested without linking RTKLIB into the data plane.

**Tech Stack:** Rust 2024, Tokio process and I/O, serde JSON.

## Global Constraints

- RTKLIB/FFI never runs inside the NTRIP connection task.
- Protocol messages are versioned and request IDs must match.
- Input and output sizes are bounded.
- Timeout, malformed response, EOF, or process exit kills the worker.
- A later request starts a fresh worker.
- Capability results are explicit; a linked/mock decoder never implies CMR/BINEX support.
- This spike validates isolation mechanics, not RTKLIB semantic accuracy.

---

### Task 1: Define Worker Protocol and Supervisor

- [x] Add `rcaster-rtklib` workspace crate.
- [x] Define framed request/response DTOs.
- [x] Implement start, request timeout, bounded line reads, crash detection, and restart.

### Task 2: Add Fixture Worker and Failure Tests

- [x] Add a fixture child process with capability, decode, crash, hang, and oversized-output commands.
- [x] Test capability probe and normal decode.
- [x] Test input limit rejection before process write.
- [x] Test crash and automatic restart.
- [x] Test timeout/kill and automatic restart.
- [x] Test oversized response rejection.

### Task 3: Verify

- [x] Run format, Clippy, and full workspace tests.
- [x] Record isolation limitations and the next RTKLIB ABI step.

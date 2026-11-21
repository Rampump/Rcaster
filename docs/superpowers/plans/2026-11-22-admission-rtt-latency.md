# mTLS Admission RTT Latency Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure admission latency through a real mTLS gRPC connection under simulated 20/50/100/250 ms RTT.

**Architecture:** A local TCP proxy delays both directions while forwarding encrypted HTTP/2 bytes unchanged. The client still performs real certificate and service-token verification against the admission server. Measurements are explicitly local emulation, not a cross-region deployment test.

**Tech Stack:** Rust 2024, Tokio TCP proxy, tonic mTLS, etcd.

## Global Constraints

- TLS is terminated only by the admission server and client, never by the proxy.
- The proxy preserves HTTP/2 and mTLS semantics.
- Report one-way simulated delay and total RTT separately.
- Record p50/p95/p99 and successful request count per RTT.
- Do not claim geographic network validation from loopback emulation.

---

### Task 1: Add Delayed mTLS Benchmark

- [x] Start a real coordinator and reloadable mTLS server.
- [x] Insert a two-direction delayed TCP proxy.
- [x] Run sequential admissions for 20/50/100/250 ms RTT.
- [x] Report latency and throughput per scenario.

### Task 2: Record Findings

- [x] Update admission report with methodology and results.
- [x] State loopback limitations and remaining real-network tests.
- [x] Run format, Clippy, and full workspace tests.

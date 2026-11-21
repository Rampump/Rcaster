# Admission mTLS Certificate Rotation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rotate admission server certificates and trusted client CAs without restarting the gRPC listener.

**Architecture:** A stable TCP listener feeds a dynamic rustls acceptor. Each new connection loads the current server config snapshot. Existing established TLS connections retain their negotiated configuration; new handshakes use the rotated certificate and client CA.

**Tech Stack:** Rust 2024, rustls, tokio-rustls, tonic, etcd.

## Global Constraints

- Rotation is atomic for new connections.
- No admission request bypasses certificate verification or service token authorization.
- Existing connections may continue with the previous certificate until reconnect.
- Invalid certificate/key/CA material must not replace the active configuration.
- Private keys are never logged.

---

### Task 1: Add Reloadable TLS Acceptor

- [x] Keep a stable TCP listener and dynamic rustls server configuration.
- [x] Accept HTTP/2 streams into tonic through an incoming channel.
- [x] Add atomic `rotate` for server certificate, private key, and client CA.
- [x] Preserve service-token authorization on the reloadable server.

### Task 2: Verify Rotation

- [x] Admit with CA-1 client and server certificates.
- [x] Rotate to CA-2 server/client configuration.
- [x] Reject a new CA-1 client connection.
- [x] Admit a new CA-2 client without restarting the listener.
- [x] Verify rejected handshakes create no session.

### Task 3: Verify the Gate

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] Run `cargo test --workspace`.
- [x] Update README and admission report.

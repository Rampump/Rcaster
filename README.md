# RCaster

RCaster is a Rust NTRIP Caster implemented from the design in
[`rust-rewrite-plan.md`](rust-rewrite-plan.md).

## Current state

The repository currently contains the Gate 1 source-fencing and user-admission
prototypes:

- `rcaster-core` contains the in-process dispatch fence.
- `rcaster-service` contains atomic etcd source claims, activation, and cleanup.
- User and IP admission locks serialize quota checks and conditionally commit session indexes.
- Source admission commits the owner session and source claim in one transaction.
- Client and source admission reject stale Caster Config revisions and resume after reloading.
- Integration tests verify single-claim ownership and stale-owner fencing after lease expiry.
- Integration tests verify final-slot races, stale lock takeover, atomic source rollback,
  cluster-wide user limits, and IP quota isolation.
- Integration tests verify stale configuration rejection and recovery for clients and sources.
- The admission spike record is in
  [`docs/superpowers/reports/2026-11-22-etcd-admission-spike.md`](docs/superpowers/reports/2026-11-22-etcd-admission-spike.md).

The lock-based design saturated at roughly 100/s for one hot user and roughly
66/s for one shared NAT IP. Revision records sustained 1000 admissions/s for
unique users, but still saturated near 130/s for hot scopes. The selected
coordinator prototype batches admissions under one lease-fenced owner and
sustained 1000/s for hot users and shared NAT IP with p99 around 20 ms.
The same workload over a loopback gRPC channel sustained roughly 992–994/s;
gRPC now requires internal mTLS, a cluster-scoped service token, and
`admission:write` authorization.

This is not yet a runnable NTRIP Caster.

## Development

Requirements:

- Rust 1.87 or newer.
- `protoc`, required by `etcd-client`.
- etcd 3.5 reachable at `http://127.0.0.1:2379`.

Start etcd and run the workspace tests:

```bash
docker compose up -d etcd
cargo test --workspace
```

Set `RCASTER_ETCD_ENDPOINT` to test against a different etcd endpoint.

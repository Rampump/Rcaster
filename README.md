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
- Integration tests verify single-claim ownership and stale-owner fencing after lease expiry.
- Integration tests verify final-slot races, stale lock takeover, atomic source rollback,
  cluster-wide user limits, and IP quota isolation.

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

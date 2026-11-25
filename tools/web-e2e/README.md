# Browser-Level Web Validation

This harness starts isolated standalone and clustered Caster instances, an
ephemeral local etcd, a synthetic NTRIP source built from the real COM3 RTCM3
capture, and headless Chromium. All traffic stays on `127.0.0.1`; the four soak
servers are not contacted.

## Prerequisites

- Debian/WSL with `chromium`, `openssl`, `etcd`, `etcdctl`,
  `python3-websockets`, and `python`.
- A release binary at `target/release/rcaster`.
- `target/com3-rtcm3-capture.bin`.

## Run

```bash
python3 tools/web-e2e/run_browser_e2e.py
```

Results are written to `tools/web-e2e/runs/<timestamp>/result.json`.

Both modes exercise administrator login, user create/update/disable/delete,
mountpoint create/update/disable/delete, blacklist create/update/delete,
source filtering, health, history, maintenance enable/disable, public
directory rendering, map auto-fit, public login, one-time ticket consumption,
and the monitor WebSocket. The clustered mode additionally uses a real NTRIP
client and source session to verify administrator session drop, user-disable
disconnect, source disconnect and configuration application with generation
advancement. The clustered flow also previews and completes a batch user-disable
operation through the browser. Console exceptions, uncaught errors, failed
network requests, and unexpected HTTP errors fail the run.

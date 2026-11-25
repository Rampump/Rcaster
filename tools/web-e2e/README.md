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

Both modes exercise administrator login, user create/update/disable/delete with
NTRIP and Web permissions, mountpoint create/update/disable/delete with
coordinates, coverage, STR metadata and source expectations, blacklist
create/update/delete, API token create/list/revoke, administrator logout,
source filtering, health, history, maintenance enable/disable, public directory
rendering, map auto-fit, public login, one-time ticket consumption, and the
monitor WebSocket. Both modes also list and disconnect a real NTRIP client
session and disconnect a live source. The clustered mode additionally applies
configuration with generation advancement and previews/completes a batch
user-disable operation through the browser. Console exceptions, uncaught
errors, failed network requests, and unexpected HTTP errors fail the run.
Disabling an active mountpoint is also verified to close its live source.

The public map test also injects the 1,301-station CRTK.NET snapshot into the
production map JavaScript, verifies low-zoom cluster totals, and compares 15
individual marker pixel positions against an independent WGS-84/Web-Mercator
calculation. Standalone uses WGS-84 XYZ tiles; clustered uses GCJ-02 AMap
satellite tiles. See `MAP_REPORT.md`.

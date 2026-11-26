# Browser-Level Web Validation

This harness starts isolated standalone and clustered Caster instances, an
ephemeral local etcd, a synthetic NTRIP source built from the real COM3 RTCM3
capture, and headless Chromium. All traffic stays on `127.0.0.1`; the four soak
servers are not contacted.

## Prerequisites

- Debian/WSL with `chromium`, `openssl`, `etcd`, `etcdctl`,
  `python3-websockets`, and `python`.
- Optional cross-browser smoke: `firefox-esr`, `python3-selenium`, and a
  compatible `geckodriver` on `PATH` or at `~/.local/bin/geckodriver`.
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

The HCI gate also verifies paired coordinate errors, required user passwords,
JSON field invalidation, configuration diff before Apply, the accessible
destructive-action impact dialog, bounded refresh retry with `Data stale`,
idle public-session expiry, reconnect status from the active socket, and
single-flight mutation controls. The public map is exercised with CDP mouse
wheel and two-pointer pinch zoom, asserting that zoom changes while the
viewport center stays anchored, followed by one-finger pan. Batch
preview/start and operation Cancel/Retry are exercised when the local
etcd-backed mode is available.

The clustered run also configures a trusted Ed25519 upgrade key and drives the
browser upgrade UI through signed Preview, Start, pending approval, Cancel,
Rollback, Rollback Cancel, and a second-administrator Approve to `RUNNING`.
That approval is the final assertion because the isolated Caster intentionally
receives the upgrade command and shuts down.

When Firefox prerequisites are present, both modes also run a Firefox 153.7 ESR
smoke covering public directory rendering, login, monitor snapshot, OSM/AMap
tile requests, administrator login, source rendering, configuration-preview
controls and upgrade controls. The check records screenshots and fails on
window errors or unhandled Promise rejections.

The public map test also injects the 1,301-station CRTK.NET snapshot into the
production map JavaScript, verifies low-zoom cluster totals, and compares 15
individual marker pixel positions against an independent WGS-84/Web-Mercator
calculation. Standalone loads real OpenStreetMap WGS-84 XYZ tiles; clustered
loads real GCJ-02 AMap satellite tiles and captures a Shanghai zoom-12 view.
Every screenshot waits for all visible tile images to decode at 256×256.
See `MAP_REPORT.md`.

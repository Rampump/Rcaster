# RCaster Acceptance Matrix

Status date: 2026-11-24 23:29 CST

Deployed soak release:
`76f542d70efce5cbc9b3a29213654ea62bb3d555dfb98336fd7570f59498ae2c`

Deployed soak source code commit:
`9bdc2f8`

Local remediation commit:
`18d795e` (committed locally, not pushed or deployed)

## Status

- **Verified**: current source and runtime evidence prove the requirement.
- **Partial**: the implementation and automated coverage exist, but the
  requirement also asks for a real external environment or longer run that is
  not yet proven.
- **Pending**: required evidence does not exist yet.
- **Not applicable**: explicitly excluded by the design.

The 72-hour acceptance run started between 20:12:42 and 20:12:45 CST on
2026-11-24 and is expected to finish by 20:12:45 CST on 2026-11-27. Until that
window exits successfully, functional implementation and short-run tests must
not be described as final production acceptance.

## Requirements by Design Section

| Design section | Current status | Authoritative evidence | Remaining proof |
|---|---|---|---|
| 18.1 Functional behavior | Partial | Workspace and service integration tests cover NTRIP1/2 authentication, source admission, private source-table filtering, source fencing, local and cross-node relay, NEAR switching, admin sessions/CSRF/API scopes, WebSocket lifecycle and optional RTKLIB isolation. The live compatibility matrix records the real CH340 base, RTKLIB, u-center, BNC and Lefebure. A live CRTK.NET test models 1,302 public RTCM3 STR candidates and matches 26 sampled NEAR queries against a full scan. | More receiver models; real CMR/CMR+ capture; public-monitor and management UI paths still rely on API/HTML assertions rather than full browser E2E. |
| 18.2 Consistency and ownership | Verified | `source_fencing`, `user_admission`, `revision_admission`, `config_revision`, `cluster_runtime`, `blacklist_write`, `account_directory`, `connections` and `upgrade` integration tests cover races, stale revisions, generation/connection fencing, owner commands and cleanup. Live cluster ownership was checked at 3/2/3/2 across four nodes. | Re-run the same assertions after the 72-hour run exits to prove no orphan state remains. |
| 18.3 Capacity | Verified | Target-node probes completed 10 sources/8,000 subscribers, 1 source/10,000 subscribers and 10,000 mountpoints. Coordinator spikes achieved about 1,000 admissions/s. A local four-Caster cluster with 500 sources and 100 cross-node relay subscribers ran 30 seconds: 16,872,000 source bytes and 3,373,032 subscriber bytes. | The frozen soak remains the long-duration stability run, not a capacity proof gap. |
| 18.4 Stability | Pending | Pre-runs showed zero restarts and bounded RSS/FD. The current frozen candidate is running four local and one cross-region soak units. | Successful completion of all five 259,200-second runs, final RSS/FD curves, zero restart, empty stderr and clean post-run state. |
| 18.5 Security | Partial | Automated tests cover forced TLS, mTLS etcd/relay, admin authorization, CSRF, token scope escalation, trusted proxy IP handling, WebSocket origin, private-key permissions and log redaction paths. RustSec scanned 248 dependencies with no known vulnerability. Live checks confirmed public plaintext 2101 is closed and private SOAK sources are absent from TLS source tables. | A formal long-running fuzz campaign for the RTKLIB worker and protocol decoders; production certificate/OCSP validation. |
| 18.6 Multi-protocol regression | Partial | Frozen official RTCM2/RTCM3 fixtures and a 17.6 MB real BINEX stream pass across chunk boundaries. Deterministic malformed, split, oversized and resynchronization cases pass for RTCM2, RTCM3, BINEX, CMR/CMR+, NMEA and mixed streams. A new 64-seed random-input test proves the demux pending buffer never exceeds 8 KiB. | No public, traceable real CMR/CMR+ capture was found; CMR remains structurally validated with synthetic fixtures only. |
| 18.7 NTRIP compatibility | Partial | Byte-level request/response tests pass. Live clients include RTKLIB `str2str`, Windows `strsvr`, BKG BNC 2.13.7, Lefebure 2017.07.27, u-center 25.03 and the local CH340 RTK base. | Additional commercial clients and receiver models remain explicitly `NOT VERIFIED`. |
| 18.8 STR lifecycle | Verified | NTRIP, source-fencing, cluster-runtime and public API tests cover VALIDATING visibility, ACTIVE publication, private filtering, source generation changes, remote owner routing and removal. `second_caster_serves_cached_etcd_str_without_local_source` proves Caster B serves its locally cached source table without owning the source. A late-position test proves 1005/1006 and format details refresh the existing ACTIVE etcd STR in place when they arrive after initial MSM admission. | Confirm the final soak sources disappear and public/private source tables converge after cleanup. |
| 18.9 etcd Watch | Verified | Config, blacklist, source and account watch tests cover snapshot-before-watch, incremental updates, compaction/reconnect, silent-stall detection, leader failure and full-quorum recovery. The two-Caster STR test confirms etcd changes converge into the edge source cache and are served locally. Account seeding now caps transaction operations and is covered with 500 mountpoints. | Final soak exit check should confirm no watch-related warnings and the expected cleaned key set. |
| 18.10 Ownership and reconnect | Verified | Automated tests cover owner-only writes, runtime epoch changes, source generation fencing, lease expiry, stale cache replacement and old-connection rejection. | Repeat a post-window orphan-key and source/session count audit. |
| 18.11 Automatic upgrade and rollback | Partial | Signed manifest verification, untrusted-host/size/hash rejection, two-person approval, rollout locking, failure stopping and rollback state tests pass. Updater unit tests cover artifact download and slot switching. | No real four-node signed rolling upgrade and rollback has been performed under production traffic. |
| 18.12 External DNS/LB | Partial | Real HAProxy PROXY Protocol v2 preserved the source IP; trusted proxy configuration is regression-tested; health and readiness endpoints are available. | Real GeoDNS/region VIP, draining/removal behavior, all public hostnames and production TLS certificate coverage. |
| 18.13 Public Web/STR/map/monitor | Partial | Public directory, ticket, authentication, monitor, tile-host validation and source privacy tests pass. The admin dashboard now exposes source filters, health/history charts, NEAR clients, maintenance, config apply/reload, versions, audit and operation/upgrade lookup. Inline JavaScript is parse-checked. | Public web is not exposed on this test deployment (`public_bind=null`); external DNS/LB, real tile service, large-map behavior and production monitor load remain unverified. |

## Local Design-Parity Remediation

Commit `8108912` closes the remaining code gaps tracked by
`docs/superpowers/plans/2026-11-24-design-parity-remediation.md`:

- `ProtocolDemux` now guarantees at most 8 KiB of pending bytes. A 64-seed
  deterministic random regression covers arbitrary 1–257-byte chunks without
  panics or unbounded growth.
- Source health implements short/long windows, observation timeout, valid-frame
  and CRC ratios, alert clearing, disable semantics, station changes and base
  position jumps.
- Late RTCM3 position/device fields refresh an ACTIVE source STR in place with
  source revision/value CAS and connection/epoch fencing.
- Large account seeds are split below the etcd transaction operation limit and
  covered by a 500-mountpoint watcher test.
- NEAR now enforces per-client GGA input rate and jump speed, global query QPS
  and concurrency, position TTL, and fixed-label NEAR metrics.
- A live CRTK.NET test pulls the public STR table and builds the same NEAR model
  used at runtime. The run found 1,302 RTCM3 candidates and all 26 sampled
  fixed-grid queries matched a full Haversine scan.
- The cross-Caster STR cache path is covered by an integration test: source
  Caster A uploads RTCM through NTRIP, edge Caster B converges its etcd source
  watch cache, and B serves the resulting `STR;C66` from its local source table
  while owning no source.
- Public monitor tickets are bound to source generation and close a live
  WebSocket with `SOURCE_CHANGED` after generation 1001 is replaced by 1002.
- Service-token management sessions are read-only in both the browser and the
  server: write controls are hidden and all write requests return 403.
- Standalone blacklist rules persist in a dedicated redb table, use normalized
  CIDR uniqueness and revision CAS, reload immediately, and deny matching
  clients before authentication when quota is zero.
- The real local COM3 capture again passed
  `uploaded_rtcm3_generates_active_etcd_str_record`: upload, RTCM parsing,
  admission, ACTIVE source generation and etcd STR creation with 1005/1074 and
  non-zero coordinates.
- A temporary local four-Caster cluster completed 500 sources and 100
  cross-node relay subscribers for 30 seconds, with 16,872,000 source bytes and
  3,373,032 subscriber bytes. All temporary processes and data were cleaned up.
- The management dashboard now covers the existing operational APIs rather than
  only users, mountpoints and blacklist CRUD. Config application uses the same
  source-health validation as cluster config loading.

Local verification for this remediation:

- `cargo fmt --all -- --check`: pass.
- `cargo test --workspace --all-features --locked`: pass.
- `cargo clippy --workspace --all-features --all-targets --locked -- -D warnings`:
  pass.
- Release build: pass; `target/release/rcaster` SHA-256
  `C822F31EBE2E57FEAAF34D3433BBC70E091BDE6F8F705A29C1D9F5C91030B78D`.
- The complete four-command gate was rerun on the current working tree on
  2026-11-24 23:29 CST with exit code 0. Documentation-only commits do not
  change the verified source code or release artifact.

None of this remediation was deployed, pushed, or connected to the four soak
servers. The deployed soak candidate and its 72-hour evidence remain the frozen
`76f542d...` artifact.

## Frozen Deployed Soak Candidate Evidence

The final candidate passed:

- `cargo test --workspace --all-features --locked`: 319 passed, 8 ignored,
  0 failed.
- All eight ignored external integrations, run separately with real
  dependencies: etcd credential rotation, leader failover, full-quorum
  recovery, snapshot restore, HAProxy PROXY v2, RTKLIB `str2str`, official
  RTCM fixtures and the real BINEX stream.
- `cargo clippy --workspace --all-features --all-targets --locked -- -D
  warnings`.
- All-feature release build.
- RustSec scan of 248 dependencies against 1,566 advisories.

Current 72-hour preflight evidence:

- Four Casters and the 101 coordinator are ready.
- All five soak units are active with zero restarts and empty stderr.
- Cluster status reports 10 active sources and 60 sessions.
- Five-second source-byte deltas are positive on all four nodes.
- All three etcd members commit proposals; the latest production snapshot was
  restored into an isolated etcd and became healthy.
- Caster and soak warning-level journals are empty.
- No private `STR;SOAK*` row appears in any public TLS source table.

## Completion Gate

Do not mark the project production-complete until:

1. All five soak units complete normally at approximately
   `2026-11-27 20:12:45 CST`.
2. Every result JSON parses and reports approximately 259,200 seconds,
   non-zero transferred bytes, no remaining sources or subscribers, bounded
   RSS/FD and zero application errors.
3. The four local units and the cross-region unit show successful exit,
   zero restarts and empty stderr.
4. Admin APIs report zero cluster sources and sessions after the generators
   exit; etcd contains no soak source claims, sessions or commands.
5. The temporary `soak-final` user, `SOAK000-SOAK009` mountpoints and
   `/etc/rcaster/final-soak.env` are removed only after the results are
   preserved and checked.

Items that require a separate environment or explicit operator decision are
not waived by completion of the soak: real CMR capture, additional receivers,
production OCSP, external DNS/LB behavior and a full signed rolling-upgrade
drill.

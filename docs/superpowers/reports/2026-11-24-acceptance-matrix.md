# RCaster Acceptance Matrix

Status date: 2026-11-24 20:25 CST

Deployed release:
`76f542d70efce5cbc9b3a29213654ea62bb3d555dfb98336fd7570f59498ae2c`

Source code commit:
`9bdc2f8`

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
| 18.1 Functional behavior | Partial | Workspace and service integration tests cover NTRIP1/2 authentication, source admission, private source-table filtering, source fencing, local and cross-node relay, NEAR switching, admin sessions/CSRF/API scopes, WebSocket lifecycle and optional RTKLIB isolation. The live compatibility matrix records the real CH340 base, RTKLIB, u-center, BNC and Lefebure. | More receiver models; real CMR/CMR+ capture; a few public-monitor and management UI paths still rely on API/HTML assertions rather than full browser E2E. |
| 18.2 Consistency and ownership | Verified | `source_fencing`, `user_admission`, `revision_admission`, `config_revision`, `cluster_runtime`, `blacklist_write`, `account_directory`, `connections` and `upgrade` integration tests cover races, stale revisions, generation/connection fencing, owner commands and cleanup. Live cluster ownership was checked at 3/2/3/2 across four nodes. | Re-run the same assertions after the 72-hour run exits to prove no orphan state remains. |
| 18.3 Capacity | Partial | Target-node probes completed 10 sources/8,000 subscribers, 1 source/10,000 subscribers and 10,000 mountpoints. Coordinator spikes achieved about 1,000 admissions/s for independent users, hot users and shared NAT IPs. | The design's 500-source and 100-relay per-node combination has not been exercised as one long run; current acceptance relay run uses 10 sources/50 subscribers. |
| 18.4 Stability | Pending | Pre-runs showed zero restarts and bounded RSS/FD. The current frozen candidate is running four local and one cross-region soak units. | Successful completion of all five 259,200-second runs, final RSS/FD curves, zero restart, empty stderr and clean post-run state. |
| 18.5 Security | Partial | Automated tests cover forced TLS, mTLS etcd/relay, admin authorization, CSRF, token scope escalation, trusted proxy IP handling, WebSocket origin, private-key permissions and log redaction paths. RustSec scanned 248 dependencies with no known vulnerability. Live checks confirmed public plaintext 2101 is closed and private SOAK sources are absent from TLS source tables. | A formal long-running fuzz campaign for the RTKLIB worker and protocol decoders; production certificate/OCSP validation. |
| 18.6 Multi-protocol regression | Partial | Frozen official RTCM2/RTCM3 fixtures and a 17.6 MB real BINEX stream pass across chunk boundaries. Deterministic malformed, split, oversized and resynchronization cases pass for RTCM2, RTCM3, BINEX, CMR/CMR+, NMEA and mixed streams. | No public, traceable real CMR/CMR+ capture was found; CMR remains structurally validated with synthetic fixtures only. |
| 18.7 NTRIP compatibility | Partial | Byte-level request/response tests pass. Live clients include RTKLIB `str2str`, Windows `strsvr`, BKG BNC 2.13.7, Lefebure 2017.07.27, u-center 25.03 and the local CH340 RTK base. | Additional commercial clients and receiver models remain explicitly `NOT VERIFIED`. |
| 18.8 STR lifecycle | Verified | NTRIP, source-fencing, cluster-runtime and public API tests cover VALIDATING visibility, ACTIVE publication, private filtering, source generation changes, remote owner routing and removal. | Confirm the final soak sources disappear and public/private source tables converge after cleanup. |
| 18.9 etcd Watch | Verified | Config, blacklist and account watch tests cover snapshot-before-watch, incremental updates, compaction/reconnect, silent-stall detection, leader failure and full-quorum recovery. | Final soak exit check should confirm no watch-related warnings and the expected cleaned key set. |
| 18.10 Ownership and reconnect | Verified | Automated tests cover owner-only writes, runtime epoch changes, source generation fencing, lease expiry, stale cache replacement and old-connection rejection. | Repeat a post-window orphan-key and source/session count audit. |
| 18.11 Automatic upgrade and rollback | Partial | Signed manifest verification, untrusted-host/size/hash rejection, two-person approval, rollout locking, failure stopping and rollback state tests pass. Updater unit tests cover artifact download and slot switching. | No real four-node signed rolling upgrade and rollback has been performed under production traffic. |
| 18.12 External DNS/LB | Partial | Real HAProxy PROXY Protocol v2 preserved the source IP; trusted proxy configuration is regression-tested; health and readiness endpoints are available. | Real GeoDNS/region VIP, draining/removal behavior, all public hostnames and production TLS certificate coverage. |
| 18.13 Public Web/STR/map/monitor | Partial | Public directory, ticket, authentication, monitor, tile-host validation and source privacy tests pass. The admin dashboard was loaded through real mTLS and service-token authentication. | Public web is not exposed on this test deployment (`public_bind=null`); external DNS/LB, real tile service, large-map behavior and production monitor load remain unverified. |

## Current Candidate Evidence

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

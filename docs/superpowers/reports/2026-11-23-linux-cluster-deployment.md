# RCaster Linux Cluster Deployment

- Date: 2026-11-23
- Environment: three Debian 13 etcd/Caster nodes plus one Debian 12 edge Caster
- Cluster ID: `rcaster-cluster-test`

## Result

The current worktree builds and passes its full Linux workspace test suite.
A three-member mTLS etcd cluster and four Caster processes are running. The
fourth Caster is an etcd client edge node and is not an etcd member.

During initial cluster validation, a real RTK base module on local
`COM3 @ 460800` was connected through RTKLIB `str2str` and an encrypted SSH local
tunnel to the edge Caster. The live `RTKBASE` source contained RTCM3 1005, 1074,
1084, 1094, 1114, 1124 and 1134 plus NMEA. The internal parser reported MSM4 observations for GPS, GLONASS,
Galileo, QZSS, BeiDou and NavIC. A subscriber connected to Caster 3 over public
TLS received an NTRIP2 `200 OK` and 80 CRC-valid RTCM3 frames in 10 seconds.

The edge node exposes NTRIP1-compatible plaintext on public TCP 2101 and NTRIP2
TLS on public TCP 2102. This plaintext compatibility is for the current test
cluster only; production should require TLS or place plaintext NTRIP behind a
trusted ingress.

The initial deployment release SHA-256 was
`88c7ba58a68398f06afc686bbeec2c991265ec8537a22f9cd891fb566d08009e`.
It was replaced on all four nodes by
`5d307a3c1ff2f56b5ceacbc0f2db0d1a4291e2d75e29182a4167a1fe8758ec3b` during
the first rolling update, then by
`1e86d4a18233be63b4eac59dd77fbb5bc155423f9d930da7ecc6c2e9ec64cad5` during the
online OCSP release described below.

The deployment is a test cluster because it uses the development account
password and a self-signed, 30-day certificate. It is not a production
credential or certificate rollout.

## Rolling Update 2026-11-23

The four test nodes were updated sequentially from `88c7ba58…d08009e` to
`5d307a3c…758ec3b` between 17:27 and 17:29 CST. Each node paused only for its
own service restart; etcd was not restarted.

The release was built on `139.199.14.2` from source package
`rcaster-src.tgz` SHA-256
`34be15d1a2f816b28a45e5a348974688ba253a803f2c8949bc4e83fad5e5f8b9`:

```bash
CARGO_BUILD_JOBS=1 cargo build --workspace --release --locked
```

The build finished in 9m14s. The resulting `rcaster` binary has SHA-256
`5d307a3c1ff2f56b5ceacbc0f2db0d1a4291e2d75e29182a4167a1fe8758ec3b`
and requires at most `GLIBC_2.35`. It was executed successfully on the Debian
12 edge node (`GLIBC_2.36`) before replacement.

| Node | Updated service | Activated (CST) | PID | Rollback file |
|---|---|---|---|---|
| `139.199.14.2` | `rcaster.service` | 17:27:22 | 202366 | `/opt/rcaster/rcaster.rollback-20261123T092722Z-88c7ba58` |
| `134.175.139.223` | `rcaster.service` | 17:27:32 | 322065 | `/opt/rcaster/rcaster.rollback-20261123T092732Z-88c7ba58` |
| `101.33.212.76` | `rcaster.service` | 17:28:19 | 199370 | `/opt/rcaster/rcaster.rollback-20261123T092745Z-88c7ba58` |
| `101.33.212.76` | `rcaster-coordinator.service` | 17:29:05 | 199651 | `/opt/rcaster/rcaster.coordinator-rollback-20261123T092745Z-002660` |
| `43.142.126.162` | `rcaster.service` | 17:29:24 | 360384 | `/opt/rcaster/rcaster.rollback-20261123T092923Z-88c7ba58` |

Post-update checks:

- All five tuned process images (`rcaster` plus the 101 coordinator) read back
  SHA-256 `5d307a3c…758ec3b` from `/proc/PID/exe`.
- All four `/health/ready` endpoints returned `ready`; all dependency gauges
  were `1`.
- Ports 2101, 2102, and 12101 were listening on every node; port 5757 was
  listening on `101.33.212.76`.
- The 5757 coordinator endpoint completed an mTLS TLS 1.3 handshake.
- The three-member etcd cluster reported successful writes on all members.
- Public NTRIP source tables answered on edge ports 2101 and 2102.

Operational observations:

- Stopping the previous 101 coordinator did not finish within
  `TimeoutStopSec=30s`; systemd sent SIGKILL and then started the new process.
  A planned graceful-stop test of the new coordinator later completed in
  58 ms with `Result=success`.
- The Debian 13 `rcaster-updater` artifact initially required `GLIBC_2.39`
  and could not run on the Debian 12 edge node. It was later rebuilt on the
  Debian 12 node; the replacement requires at most `GLIBC_2.34` and is
  installed on all four nodes.
- No local `COM3` device, `str2str` process, or 2101 SSH tunnel was present
  during final verification, so edge `rcaster_active_sources` remained `0`.
  The public NTRIP listeners were reachable, but real-source data flow could
  not be revalidated in that state.

## Online OCSP Release

The cluster was rolled from `5d307a3c…758ec3b` to
`1e86d4a1…64cad5` between 18:43 and 18:46 CST. The source package was
`rcaster-src-ocsp.tgz`, SHA-256
`1ceb673886466a60f9c24492a77f2076df06b55a90f884c7bc8a972efdf411e8`.
The release was built once on `139.199.14.2` with
`CARGO_BUILD_JOBS=1 cargo build --workspace --release --locked`.

The Linux full workspace suite on Debian 13 WSL completed with
`308 passed, 7 ignored, 0 failed`. Clippy passed with `-D warnings`.
The candidate binary requires at most GLIBC 2.35 and was launched
successfully on the Debian 12 edge node before replacement.

| Node | Updated service | Activated (CST) | Current PID | Rollback file |
|---|---|---|---|---|
| `139.199.14.2` | `rcaster.service` | 18:43:55 | 225030 | `/opt/rcaster/rcaster.rollback-20261123T104355Z-5d307a3c` |
| `134.175.139.223` | `rcaster.service` | 18:44:28 | 340070 | `/opt/rcaster/rcaster.rollback-20261123T104428Z-5d307a3c` |
| `101.33.212.76` | `rcaster.service` | 18:44:40 | 217363 | `/opt/rcaster/rcaster.rollback-20261123T104440Z-5d307a3c` |
| `101.33.212.76` | `rcaster-coordinator.service` | 18:46:38 | 218030 | `/opt/rcaster/rcaster.coordinator-rollback-20261123T104451Z` |
| `43.142.126.162` | `rcaster.service` | 18:46:12 | 378122 | `/opt/rcaster/rcaster.rollback-20261123T104612Z-5d307a3c` |

Post-release checks:

- All five process images read back SHA-256 `1e86d4a1…64cad5`.
- All four Caster instances returned `ok` from `/health/live` and `ready`
  from `/health/ready`; every control-plane dependency gauge remained `1`.
- Ports 2101, 2102, and 12101 are listening on every node; coordinator 5757
  is listening on 101.
- All four relay listeners completed TLS 1.3 mTLS handshakes with verify
  code 0, including the reloadable relay path used by this release.
- The coordinator completed a TLS 1.3 mTLS handshake with verify code 0.
- A planned stop/start of the new coordinator completed with `Result=success`;
  the measured stop time was 58 ms, replacing the previous 30-second SIGKILL
  observation.
- All three etcd members reported healthy committed proposals. Raft term 2,
  index 1226, and applied index 1226 matched across members.
- Public NTRIP1/2 source tables on the edge node returned
  `SOURCETABLE 200 OK`. No real source was connected during this release, so
  source data-plane flow was not part of the final check.
## Topology

| Node | Public / private address | Roles |
|---|---|---|
| 1 | `134.175.139.223` / `10.0.24.4` | etcd member, Caster, relay 12101 |
| 2 | `101.33.212.76` / `10.0.0.9` | etcd member, Caster, coordinator 5757, relay 12101 |
| 3 | `139.199.14.2` / `10.0.4.4` | etcd member, Caster, relay 12101 |
| 4 | `43.142.126.162` | etcd client edge Caster, relay 12101, real source |

The nodes use etcd client and peer TLS, Caster-to-coordinator mTLS,
Caster-to-etcd mTLS, and Caster-to-Caster relay mTLS. Public NTRIP is available
on TCP 2102 with server TLS and on TCP 2101 without TLS for compatibility
testing. The local `str2str` source uses the same loopback path through
`127.0.0.1:2101`.

The three decision nodes and the edge node are deployed in different cities.
The operator workstation is also thousands of kilometres from the servers.
Consequently the TLS relay, etcd quorum, and fault-injection results exercise
real cross-city public network paths rather than only loopback emulation.
This does not by itself complete a cross-region disaster-recovery drill.

From the workstation, TCP connection setup to the four public nodes ranged
from 22 to 36 ms. A 30-second cross-region TLS relay run used 10 sources
distributed over all four Casters and 50 subscribers. It transferred 337,440
source bytes and 1,684,046 subscriber bytes at 0.0535 MiB/s, with a peak of
70 file descriptors and 7.6 MiB RSS. All SOAK routes returned to zero after
the run.

## Real Source Path

The source path used for validation was:

```text
COM3 @ 460800
  -> RTKLIB str2str
  -> SSH local forward 127.0.0.1:2101
  -> edge Caster 127.0.0.1:2101
  -> etcd source record RTKBASE
  -> mTLS relay 12101
  -> remote NTRIP2 subscriber
```

The effective upload form is:

```text
str2str -in serial://COM3:460800 \
  -out ntrips://:<source-password>@127.0.0.1:2101/RTKBASE
```

The SSH tunnel and `str2str` process are currently running as user processes;
restart them after a Windows reboot or logoff.

## Verification

| Check | Result |
|---|---|
| Debian 12 Linux workspace tests after source expectations | 300 passed, 7 ignored, 0 failed, exit 0 |
| Debian 13 Linux workspace tests on node 1 after OCSP refresh work | 307 passed, 7 ignored, 0 failed, exit 0 |
| Workspace tests with all features before MSM restrictions | 290 passed, 7 ignored, 0 failed, exit 0 |
| Formatting and Clippy with `-D warnings` | Passed |
| Initial deployment release workspace build | Passed; `rcaster` SHA-256 `88c7ba58…d08009e` |
| Rolling update release workspace build | Passed; `rcaster` SHA-256 `5d307a3c…758ec3b` |
| Three-member etcd mTLS quorum after rollout | All three members started and healthy |
| Stop etcd-3 and write with members 1 and 2 | Passed |
| Restart etcd-3 and rejoin quorum | Passed |
| etcd client credential hot rotation | Passed |
| Three-member leader failure recovery | Passed |
| Snapshot restore and stale-runtime cleanup | Passed |
| Four Caster readiness and dependency metrics | Ready |
| Config generation propagated through etcd Watch | Generation 2 on all nodes |
| Real COM3 capture | 80/80 CRC-valid RTCM3 frames in 10 seconds |
| Real source parser profile | 98 MSM observations, 26 satellites, 9 signals, 0 unknown bytes, 0 CRC errors |
| Correct RTCM constellation mapping | 1124=BeiDou, 1134=NavIC on the live module |
| Real `RTKBASE` source state | `ACTIVE`, last correction age 0 seconds |
| Live restricted MSM configuration | `restrict_versions=true`, allowed `[4]`, source remained ACTIVE, zero MSM-version rejections |
| End-to-end MSM7 rejection | Source closed and `msm_version_not_allowed` metric incremented to 1 |
| Real `RTKBASE` source expectation | `rtcm3`, MSM `[4]`; leading NMEA followed by RTCM3 accepted, source ACTIVE |
| Enforce-mode NMEA-only mismatch | Closed after 512-frame admission window; expected-format and missing-correction metrics incremented; cooldown created and cleared |
| Observe-mode NMEA-only mismatch | Source remained ACTIVE and reject metrics stayed at 0 |
| Real HAProxy PROXY Protocol v2 on Debian | Passed |
| Official RTKLIB `str2str` source/client round trip on Linux | Passed |
| Official RTKLIB RTCM2/RTCM3 fixtures | Passed |
| Official RTKLIB FFI worker | Capability/decode and isolation tests passed |
| External subscriber on Caster 3 | `200 OK`, 452 NMEA lines, 80 RTCM3 frames |
| Four users, four sources and four cross-node subscribers | All source and subscriber handshakes returned 200 |
| Throttled four-user cross-node test | 4 ACTIVE sources on caster-1 through caster-4; 4 unique source and 4 unique client sessions; every subscriber received RTCM |
| Active etcd snapshot | 4 QA source claims and sources, 4 QA generations, 4 QA mount expectations, 8 unique QA sessions |
| Cleanup without real source | Only `RTKBASE` and its owner source session remained |
| Public NTRIP1 STR table | `43.142.126.162:2101` returned `SOURCETABLE 200 OK` and `STR;RTKBASE;` |
| Manual `test` account | Created with NTRIP access; public 2101 pull returned RTCM3 data |
| Old Docker container | Removed edge `source-etcd-1`; no containers remain |
| Final etcd state | 6 users, 6 configured mounts, 1 ACTIVE source, 4 version-status records |

## Build Node Readiness

Checked on 2026-11-23. Compile commands used `CARGO_BUILD_JOBS=1` to fit the
2-vCPU nodes.

| Node | Resources | Build result | Intended use |
|---|---|---|---|
| `101.33.212.76` | 2 vCPU, 1.9 GiB RAM, 41 GiB free; Rust 1.88, protoc 3.21.12, GCC 14 | `cargo clippy --workspace --all-targets -- -D warnings` passed | Clippy/build node |
| `134.175.139.223` | 2 vCPU, 1.9 GiB RAM, 10 GiB free; Rust 1.89, protoc 3.21.12, GCC 14 | `cargo test --workspace` passed: 307 passed, 7 ignored, 0 failed | Primary test node |
| `139.199.14.2` | 2 vCPU, 1.9 GiB RAM, 45 GiB free; Rust 1.89, protoc 3.21.12, GCC 14 | `cargo check --workspace --all-targets` passed in 52 seconds; `cargo build --workspace` passed in 3m30s | Third build/test candidate |
| `43.142.126.162` | 2 vCPU, 960 MiB RAM; 4 GiB swap | Excluded from compilation by design | Edge runtime only |

Node 3 initially lacked the Google protobuf include files. Installing
`libprotobuf-dev` made both the workspace check and debug build pass.

During an earlier test run the existing three-member etcd cluster had no quorum.
The workspace test therefore used a temporary isolated single-node etcd on
`127.0.0.1:32379`; without that override, the two `etcd_backup` tests failed
with `ConnectionReset`. Before and after the rolling update on 2026-11-23, all
three etcd members reported healthy cluster writes and quorum was intact.

## Port Layout

All four nodes expose internal mTLS relay on 12101. Node 2 hosts the shared
admission coordinator on 5757. The edge node reaches etcd through the public
2379 endpoints of all three members. All four nodes expose public NTRIP on
2101 and 2102; the edge listener is the one used by the real source and manual
compatibility clients.

## Linux Fixes Found

- The `serve_binary` TLS test fixture now makes its generated private key
  `0600` on Unix, matching the production key-permission validation.
- The standalone configuration test now creates a protected private-key
  fixture before checking TLS path resolution.
- The updater's Windows-only rollback path no longer emits an unused-variable
  warning on Unix.
- The systemd templates now use a pre-created dedicated `rcaster` account
  instead of `DynamicUser=yes`; a dynamic UID cannot read the deliberately
  `0600` TLS private key.
- `deploy/sysusers/rcaster.conf` creates the dedicated account, and CI creates
  it before validating the units.
- The development etcd Compose ports are bound to `127.0.0.1` instead of all
  interfaces.
- Mixed NMEA-before-RTCM admission now remains pending until the byte/frame
  window ends or correction data arrives, instead of rejecting at the first
  NMEA frame.
- The documented source-admission `max_frames` example is 512 because common
  RTK modules emit a burst of NMEA before their first RTCM correction.
- The real module exposed an RTKLIB `str2str` compatibility requirement:
  NTRIP1 `SOURCE` mountpoints without a leading slash are accepted.
- The mixed NMEA/RTCM test also showed that a `max_frames=32` admission window
  is too small for receivers that emit a complete NMEA update burst before
  RTCM; the cluster test uses 512.
- The external etcd leader-failover test now verifies a real status response
  after creating its lazy gRPC client.
- The restore drill reserves all temporary ports simultaneously and verifies
  etcd status before publishing, preventing accidental port reuse.
- The real BDS/NavIC stream exposed reversed RTCM MSM constellation mapping:
  1121-1127 is BeiDou and 1131-1137 is NavIC. The parser and regression test
  were corrected, then the live profile was revalidated on all four nodes.
- Source admission now supports optional RTCM MSM version restrictions:
  `enforce` rejects disallowed versions, while `observe` records the warning
  and keeps the source online. A dedicated rejection metric was added.
- Mountpoint `source_expectation` adds per-mount format and MSM expectations.
  The real receiver exposed a leading-NMEA failure: an expected RTCM3 format
  was rejected before correction data arrived. The admission window now waits
  for correction data when the only pending violations are a format mismatch
  and missing correction data. A regression test covers this sequence.
- The edge node was switched from loopback-only to public 2101 so common
  plaintext NTRIP clients can read the source table and subscribe. Its plaintext
  client authentication was also enabled for compatibility testing.

## Still Required for Production

- Replace the development account and self-signed certificates with managed
  credentials and CA certificates with complete key-usage extensions.
- The manual `test` account has been deleted. After compatibility testing,
  plaintext NTRIP was restricted to loopback on every node; public clients use
  NTRIP2 over TLS on 2102.
- Complete the running 72-hour soak, target-hardware resource gates, additional
  receiver models, cross-region RTO/RPO drills, and production
  credential/OCSP rotation.

## Online OCSP Integration

- `StandaloneConfig` now exposes `tls_ocsp_responder` for NTRIP/public TLS and
  `cluster.coordinator_tls.server_ocsp_responder` for admin/relay mTLS.
- The response URL must use HTTP or HTTPS and the refresh interval is bounded
  to 1 through 86400 seconds, defaulting to one hour.
- The existing pre-generated DER response remains the fallback when the online
  responder fails and no cached response exists.
- The full Linux workspace tests and the focused non-blocking responder refresh
  test pass. CI runs the focused refresh test explicitly.
- The four test nodes use a self-signed certificate without an OCSP responder
  URL, so online stapling is available but intentionally not enabled there.

## Lease Recovery and Four-Node Soak

Real cross-node fault testing found two recovery defects that the local etcd
proxy tests did not expose:

- A Caster whose lease was lost against one etcd client endpoint kept retrying
  the expired lease ID forever and stayed not ready after network recovery.
- A blackholed client connection could leave keepalive RPCs pending without a
  deadline, so lease health retained its previous true value.

The lease supervisor now bounds keepalive, lease grant, and version-status
publication by a timeout derived from the lease TTL. When the old lease no
longer exists, it grants a replacement, atomically changes the ID used by
admission, rewrites version status, and returns to ready without restarting.
The focused etcd regression test and all 11 cluster-runtime integration tests
pass; the full workspace result is `310 passed, 7 ignored, 0 failed`.

The release binary is
`8315de3903a53c4fe0d85c98f4988d606b697601213900241c9e0f3f1b48676e`
with maximum GLIBC requirement 2.35. It was rolled to all four Casters and the
101 coordinator between 20:28 and 20:29 CST; the previous `6ce223dd…`
binaries remain as per-node rollback files. The final source package is
`rcaster-src-final.tgz`; its SHA-256 is recorded in the adjacent
`rcaster-src-final.tgz.sha256` file so the package does not need to contain a
self-referential hash.

On the Debian 2C2G test node, the candidate release passed:

| Probe | Result |
|---|---|
| 10 sources / 1000 subscribers / 60 s | 0.961 MiB/s; peak RSS 35 MiB; peak FD 2031 |
| 10 sources / 3000 subscribers / 60 s | 2.884 MiB/s; peak RSS 96 MiB; peak FD 6031 |
| 10 sources / 8000 subscribers / 30 s | 7.678 MiB/s; peak RSS 257 MiB; peak FD 16031 |
| 1 source / 10000 subscribers / 5 s | 9.514 MiB/s; peak RSS 267 MiB; peak FD 20013 |
| 10000 sources / 1 subscriber / 5 s | 33 MiB source input; peak RSS 331 MiB; peak FD 20013 |

Every probe ended with zero active sources and zero remaining subscribers.
A real Caster client-endpoint blackhole produced `not ready` and `lease=0`
within 15 seconds; after removing the blackhole, the same PID returned to
`ready` with `lease=1`.

A separate full-quorum-loss drill isolated all three etcd members from each
other on peer port 2380. Within 45 seconds every Caster returned `not ready`
with `lease=0`. After all rules were removed, all three etcd endpoints reported
healthy committed proposals and all four Casters returned to `ready` with
their original PIDs; no process restart was required.

All four nodes now run a 72-hour loopback data-plane soak as a transient
systemd unit named `rcaster-soak-72h.service`, with CPU quota 50% and bounded
memory:

| Node | Sources | Subscribers | MemoryMax | LimitNOFILE |
|---|---:|---:|---:|---:|
| `101.33.212.76` | 10 | 100 | 512 MiB | 131072 |
| `134.175.139.223` | 10 | 100 | 512 MiB | 131072 |
| `139.199.14.2` | 10 | 100 | 512 MiB | 131072 |
| `43.142.126.162` | 5 | 50 | 256 MiB | 65536 |

The units write their final JSON to `/tmp/rcaster-soak-72h.json` and errors to
`/tmp/rcaster-soak-72h.err`. This run started before the code freeze and was
stopped at 01:44 CST on 2026-11-24 after 6 hours 17–40 minutes. It is retained
as a development pre-run only; the 72-hour acceptance run must be restarted
after the final candidate is frozen.

## Target etcd Restore and Scheduled Backup

A point-in-time snapshot was taken from the live three-member etcd cluster on
101:

- Snapshot: `/tmp/rcaster-prod-snapshot-20261123T113054Z.db`
- Size: 581,664 bytes
- SHA-256: `ba816dd649f610d70361f651e615a4a97276caa117a0c607c320ef9824516e32`
- Backup command duration: 18 ms

The snapshot was restored with `etcdutl` into a new data directory on loopback
ports 32384/32385; restore took 58 ms and the isolated member became healthy.
Before cleanup the restored database contained 11 runtime keys and 21 config
keys. `rcaster etcd-recovery-prepare` completed in 12 ms, removed all 11 stale
runtime keys, and left runtime, online, and lock prefixes empty. Configuration
remained at 21 keys with generation 2, 6 users, and 6 mountpoints.

`rcaster etcd-backup` now supports `--backup-dir` and `--retention`. It writes
private timestamped `rcaster-*.db` snapshots and only prunes files matching
that prefix and suffix. The repository includes hardened
`rcaster-etcd-backup.service` and `rcaster-etcd-backup.timer` templates; the
timer is persistent across downtime, runs every 15 minutes, and defaults to
672 snapshots (seven days).
The timer was installed only on 101. Its first snapshot completed successfully
as `/var/lib/rcaster/etcd-backups/rcaster-1795433844.db`, owned by
`rcaster:rcaster` with mode 0600 and size 581,664 bytes. The next hourly run
was scheduled by systemd.

The next scheduled run completed at 21:03:45 CST and wrote
`/var/lib/rcaster/etcd-backups/rcaster-1795439025.db`. The file is owned by
`rcaster:rcaster`, mode 0600, 1,261,600 bytes, SHA-256
`c61e9e06f98ee8ee59fe434c977a99bc7a20d35edb1a652a2ac722f693c685f8`.
`etcdctl snapshot status` reported revision 1578, 2507 total keys, and 1.3 MB.
Retention kept 168 snapshots and removed none. The next run is scheduled for
22:01:42 CST.

## updater Debian 12 Compatibility

The Debian 13 updater required GLIBC 2.39 and could not execute on the Debian
12 edge node. The updater was instead built from the final source package on
the Debian 12 node:

- SHA-256: `125efc6615bf32e29de48dc8de52512d3e299f6290a418ad9f2c49794a0b787d`
- Size: 6,851,152 bytes
- Maximum GLIBC requirement: 2.34
- `--help` successfully executed on Debian 12

The binary was installed as `/opt/rcaster/rcaster-updater` on all four nodes.
`rcaster-updater.service` remains inactive everywhere because the Caster
services are currently supervised directly; enabling both would violate the
unit `Conflicts=` policy.

## Cross-Node Relay Soak and Watch Convergence

Cross-node testing exposed that the relay listener moved to
`ReloadableTlsConfig` during the OCSP work without advertising ALPN `h2`.
OpenSSL completed the TLS handshake, but Tonic could not establish HTTP/2 and
all remote subscriptions returned 503. Reloadable TLS now has explicit gRPC
constructors that preserve `h2` across certificate, CRL, and OCSP reloads.
The focused ALPN regression test passes.

After the ALPN fix, a cross-node smoke test used 10 sources distributed across
the four Casters and 50 subscribers that connected to different Caster nodes:

- Duration: 60 seconds
- Source bytes: 674,880
- Subscriber bytes: 3,374,400
- Subscriber throughput: 0.054 MiB/s
- Process peak FDs: 70
- Errors: none

A cross-node relay pre-run ran on 101 as
`rcaster-cluster-soak-72h.service`. It used `SOAK008` through `SOAK017`,
10 sources, 50 subscribers, 1 KiB/s/source, 50% CPU quota, 256 MiB memory
limit, and 65,536 file descriptors. Endpoints spanned all four Caster nodes
over TLS 2102. It was stopped at 01:44:04 CST on 2026-11-24 after
5 hours 13 minutes and is not a final 72-hour result.

etcd Watch streams now have a 30-second idle bound. Config, blacklist, source,
and session watchers reconnect and rebuild snapshots on timeout. Account
watching reconnects from its last observed revision without periodically
re-reading the full account set; compaction still forces a full snapshot.
The full-quorum revalidation for this Watch repair remains pending so the
active cross-node soak is not interrupted.

Final verification at 20:31 CST rechecked all four Caster instances:

- `rcaster.service` was `active`, `/health/live` returned `ok`, and
  `/health/ready` returned `ready` on every node.
- Every dependency gauge remained `1`.
- The process image SHA-256 remained `8315de39…48676e` on all four Casters
  and on the 101 coordinator.
- The hourly etcd backup timer completed its scheduled run successfully. The
  newest snapshot was 725,024 bytes and the next run was scheduled for 21:03.
- Four node-local 72-hour soak units and the cross-node relay soak were active.
- The compatible updater binary is installed on all four nodes, but no
  `rcaster-updater` service unit is enabled or active.

## Public-Only TLS Startup Repair

A reverse-proxy drill exposed a startup defect when the public API was enabled
with a certificate and private key but the NTRIP TLS listener was intentionally
left disabled. The listener initialization rejected that valid public-only
configuration and exited with `incomplete TLS configuration`.

Listener startup now distinguishes the two cases:

- Without `tls_bind`, the NTRIP TLS listener is not started; public TLS remains
  available through `public_bind`.
- With `tls_bind`, a missing certificate or private key is still rejected
  before startup.

The regression test
`serve_starts_public_tls_without_ntrip_tls_listener` launches the real
`rcaster` process, connects to the public listener over TLS, requests `/`, and
asserts a successful response. The test failed against the previous listener
branch and passes with the repair.

Fresh verification completed at 20:57 CST:

- `cargo test --workspace --locked`: 312 passed, 7 ignored, 0 failed.
- `cargo fmt --all -- --check`: passed.

### Node Clock and Backup Cadence

All four nodes reported `NTPSynchronized=yes` in `Asia/Shanghai`. The three
nodes exposing chrony tracking data had system-time offsets between roughly
0.014 ms and 0.096 ms, with a maximum RMS offset of about 0.196 ms during this
observation. The Debian 12 edge node runs NTPsec and does not provide
`chronyc`; an unprivileged `adjtimex` read as the `rcaster` user succeeds.
At 01:39 CST its kernel offset was about 0.3465 seconds, consistent with
`ntpq` reporting 0.370 seconds for the selected NTP peer. This is a genuine
clock-skew finding that should activate `RCasterClockSkewHigh`, not a metric
collection failure.

Linux releases now expose `rcaster_node_clock_offset_seconds` and
`rcaster_node_clock_synchronized`. The implementation prefers
`chronyc -c tracking` and falls back to `adjtimex`. On the Debian 12 node, the
release metric returned offset `0.000221976` and synchronized `1`, matching
chrony's system-time value of `0.000222199`. The Prometheus ruleset includes
`RCasterClockNotSynchronized` and `RCasterClockSkewHigh`; the latter computes
the spread between node offset maxima and minima and alerts above 250 ms.

The corrected 15-minute backup timer continues on schedule. The latest
observed snapshot at 01:30 completed with `Result=success` and exit status 0.

### Recovery-to-Ready RTO Baseline

The real `etcdutl` drill now starts timing when the source etcd is stopped and
continues through snapshot restore, stale runtime cleanup, and startup of a
real `rcaster serve`. Readiness requires `/health/ready`, all five cluster
dependency gauges, and config generation 7. Two local runs completed in 905 ms
and 771 ms.

This is a WSL recovery-path baseline, not a production SLA: target hardware,
three-member total loss, TLS/etcd identity recovery, and cross-region operator
steps still require separate drills.

### Real Receiver Validation

The local `COM3` device is a real RTK base module behind a `USB-SERIAL CH340`
bridge. Read-only framing checks identified RTCM3 at 460800 baud. It was sent
through an isolated real Caster over NTRIP2 while an independent subscriber
consumed the same source.

The 15-second run received 15,650 source bytes and 15,138 subscriber bytes.
CRC-valid frames were 128 at the source and 123 at the subscriber. Both paths
contained RTCM3 1005, 1074, 1084, 1094, 1114, 1124, and 1134, with NTRIP2 200
responses and no socket or parser errors. The USB descriptor does not expose a
receiver model, and no 1033 message was emitted, so receiver/firmware identity
remains unrecorded.

The full status is maintained in
`docs/runbooks/ntrip-compatibility-matrix.md`. Additional receiver models
remain explicitly unverified.

u-center 25.03 was subsequently validated as an NTRIP client against the same
real C63 source. It loaded the RCaster sourcetable, selected C63, and reported
`NTRIP client: 127.0.0.1:2101`. Its received-byte counter increased from 29,873
to 32,624 during a three-second socket sample. An idle COM1 was used as the
receiver output so u-center did not write corrections back into the real COM3
source.

BKG BNC 2.13.7 (`BNC-2.13.7-Windows.msi`, SHA-256
`5A22015247D015692FB538D47B55854969CFC3B2540E4B008BA8846934F9B536`) and
Lefebure NTRIP Client (`NTRIPClient.exe`, SHA-256
`577149DF7BFFC16390F10B809B37C565E76260606F910124E498203B28F72ECF`)
were downloaded from their official sites. Both are unsigned and were executed
only after explicit user confirmation.

BNC 2.13.7 ran from its extracted package in `--conf <file> --nw` mode. Its
log continuously decoded C63 messages 1005, 1074, 1084, 1094, 1114, 1124, and
1134. The final raw output was 41,550 bytes and 287 CRC-valid RTCM3 frames.

Lefebure 2017.07.27 loaded C63 from the sourcetable and reported
`NTRIP Client is receiving data`; its received-byte display grew from 341 to
1,200 and then 6,360 bytes.

The Windows RTKLIB 2.5.0 `strsvr` GUI was configured manually against the same
local C63 source. During a three-second socket sample, its subscription
connection received bytes grew from 24,296 to 33,640 while the source grew
from 107,495 to 110,054 bytes. The Windows GUI path is therefore verified.

### Debian 12 Public-Only TLS Candidate Check

The release candidate was copied to the Debian 12 test node and started as an
isolated process on alternate ports 3101 and 3202 without restarting the live
`rcaster.service`. The configuration enabled only `public_bind` plus the node
certificate and private key, with no NTRIP `tls_bind`.

The process started successfully. HTTPS `/` returned 200 with a 17,097-byte
page and CSP, HSTS, nosniff, and Referrer-Policy headers; 
`/api/v1/public/str` returned 200. The temporary process was stopped and the
alternate ports and directory were removed after validation.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo build --workspace --release --locked`: passed.

The new Caster candidate is
`8b10ad7f0dadb0b4ae1fe66abc84116efaa5f48864c9b99da9099b1ed2627e42`
with maximum GLIBC requirement 2.35. It has not been deployed because all four
node-local soak units and the cross-node relay soak are still running. The WSL
updater build requires GLIBC 2.39 and is not suitable for the Debian 12 edge
nodes; the existing compatible updater remains installed and inactive.

The node-local soak units had zero restarts and no warning-level journal
entries. The cross-node soak began at 20:30:54 CST with zero restarts and empty
stderr. Result JSON files remain empty by design until each soak exits.

### Backup RPO Correction

The design requires etcd configuration snapshots every 15 minutes with an RPO
of at most 15 minutes. The first deployed timer still used an hourly schedule,
which left the production configuration outside that requirement. The timer
template and 101 deployment were corrected to use `OnCalendar=*:0/15`,
`AccuracySec=1s`, and `Persistent=true`. The five-minute randomized delay was
removed so scheduling jitter cannot extend the RPO. Retention defaults to 672
snapshots to preserve the existing seven-day window at the new cadence.

The corrected timer was deployed on 101 at 23:47 CST and reported the next run
at 00:00. A manual service run completed successfully under the new
environment, retained 672 snapshots, and wrote a 2,027,552-byte snapshot. The
previous unit and environment files were preserved under
`/root/rcaster-config-backups/`.

### Final Candidate and Quorum Recovery

The final candidate
`8e29de8b864d53acc80dbf5b906e18a905358c9aaed4e6a8e3fd1e4e25458a0d`
was rolled to all four Casters and the 101 coordinator on 2026-11-24 between
02:21:06 and 02:21:20 CST. Upload and hash checks preceded each atomic replace;
every service returned live/ready and reported all five cluster dependencies
healthy. The coordinator mTLS verification returned 0. Per-node rollback files
use the suffix `8e29de8b`.

The edge node runs NTPsec rather than chrony. Before its service restart its
kernel offset was approximately 0.37 seconds; after restart the new
`rcaster_node_clock_offset_seconds` metric reported 0.038 seconds, below the
250 ms skew guard. The metric reads the kernel `adjtimex` syscall directly as
the unprivileged `rcaster` user.

For the Watch regression, an independent `nft` table dropped bidirectional TCP
port 2380 on all three etcd members. Within 20 seconds all four Casters returned
HTTP 503 with `lease=0`; all four retained their PIDs. After removing the table,
the same PIDs returned to HTTP 200 with every dependency healthy. Each etcd
member then completed a committed health proposal in 11–20 ms.

A new `watch-probe` user and `WATCHPROBE` mountpoint were published through the
101 loopback mTLS admin listener while every Caster was running. All four
Castors authenticated that new account without restart and returned 503 because
the mountpoint had no source; the incorrect-password path returns 401. The
temporary user, mountpoint, 30 SOAK mountpoints, credentials, and both custom
Watch accounts were removed after validation. The cluster returned to six
configured users and six configured mountpoints.

The obsolete manual `test` account was then deleted, leaving five users. The
plaintext NTRIP bind was changed from `0.0.0.0:2101` to `127.0.0.1:2101` on all
four nodes. This preserves the SSH-tunnel source path while removing public
plaintext service. Public `2101` connection attempts fail and NTRIP2 on TLS
`2102` still returns a 200 source table. The loopback admin listeners on 101
and 43 require mTLS and a service token.

The final source archive is generated from the final commit as
`rcaster-src-final.tgz`; its SHA-256 is recorded in the adjacent `.sha256`
file. It excludes Git metadata, build output, historical archives, private
keys, temporary credentials, and the locally modified README.

Before the final soak, a management API shape regression was found in
`GET /admin/v1/sources`: the route wrapped the already paginated response a
second time. The route now returns `sources` as a top-level array plus
`next_cursor`, and the admin integration test parses and asserts that shape.
This changed the release hash from `8b10ad7f…` to `8e29de8b…`; the new binary
was revalidated with 318 passing tests, Clippy, a 60-second cross-region relay
smoke, and another full-quorum loss/recovery drill before the final soak.

### Final 72-Hour Acceptance Run

The final local soak units `rcaster-final-soak-72h.service` started on all four
nodes between 02:25:21 and 02:25:24 CST on 2026-11-24. The 101, 134, and 139
nodes run 10 sources and 100 subscribers; the smaller 43 edge node runs 5
sources and 50 subscribers. All four units are active with zero restarts and
empty stderr.

The cross-region unit `rcaster-final-cluster-soak-72h.service` started on 101
at 02:29:15 CST with 10 sources and 50 subscribers distributed over all four
TLS endpoints. Twenty seconds after start it was active with zero restarts,
empty stderr, 10 cluster sources, and 60 total sessions (10 sources plus 50
subscribers). Its expected completion is 2026-11-27 02:29:15 CST. Source-connect
setup in the soak helper now retries transient 503/connection failures 20 times
at 500 ms intervals; this changes only the test helper, not the production
Caster binary.

Final result files are `/tmp/rcaster-final-soak-72h.json` on each node and
`/tmp/rcaster-final-cluster-soak-72h.json` on 101. They remain empty until the
processes exit normally after 259,200 seconds. No further production code or
node configuration changes are part of this acceptance window.

### Repository and CI

The source is published to both
`https://github.com/Rampump/Rcaster.git` and
`https://gitcode.com/rampump/Rcaster`. The local README modification was not
committed and is excluded from the source archive. GitHub Actions initially
failed at environment setup because its etcd service container did not start
and the test job lacked HAProxy. The workflow now downloads and starts the etcd
binary directly and installs HAProxy in the test job.

GitHub Actions run `45242799276` completed successfully for both `msrv` and
`test`, covering RustSec, formatting, Clippy, workspace tests, OCSP and lease
recovery, etcd credential rotation/leader failover/snapshot restore, HAProxy
PROXY v2, official RTKLIB, RTCM and BINEX fixtures, all-feature tests,
data-plane smoke tests, and the release build. GitCode's pre-receive checks
also passed for both final code pushes.

### Watch Health and Silent-Stall Recovery

`ConfigWatcher` previously set its healthy flag before the asynchronous
`client.watch` stream had been established. It now remains unhealthy until
the stream is successfully created and only reports ready after that point.

Two real fault regressions were added:

- A pausable TCP proxy holds both directions open while blackholed, proving
  the test does not recover merely because sockets were closed.
- An established config Watch is blackholed for 35 seconds. The 30-second idle
  bound marks it unhealthy, and reconnecting after resume converges to the
  generation published during the stall without a process restart.

A three-member local etcd regression also stops two members, confirms the
Watch becomes unhealthy, restarts those members with their original data
directories, and verifies convergence after quorum returns.

Fresh combined verification:

- `cargo test --workspace --all-features --locked`: 318 passed, 8 ignored,
  0 failed.
- `cargo clippy --workspace --all-targets --all-features --locked --
  -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.

Source, blacklist, and account watchers now use the same readiness rule as
config Watch: healthy is set only after the watch stream is established.
`ClusterRuntime::start` waits for all four watches and closes them if they do
not become ready within 30 seconds, returning `WatchesUnavailable` instead of
exposing a partially initialized runtime. The 11 cluster-runtime tests,
including remote gRPC coordinator admission, pass after the change.

## External Compatibility and Failure Verification

All seven integration tests ignored by the default workspace run were executed
with their real external dependencies and passed:

| Area | Real dependency and result |
|---|---|
| PROXY v2 | HAProxy; source client IP was preserved through the proxy |
| etcd credentials | Local TLS etcd; client credentials rotated without restarting the client |
| etcd leader failover | Config Watch recovered after the leader process stopped |
| etcd restore | Snapshot restored into an isolated member; stale runtime state was removed |
| NTRIP round trip | RTKLIB `str2str` acted as a real source and client over RTCM3 |
| RTCM fixtures | Official RTCM2/RTCM3 fixtures matched across seven chunking modes |
| BINEX | Public stream parsed as 90,010 frames with zero unknown bytes and zero CRC errors |

RTKLIB was built from commit
`71db0ffa0d9735697c6adfd06fdf766d0e5ce807` (2.4.2 p13). The official
fixtures produced:

| Fixture | Bytes | Frames | Unknown bytes | CRC errors |
|---|---:|---:|---:|---:|
| `testglo.rtcm2` | 153,397 | 1,727 | 6,292 | 0 |
| `testglo.rtcm3` | 57,931 | 429 | 58 | 0 |
| `GMSD7_20121014.rtcm3` | 262,144 | 1,143 | 0 | 0 |

The public BINEX fixture was 17,596,434 bytes after decompression, parsed as
90,010 frames with zero unknown bytes and zero CRC errors. Its SHA-256 is
`e3971489fbcfd34d2b871289cc62eea83dec714ea11359491746be0f2dff6d20`.

The optional `rtklib-ffi` worker reported RTKLIB 2.4.2, decoded RTCM3 1005,
and kept a partial frame from leaking into the next request. Its crash,
timeout, oversized input/output, and restart tests all passed.

Fresh combined verification:

- `cargo test --workspace --all-features --locked`: 314 passed, 7 ignored,
  0 failed.
- `cargo clippy --workspace --all-targets --all-features --locked --
  -D warnings`: passed.

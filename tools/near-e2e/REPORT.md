# NEAR Standalone End-to-End Report

Test date: 2026-11-25 CST

## Environment

- Windows workstation with WSL2 Debian 13
- Linux kernel: `6.6.87.2-microsoft-standard-WSL2`
- CPU available to WSL: 12 vCPU
- Memory available to WSL: 15 GiB
- Python: 3.13.5
- Rust: 1.100.0
- Cargo: 1.100.0
- Release binary: `target/release/rcaster`

All Caster, NTRIP source, client, redb, and HTTP traffic stayed on
`127.0.0.1`. The four soak servers were not contacted.

## Candidate snapshot

- Source: one `GET http://crtk.net:2101/` request
- Snapshot: `tools/near-e2e/data/crtk-str-20261125.json`
- SHA-256:
  `DB5C613FD1ADA2F11D3B0A01A703F901E1D35AB9DBEC6435D4801A42EBA986D4`
- Parsed RTCM3 records: 1,302
- Locally sourced records: 1,301 after excluding the reserved synthetic
  mountpoint name `NEAR`
- No subscription, polling, or correction-stream access to CRTK.NET

## L1 model verification

Command:

```bash
cargo test -p rcaster-service --test near \
  offline_snapshot_matches_full_scan_and_stationary_arrival_switches \
  --locked -- --ignored --nocapture
```

Result: pass.

- 200 deterministic random query points matched a full Haversine scan.
- 10 synthetic paths switched once in order after reaching the destination and
  remaining stationary through the observation window.
- A separate 8 m/s continuous-crossing test switches once after three seconds
  of cumulative progress toward the selected target.

## L2a real NTRIP socket verification

Evidence:

- `tools/near-e2e/runs/20261125-030830/summary.json`
- `tools/near-e2e/runs/20261125-030830/*.jsonl`
- `tools/near-e2e/runs/20261125-030830/metrics.json`
- `tools/near-e2e/runs/20261125-030830/metrics-clean.json`
- `tools/near-e2e/runs/20261125-030830/str-table.txt`

| Case | Expected | Actual | Result |
|---|---|---|---|
| L2-01 static 20 | 20/20 selected and delivered | 20/20 | PASS |
| L2-02 99/101 km | 99 km receives; 101 km receives no station | same | PASS |
| L2-03 hysteresis | no switch under jitter | no switch | PASS |
| L2-04 A->B, 1 s query cadence | A then B, first switch not before 3 s | A then B at 6.052 s | PASS |
| L2-05 A->B->C, 1 s query cadence | ordered A, B, C | A, B at 6.036 s, C at 18.103 s | PASS |
| L2-06 60 km jump | rejected, station stays A | rejected, station A | PASS |
| L2-07 GGA burst | rate limiter increments, station unchanged | same | PASS |
| L2-08 HDOP 2.6 | invalid, no switch | station A | PASS |
| L2-09 position TTL | stale source excluded | no station after TTL | PASS |
| L2-10a source disconnect failover | switch to second source | stations 2 then 577, connection stayed open | PASS |
| L2-10b failover keeps client | no client disconnect after failover | no close event | PASS |
| L2-10c no alternative source | close within 10 s | closed at 12.002 s, 10 s after source loss | PASS |
| L2-11 20 clients | 20/20 independent selections | 20/20 | PASS |
| STR coordinates | no uploaded `0,0` coordinate | 0 rows | PASS |
| Cleanup | active sources 0, stderr 0 bytes | same | PASS |

Observed NEAR events during L2a:

- `query=210`
- `gga_rate_limited=3`
- `gga_jump_rejected=4`
- `gga_invalid=5`
- cleanup: `active_sources=0`

The runtime source STR coordinates came from rewritten RTCM3 1005 ECEF values.
No mountpoint coordinate override was required.

## L2b full-table sampling

Evidence:

- `tools/near-e2e/runs/20261125-031140-l2b/summary.json`
- `tools/near-e2e/runs/20261125-031140-l2b/l2b.jsonl`
- `tools/near-e2e/runs/20261125-031140-l2b/metrics.json`
- `tools/near-e2e/runs/20261125-031140-l2b/cleanup-metrics.json`

Result: pass.

- Simultaneous local correction sources: 1,301
- Non-station query samples: 15 neighbor midpoints, 15 midpoint ±50 m and
  20 quarter-point interpolations
- Distributed query samples: 50
- Correct station/data matches: 50
- Hit rate: 100%
- Required threshold: 95%
- Cleanup: `active_sources=0`, `stderr_bytes=0`

Every sampled query whose expected result was a station received the expected
RTCM3 1005 station ID. Two sparse-region samples correctly returned no source.

## Notes and corrections found during validation

- RTCM3 1005 ECEF fields must be encoded in 0.1 mm units. Writing meters
  produced plausible longitude but invalid latitude and was corrected before
  the passing runs.
- Standalone `admin_bind` originally required a nonexistent
  `cluster.coordinator_tls`. The configuration validation, standalone admin
  startup, login session handling and local source-list path were fixed and
  covered by binary-level tests.
- `validation_buffer_max_bytes` must cover
  `max_validating_sources * source_admission.max_bytes`; the harness sets an
  explicit 1 GiB bound.
- The L2 switching evidence uses `min/max_recompute_interval_ms=1000`. Under
  production defaults, a stationary client can wait for the 5-second maximum
  recompute interval and then complete the 3-second observation window, so the
  expected practical switch latency is roughly 5–10 seconds.

## Conclusion

The local standalone data path passes for quasi-static, moving, failover and
1-second-query operation:

`GGA input -> spatial selection -> source switch -> selected source RTCM data`

Continuous movement, active-source failover and the no-alternative timeout are
now covered. Under production-default recompute intervals, a stationary client
can still take roughly 5–10 seconds to complete a switch; that remains a
documented latency characteristic rather than an unverified failure.

This evidence does not replace the external Soak, real CMR, DNS/LB, receiver
compatibility, or rolling-upgrade acceptance tests.

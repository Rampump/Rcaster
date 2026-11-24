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
  offline_snapshot_matches_full_scan_and_motion_paths \
  --locked -- --ignored --nocapture
```

Result: pass.

- 200 deterministic random query points matched a full Haversine scan.
- 10 synthetic motion paths switched once in order.
- First switch was never earlier than the configured 3-second observation
  window.

## L2a real NTRIP socket verification

Evidence:

- `tools/near-e2e/runs/20261125-010913/summary.json`
- `tools/near-e2e/runs/20261125-010913/*.jsonl`
- `tools/near-e2e/runs/20261125-010913/metrics.json`
- `tools/near-e2e/runs/20261125-010913/metrics-clean.json`
- `tools/near-e2e/runs/20261125-010913/str-table.txt`

| Case | Expected | Actual | Result |
|---|---|---|---|
| L2-01 static 20 | 20/20 selected and delivered | 20/20 | PASS |
| L2-02 99/101 km | 99 km receives; 101 km receives no station | same | PASS |
| L2-03 hysteresis | no switch under jitter | no switch | PASS |
| L2-04 A->B | A then B, stable observation honored | A then B | PASS |
| L2-05 A->B->C | ordered A, B, C | A, B, C | PASS |
| L2-06 60 km jump | rejected, station stays A | rejected, station A | PASS |
| L2-07 GGA burst | rate limiter increments, station unchanged | same | PASS |
| L2-08 HDOP 2.6 | invalid, no switch | station A | PASS |
| L2-09 position TTL | stale source excluded | no station after TTL | PASS |
| L2-10 source disconnect | switch to next or close within 10 s | connection closed at 2.001 s | PASS |
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

- `tools/near-e2e/runs/20261125-011201-l2b/summary.json`
- `tools/near-e2e/runs/20261125-011201-l2b/l2b.jsonl`
- `tools/near-e2e/runs/20261125-011201-l2b/metrics.json`
- `tools/near-e2e/runs/20261125-011201-l2b/cleanup-metrics.json`

Result: pass.

- Simultaneous local correction sources: 1,301
- Distributed query samples: 50
- Correct station/data matches: 50
- Hit rate: 100%
- Required threshold: 95%
- Cleanup: `active_sources=0`, `stderr_bytes=0`

Every sampled query received RTCM3 1005 from the expected source station ID.

## Notes and corrections found during validation

- RTCM3 1005 ECEF fields must be encoded in 0.1 mm units. Writing meters
  produced plausible longitude but invalid latitude and was corrected before
  the passing runs.
- The standalone admin listener currently requires cluster coordinator TLS.
  This test therefore used the standalone NTRIP listener and health metrics
  endpoint and did not require an admin API.
- `validation_buffer_max_bytes` must cover
  `max_validating_sources * source_admission.max_bytes`; the harness sets an
  explicit 1 GiB bound.
- For deterministic switch timing, the test sets
  `min/max_recompute_interval_ms=1000`, while keeping the production 3-second
  observation window and hysteresis rules.

## Conclusion

L1, L2a, and L2b all meet the requested local acceptance threshold. This proves
the standalone data path:

`GGA input -> spatial selection -> source switch -> selected source RTCM data`

It does not replace the separate dual-node relay case or the external Soak,
CMR, DNS/LB, or rolling-upgrade acceptance tests.

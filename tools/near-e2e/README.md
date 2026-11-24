# NEAR Standalone End-to-End Validation

This harness validates the complete local path:

`GGA -> NearIndex/NearSelector -> source subscription -> selected RTCM source`

It never contacts the four soak servers and does not pull correction streams
from CRTK.NET. One explicit CRTK.NET `GET /` creates the checked-in STR snapshot;
all later runs are offline.

## Prerequisites

- Linux/WSL Python 3.10 or newer.
- A release `rcaster` binary.
- `target/com3-rtcm3-capture.bin`, the real COM3 RTCM3 capture.
- `tools/near-e2e/data/crtk-str-20261125.json`.

## Refresh the CRTK snapshot

Only run this when a fresh public STR snapshot is required:

```bash
python3 tools/near-e2e/fetch_crtk_str.py
```

The script sends one request with a User-Agent and writes the parsed RTCM3
records plus fetch metadata. It does not subscribe to a correction stream.

## L1 offline model verification

```bash
cargo test -p rcaster-service --test near \
  offline_snapshot_matches_full_scan_and_stationary_arrival_switches \
  --locked -- --ignored --nocapture
```

This checks 200 deterministic random queries against a full Haversine scan and
10 stationary-arrival paths with the production `NearIndex` and
`NearSelector`. A separate normal test covers continuous moving-client
switching.

## L2a real socket verification

From the repository root:

```bash
python3 tools/near-e2e/run_e2e.py \
  --binary target/release/rcaster \
  --snapshot tools/near-e2e/data/crtk-str-20261125.json \
  --capture target/com3-rtcm3-capture.bin \
  --keep
```

The harness imports one owner and all snapshot mountpoints, rewrites RTCM3
1005 station IDs/ECEF coordinates, starts local NTRIP sources, and checks:

- 20 static points
- 99 km / 101 km boundary
- hysteresis / no flapping
- one and two ordered source switches
- rejected 60 km GGA jump
- GGA rate limiting
- invalid HDOP
- stale source position TTL
- active source disconnect
- active source failover and no-alternative timeout
- 20 concurrent clients
- non-zero STR coordinates
- source cleanup and empty stderr

Each case writes a JSONL file under `tools/near-e2e/runs/<timestamp>/`.
`summary.json`, `metrics.json`, `metrics-clean.json`, and `str-table.txt` are
written beside it.

## L2b full-table sampling

```bash
python3 tools/near-e2e/run_l2b.py \
  --binary target/release/rcaster \
  --snapshot tools/near-e2e/data/crtk-str-20261125.json \
  --capture target/com3-rtcm3-capture.bin \
  --keep
```

This starts 1,301 local correction sources (the CRTK mountpoint named `NEAR`
is excluded because it is the synthetic Caster mountpoint), then checks 50
neighbor midpoints, midpoint offsets and interpolated points rather than
querying each station at its own coordinates. The required hit rate is at least
95%; sparse points with no source within 100 km must also return no station.

All generated run directories are ignored by Git. Temporary redb files,
processes, sockets, and logs are removed unless `--keep` is used.

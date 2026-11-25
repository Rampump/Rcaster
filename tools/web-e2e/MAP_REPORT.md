# Public Map Marker Validation

Date: 2026-11-26

## Data

- Source snapshot: `tools/near-e2e/data/crtk-str-20261125.json`
- Source endpoint: `http://crtk.net:2101/`
- RTCM3 records before filtering: 1,302
- Valid map features after excluding the reserved `NEAR` mountpoint: 1,301
- Snapshot fetch was repeated independently on 2026-11-26 and returned 1,291
  current RTCM3 records; the committed 2026-11-25 snapshot remains the
  reproducible test input.

## Browser measurements

Evidence:

- `tools/web-e2e/runs/20261126-005946/result.json`
- `tools/web-e2e/runs/20261126-005946/map-standalone.png`
- `tools/web-e2e/runs/20261126-005946/map-clustered.png`

| Mode | Tile projection | Features | Global clusters | Dense markers | Max pixel error |
|---|---|---:|---:|---:|---:|
| standalone | XYZ / WGS-84 | 1,301 | 17 | 15/15 visible | 0.0 px |
| clustered | AMap GCJ-02 | 1,301 | 17 | 15/15 visible | 0.0 px |

The dense cluster was centred on CRTK station `IREN` and contained 15 stations
within 30 km. Every rendered marker was compared with an independently
computed Web Mercator pixel position.

The GCJ-02 control point `116.397,39.909` differs from its WGS-84 input by
554.92 m. RCaster converts marker coordinates to GCJ-02 for AMap and converts
viewport bounds back to WGS-84 before querying map data.

## Tile providers

Direct request checks from the Windows host:

- OSM `https://tile.openstreetmap.org/5/26/11.png`: HTTP 200, `image/png`,
  18,750 bytes.
- AMap satellite
  `https://webst01.is.autonavi.com/appmaptile?style=6&x=26&y=11&z=5`:
  HTTP 200, `image/jpeg`, 8,551 bytes.
- AMap satellite loaded 15/15 visible browser tiles in clustered mode.
- OSM was not reachable from the WSL Chromium egress during this run, so the
  standalone browser test uses a local HTTPS XYZ/PNG server with identical
  WGS-84 tile semantics. The real OSM endpoint was verified independently
  from Windows.

Configuration examples are in `docs/runbooks/public-map-tiles.md`.

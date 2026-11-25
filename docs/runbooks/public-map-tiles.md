# Public Map Tile Configuration

RCaster renders mountpoint markers as an SVG overlay on standard `{z}/{x}/{y}`
XYZ tiles. The data coordinates in STR are WGS-84. Choose the tile coordinate
system explicitly; do not display WGS-84 markers directly on GCJ-02 tiles.

## OpenStreetMap / WGS-84

Use OSM for global CRTK.NET coverage:

```json
{
  "public_map_tiles": {
    "url_template": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    "attribution": "© OpenStreetMap contributors",
    "min_zoom": 0,
    "max_zoom": 19,
    "coordinate_system": "wgs84"
  }
}
```

OSM's public tile service is suitable for light validation. A production
deployment should follow the OSM tile usage policy or use an OSM-compatible
commercial/self-hosted provider.

## AMap / GCJ-02

For a China-region deployment, an AMap satellite XYZ endpoint can be used:

```json
{
  "public_map_tiles": {
    "url_template": "https://webst01.is.autonavi.com/appmaptile?style=6&x={x}&y={y}&z={z}",
    "attribution": "© 高德地图",
    "min_zoom": 0,
    "max_zoom": 18,
    "coordinate_system": "gcj02"
  }
}
```

When `coordinate_system=gcj02`, RCaster converts WGS-84 STR coordinates to
GCJ-02 for marker display and converts viewport bounds back to WGS-84 before
querying the map API. This keeps CRTK/RTCM-derived stations aligned with AMap
tiles inside China.

AMap public tile endpoints, quotas and usage terms can change. Confirm the
current provider terms before production use, or replace the template with a
licensed/self-hosted tile service.

The map API still stores and serves WGS-84 coordinates. The coordinate system
setting only affects browser tile alignment.

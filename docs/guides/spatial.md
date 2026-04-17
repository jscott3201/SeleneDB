# Spatial Queries

SeleneDB stores geometries as a first-class property type and ships 18 OGC-aligned `ST_*` scalar functions for point-in-polygon, distance, intersection, and envelope operations. Everything runs inside the database process — no PostGIS sidecar, no external tile server.

## When to use it

| You want to | Reach for |
|-------------|-----------|
| Tag sensors, assets, or nodes with a location | `Value::Geometry` property |
| Find all entities inside a zone polygon | `ST_Contains` in `WHERE` |
| Rank by distance from a reference point | `ST_Distance` in `ORDER BY` |
| Filter by "within N meters" | `ST_DWithin` |
| Check if two shapes overlap at all | `ST_Intersects` |
| Compute area of a zone or length of a run | `ST_Area`, `ST_Length` |
| Get the axis-aligned bounding box | `ST_Envelope` |

If you need raster data, network routing, or full OGC Simple Features coverage (M/Z coordinates, CRS reprojection, topology operations), SeleneDB v1.1 is not the right fit — look at PostGIS or a dedicated GIS engine.

## Geometry types

SeleneDB supports every 2D geometry kind defined in GeoJSON:

- `Point`
- `LineString` / `MultiLineString`
- `Polygon` / `MultiPolygon` (with holes via interior rings)
- `MultiPoint`
- `GeometryCollection`

Geometries carry an optional CRS hint (`EPSG:4326` for geographic, `None` for planar). This steers distance functions — haversine for both operands in EPSG:4326, euclidean otherwise.

## Ingest

Four ways to put a geometry into a property:

### As a literal via `ST_Point`

```gql
MATCH (b:building) WHERE b.name = 'HQ'
SET b.location = ST_Point(-74.0060, 40.7128)
```

`ST_Point(lng, lat)` produces a Point in EPSG:4326. Lng-then-lat follows GeoJSON convention (not the opposite lat-lng order used by some map libraries).

### From GeoJSON text

```gql
MATCH (z:zone) WHERE z.name = 'Manhattan'
SET z.boundary = ST_GeomFromGeoJSON('{
  "type": "Polygon",
  "coordinates": [[
    [-74.02, 40.70], [-73.97, 40.70],
    [-73.97, 40.80], [-74.02, 40.80],
    [-74.02, 40.70]
  ]]
}')
```

Accepts a geometry, feature, or feature-collection object. Features and feature-collections unwrap to the first geometry — properties and foreign members are discarded.

### From a list of points

```gql
INSERT (z:zone {name: 'Campus'})
SET z.boundary = ST_MakePolygon([
  ST_Point(-74.02, 40.70),
  ST_Point(-73.97, 40.70),
  ST_Point(-73.97, 40.80),
  ST_Point(-74.02, 40.80)
])
```

The ring is auto-closed if you omit the repeated first point. All input points must share the same CRS; the resulting polygon inherits it.

### From a programmatic client

When building a property map directly in Rust:

```rust
use selene_core::{GeometryValue, PropertyMap, Value};

let props = PropertyMap::from_pairs(vec![
    ("name".into(), Value::str("HQ")),
    (
        "location".into(),
        Value::geometry(GeometryValue::point_wgs84(-74.0060, 40.7128)),
    ),
]);
```

## Querying

### Points inside a polygon

```gql
MATCH (z:zone), (s:sensor)
WHERE ST_Contains(z.boundary, s.location)
RETURN z.name, s.name
```

Note the `WHERE` — see the [FILTER vs WHERE](#filter-vs-where) section below.

### Ordered by distance

```gql
MATCH (b:building {name: 'HQ'}), (s:sensor)
RETURN s.name, ST_Distance(b.location, s.location) AS dist
ORDER BY dist ASC
LIMIT 10
```

`ST_Distance` returns meters when both operands are WGS84 Points (haversine). Otherwise it returns the minimum planar euclidean distance across all vertex pairs — adequate for "which sensor is roughly closest?" but not for true nearest-point-on-segment queries (deferred to a later release).

### Within a radius

```gql
MATCH (b:building {name: 'HQ'}), (s:sensor)
WHERE ST_DWithin(b.location, s.location, 500.0)
RETURN s.name
```

Same dispatch as `ST_Distance` — meters for WGS84 Points, euclidean units otherwise. `ST_DistanceSphere` is available when you want explicit haversine regardless of CRS hint.

### Spatial join on overlap

```gql
MATCH (a:zone), (b:zone)
WHERE id(a) < id(b) AND ST_Intersects(a.boundary, b.boundary)
RETURN a.name, b.name
```

## Function reference

### Constructors

| Function | Returns | Notes |
|----------|---------|-------|
| `ST_Point(lng, lat)` | `GEOMETRY` | Point in EPSG:4326 |
| `ST_GeomFromGeoJSON(text)` | `GEOMETRY` | Accepts geometry, feature, or feature-collection |
| `ST_MakePolygon(list)` | `GEOMETRY` | List of points; ring auto-closes; shared CRS propagates |

### Accessors

| Function | Returns | Notes |
|----------|---------|-------|
| `ST_X(point)` | `FLOAT` | Longitude for geographic CRS |
| `ST_Y(point)` | `FLOAT` | Latitude for geographic CRS |
| `ST_GeometryType(geom)` | `STRING` | `"Point"`, `"Polygon"`, ... |
| `ST_IsValid(geom)` | `BOOL` | Checks finite coords and closed polygon rings |
| `ST_AsGeoJSON(geom)` | `STRING` | Compact single-line GeoJSON |

### Predicates (return `BOOL`)

| Function | Semantic |
|----------|----------|
| `ST_Contains(a, b)` | Every vertex of `b` is inside a polygon of `a`, and no segment of `b` crosses a's boundary |
| `ST_Within(a, b)` | Inverse of `ST_Contains` |
| `ST_Intersects(a, b)` | Any shared point (bbox pre-filter, then segment test) |
| `ST_Equals(a, b)` | Structural equality including CRS |
| `ST_DWithin(a, b, d)` | `ST_Distance(a, b) <= d` |

### Measurements (return `FLOAT`)

| Function | Semantic |
|----------|----------|
| `ST_Distance(a, b)` | Haversine (meters) for two WGS84 Points; else min vertex-pair euclidean |
| `ST_DistanceSphere(a, b)` | Explicit haversine; requires both to be Points |
| `ST_Area(polygon)` | Planar shoelace area; 0.0 for non-areal |
| `ST_Length(linestring)` | Planar segment-sum; 0.0 for non-linear |

### Envelope

| Function | Returns | Notes |
|----------|---------|-------|
| `ST_Envelope(geom)` | `GEOMETRY` | Axis-aligned bounding box as a Polygon; preserves CRS |

## Coordinate reference systems

SeleneDB v1.1 tracks a CRS hint on every geometry but does no reprojection:

- `ST_Point(lng, lat)` tags the point as `EPSG:4326`.
- `ST_GeomFromGeoJSON(...)` leaves CRS unset by default (GeoJSON's spec pins its own coordinates to WGS84, but we don't auto-tag unless you explicitly construct via `ST_Point`).
- Planar geometries (unset CRS) use euclidean math; two WGS84 Points use haversine.
- Mixing CRS in a single function call is allowed but falls back to planar distance. If you care about this, normalize on ingest.

CRS reprojection (e.g., EPSG:3857 Web Mercator → EPSG:4326 lat/lng) needs `proj4`, which introduces C dependencies. Deferred indefinitely — upstream the projection in your pipeline before ingest.

## FILTER vs WHERE

Selene's GQL scopes `FILTER` **per-pattern** during the MATCH phase; `WHERE` evaluates **after** binding assembly. This matters for spatial predicates:

```gql
-- Single binding → FILTER works
MATCH (s:sensor)
FILTER ST_Y(s.location) > 40.80
RETURN s.name

-- Cross-binding → use WHERE
MATCH (b:building), (s:sensor)
WHERE ST_DWithin(b.location, s.location, 500.0)
RETURN s.name
```

Using `FILTER` with a cross-binding predicate silently returns zero rows. The predicate is scoped to one pattern element at a time, so `ST_DWithin(b.location, s.location, ...)` evaluates against an incompletely-bound tuple.

Rule of thumb: if your predicate references variables from more than one pattern element, use `WHERE`.

## Performance

No spatial index in v1.1. Predicates and measurements scan linearly. Tested workloads:

- Point-in-polygon: sub-millisecond for ~1K polygons against ~10K points on M-series Apple Silicon
- Distance sort: ~5 ms over 10K candidates
- `ST_Intersects` with bbox pre-filter: ~100 µs per pair

If your workload pushes past ~100K nodes or you need sub-ms spatial kNN, an R-tree or H3 hex-grid index is a natural v1.2 addition. Open an issue with your shape of data and we can prioritize.

## Example: zone-based sensor monitoring

A common IoT pattern — tag zones as polygons, tag sensors as points, query sensors per zone with telemetry joined in:

```gql
-- Ingest
INSERT (z:zone {name: 'Lobby'})
SET z.boundary = ST_MakePolygon([
  ST_Point(-74.006, 40.713), ST_Point(-74.005, 40.713),
  ST_Point(-74.005, 40.714), ST_Point(-74.006, 40.714)
])

INSERT (s:sensor {name: 'T-101', kind: 'temperature'})
SET s.location = ST_Point(-74.0055, 40.7135)

-- Query current reading per sensor inside each zone
MATCH (z:zone), (s:sensor)
WHERE ST_Contains(z.boundary, s.location)
RETURN z.name AS zone,
       s.name AS sensor,
       ts.latest(id(s), 'temp') AS current_temp
ORDER BY zone, sensor
```

## What's not in v1.1

| Feature | Status | Notes |
|---------|--------|-------|
| Spatial index (R-tree, H3) | Deferred | O(n) scan in v1.1; add when needed |
| Full WKT parser | Partial | v1.1 emits WKT on export and parses the `POINT (x y)` shape on reimport (enough for round-tripping points via RDF `geo:wktLiteral`). Non-Point WKT input falls back to string. Use GeoJSON for non-point input; the full parser is ~100 LOC when the demand shows up. |
| CRS reprojection | Not planned | Requires `proj4` (C deps); upstream in your pipeline |
| 3D geometries (M/Z) | Not planned | 2D only — matches GeoJSON core spec |
| Raster overlays | Not planned | Out of scope for the graph runtime |
| Network/route computation along edges | Not planned | Use graph algorithms on edge weights |

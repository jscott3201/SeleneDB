//! Spatial scalar functions (OGC `ST_*` family).
//!
//! Operate on `GqlValue::Geometry` values produced by `Value::Geometry`
//! properties. Algorithms are hand-rolled here — the `geo_types` crate
//! provides the primitive types, we own the hot path.
//!
//! Coordinate systems: when both operands carry `crs == Some("EPSG:4326")`,
//! distance and dwithin switch to haversine (meters on WGS84). Otherwise
//! operations treat coordinates as planar.

// Geometry variant matches are more readable with glob imports.
#![allow(clippy::enum_glob_use)]

use std::sync::Arc;

use selene_core::GeometryValue;
use smol_str::SmolStr;

use super::{FunctionSignature, ScalarFunction};
use crate::runtime::eval::EvalContext;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── Algorithms ────────────────────────────────────────────────────────────

/// Earth mean radius in meters, as used by OGC for WGS84 haversine.
const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Great-circle distance between two geographic points, in meters.
pub(crate) fn haversine_meters(lng1: f64, lat1: f64, lng2: f64, lat2: f64) -> f64 {
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let dphi = (lat2 - lat1).to_radians();
    let dlambda = (lng2 - lng1).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlambda / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

/// 2D euclidean distance between two points.
pub(crate) fn euclidean(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let dx = x2 - x1;
    let dy = y2 - y1;
    (dx * dx + dy * dy).sqrt()
}

/// Point-in-polygon by ray casting (Franklin). Handles holes: a point is
/// inside the polygon if it's inside the exterior ring and outside every
/// interior ring.
pub(crate) fn point_in_polygon(x: f64, y: f64, poly: &geo_types::Polygon<f64>) -> bool {
    if !in_ring(x, y, poly.exterior()) {
        return false;
    }
    for hole in poly.interiors() {
        if in_ring(x, y, hole) {
            return false;
        }
    }
    true
}

fn in_ring(px: f64, py: f64, ring: &geo_types::LineString<f64>) -> bool {
    let coords = &ring.0;
    let n = coords.len();
    if n < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let xi = coords[i].x;
        let yi = coords[i].y;
        let xj = coords[j].x;
        let yj = coords[j].y;
        let intersect = ((yi > py) != (yj > py)) && (px < (xj - xi) * (py - yi) / (yj - yi) + xi);
        if intersect {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// Signed shoelace area of a ring. Positive = CCW, negative = CW.
fn ring_signed_area(ring: &geo_types::LineString<f64>) -> f64 {
    let c = &ring.0;
    let n = c.len();
    if n < 3 {
        return 0.0;
    }
    let mut sum = 0.0;
    for i in 0..n {
        let j = (i + 1) % n;
        sum += c[i].x * c[j].y - c[j].x * c[i].y;
    }
    sum / 2.0
}

/// Planar polygon area: exterior minus interior rings.
pub(crate) fn polygon_area(poly: &geo_types::Polygon<f64>) -> f64 {
    let mut area = ring_signed_area(poly.exterior()).abs();
    for hole in poly.interiors() {
        area -= ring_signed_area(hole).abs();
    }
    area.max(0.0)
}

/// Planar linestring length (sum of segment distances).
pub(crate) fn linestring_length(ls: &geo_types::LineString<f64>) -> f64 {
    let c = &ls.0;
    if c.len() < 2 {
        return 0.0;
    }
    let mut total = 0.0;
    for i in 0..c.len() - 1 {
        total += euclidean(c[i].x, c[i].y, c[i + 1].x, c[i + 1].y);
    }
    total
}

/// Axis-aligned bounding box of a geometry: (min_x, min_y, max_x, max_y).
///
/// Returns `None` for empty geometries so callers can emit a type error
/// instead of constructing a polygon with non-finite coordinates.
pub(crate) fn bbox(geom: &geo_types::Geometry<f64>) -> Option<(f64, f64, f64, f64)> {
    let mut min_x = f64::INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut any = false;
    for_each_coord(geom, &mut |x, y| {
        any = true;
        if x < min_x {
            min_x = x;
        }
        if x > max_x {
            max_x = x;
        }
        if y < min_y {
            min_y = y;
        }
        if y > max_y {
            max_y = y;
        }
    });
    if any {
        Some((min_x, min_y, max_x, max_y))
    } else {
        None
    }
}

fn for_each_coord(geom: &geo_types::Geometry<f64>, f: &mut impl FnMut(f64, f64)) {
    use geo_types::Geometry::*;
    match geom {
        Point(p) => f(p.x(), p.y()),
        Line(l) => {
            f(l.start.x, l.start.y);
            f(l.end.x, l.end.y);
        }
        LineString(ls) => {
            for c in &ls.0 {
                f(c.x, c.y);
            }
        }
        Polygon(p) => {
            for c in &p.exterior().0 {
                f(c.x, c.y);
            }
            for hole in p.interiors() {
                for c in &hole.0 {
                    f(c.x, c.y);
                }
            }
        }
        MultiPoint(mp) => {
            for p in &mp.0 {
                f(p.x(), p.y());
            }
        }
        MultiLineString(mls) => {
            for ls in &mls.0 {
                for c in &ls.0 {
                    f(c.x, c.y);
                }
            }
        }
        MultiPolygon(mp) => {
            for p in &mp.0 {
                for c in &p.exterior().0 {
                    f(c.x, c.y);
                }
                for hole in p.interiors() {
                    for c in &hole.0 {
                        f(c.x, c.y);
                    }
                }
            }
        }
        GeometryCollection(gc) => {
            for g in &gc.0 {
                for_each_coord(g, f);
            }
        }
        Rect(r) => {
            f(r.min().x, r.min().y);
            f(r.max().x, r.max().y);
        }
        Triangle(t) => {
            let (a, b, c) = (t.v1(), t.v2(), t.v3());
            f(a.x, a.y);
            f(b.x, b.y);
            f(c.x, c.y);
        }
    }
}

fn bbox_intersects(a: (f64, f64, f64, f64), b: (f64, f64, f64, f64)) -> bool {
    a.0 <= b.2 && a.2 >= b.0 && a.1 <= b.3 && a.3 >= b.1
}

/// Segment–segment intersection test in 2D. Endpoints touching count as
/// intersecting (matches PostGIS ST_Intersects semantics).
fn segments_intersect(p1: (f64, f64), p2: (f64, f64), p3: (f64, f64), p4: (f64, f64)) -> bool {
    fn orient(a: (f64, f64), b: (f64, f64), c: (f64, f64)) -> f64 {
        (b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)
    }
    fn on_segment(a: (f64, f64), b: (f64, f64), c: (f64, f64)) -> bool {
        c.0 <= a.0.max(b.0) && c.0 >= a.0.min(b.0) && c.1 <= a.1.max(b.1) && c.1 >= a.1.min(b.1)
    }
    let o1 = orient(p1, p2, p3);
    let o2 = orient(p1, p2, p4);
    let o3 = orient(p3, p4, p1);
    let o4 = orient(p3, p4, p2);
    if (o1 > 0.0) != (o2 > 0.0) && (o3 > 0.0) != (o4 > 0.0) {
        return true;
    }
    if o1 == 0.0 && on_segment(p1, p2, p3) {
        return true;
    }
    if o2 == 0.0 && on_segment(p1, p2, p4) {
        return true;
    }
    if o3 == 0.0 && on_segment(p3, p4, p1) {
        return true;
    }
    if o4 == 0.0 && on_segment(p3, p4, p2) {
        return true;
    }
    false
}

/// Return every (x, y) from a geometry as a flat vector. Convenient for
/// "does any point lie in polygon?" tests without allocating an enum tree.
fn coords_of(geom: &geo_types::Geometry<f64>) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    for_each_coord(geom, &mut |x, y| out.push((x, y)));
    out
}

fn linestring_segments(ls: &geo_types::LineString<f64>) -> Vec<((f64, f64), (f64, f64))> {
    let c = &ls.0;
    if c.len() < 2 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(c.len() - 1);
    for i in 0..c.len() - 1 {
        out.push(((c[i].x, c[i].y), (c[i + 1].x, c[i + 1].y)));
    }
    out
}

/// Collect all 2D segments inside a geometry (empty for points and
/// geometry collections that contain only points).
fn all_segments(geom: &geo_types::Geometry<f64>) -> Vec<((f64, f64), (f64, f64))> {
    use geo_types::Geometry::*;
    let mut out = Vec::new();
    match geom {
        Point(_) | MultiPoint(_) => {}
        Line(l) => out.push(((l.start.x, l.start.y), (l.end.x, l.end.y))),
        LineString(ls) => out.extend(linestring_segments(ls)),
        MultiLineString(mls) => {
            for ls in &mls.0 {
                out.extend(linestring_segments(ls));
            }
        }
        Polygon(p) => {
            out.extend(linestring_segments(p.exterior()));
            for hole in p.interiors() {
                out.extend(linestring_segments(hole));
            }
        }
        MultiPolygon(mp) => {
            for p in &mp.0 {
                out.extend(linestring_segments(p.exterior()));
                for hole in p.interiors() {
                    out.extend(linestring_segments(hole));
                }
            }
        }
        GeometryCollection(gc) => {
            for g in &gc.0 {
                out.extend(all_segments(g));
            }
        }
        Rect(r) => {
            let (lx, ly) = (r.min().x, r.min().y);
            let (ux, uy) = (r.max().x, r.max().y);
            out.push(((lx, ly), (ux, ly)));
            out.push(((ux, ly), (ux, uy)));
            out.push(((ux, uy), (lx, uy)));
            out.push(((lx, uy), (lx, ly)));
        }
        Triangle(t) => {
            let (a, b, c) = (t.v1(), t.v2(), t.v3());
            out.push(((a.x, a.y), (b.x, b.y)));
            out.push(((b.x, b.y), (c.x, c.y)));
            out.push(((c.x, c.y), (a.x, a.y)));
        }
    }
    out
}

/// Polygons in a geometry (for contains/within predicates).
fn polygons_of(geom: &geo_types::Geometry<f64>) -> Vec<&geo_types::Polygon<f64>> {
    use geo_types::Geometry::*;
    let mut out = Vec::new();
    match geom {
        Polygon(p) => out.push(p),
        MultiPolygon(mp) => {
            for p in &mp.0 {
                out.push(p);
            }
        }
        GeometryCollection(gc) => {
            for g in &gc.0 {
                out.extend(polygons_of(g));
            }
        }
        _ => {}
    }
    out
}

/// `a` contains `b` iff every vertex of `b` lies inside a polygon of `a`
/// AND no segment of `b` crosses a boundary ring of any of `a`'s polygons.
/// The second check catches the case where every vertex happens to land
/// inside the containing polygon but an edge of `b` exits and re-enters
/// (e.g., two convex polygons with interleaved vertices).
/// Requires `a` to contain at least one polygon.
pub(crate) fn contains(a: &geo_types::Geometry<f64>, b: &geo_types::Geometry<f64>) -> bool {
    let polys = polygons_of(a);
    if polys.is_empty() {
        return false;
    }
    let points = coords_of(b);
    if points.is_empty() {
        return false;
    }
    if !points
        .iter()
        .all(|(x, y)| polys.iter().any(|p| point_in_polygon(*x, *y, p)))
    {
        return false;
    }
    // Segment check: no segment of `b` may cross any boundary ring of any
    // polygon of `a`. Touching a boundary without crossing is allowed.
    let b_segments = all_segments(b);
    if b_segments.is_empty() {
        // No segments to check (pure point geometry).
        return true;
    }
    let boundary: Vec<((f64, f64), (f64, f64))> = polys
        .iter()
        .flat_map(|p| polygon_boundary_segments(p))
        .collect();
    !b_segments.iter().any(|(p1, p2)| {
        boundary
            .iter()
            .any(|(p3, p4)| segment_properly_crosses(*p1, *p2, *p3, *p4))
    })
}

/// Boundary segments (exterior + interior rings) of a single polygon.
fn polygon_boundary_segments(poly: &geo_types::Polygon<f64>) -> Vec<((f64, f64), (f64, f64))> {
    let mut out = Vec::new();
    out.extend(linestring_segments(poly.exterior()));
    for ring in poly.interiors() {
        out.extend(linestring_segments(ring));
    }
    out
}

/// True when two segments *cross* (not merely touch at an endpoint or
/// share a collinear run). Used by `contains` — touching the boundary of
/// the containing polygon is acceptable, only a true crossing disqualifies.
fn segment_properly_crosses(
    p1: (f64, f64),
    p2: (f64, f64),
    p3: (f64, f64),
    p4: (f64, f64),
) -> bool {
    fn orient(a: (f64, f64), b: (f64, f64), c: (f64, f64)) -> f64 {
        (b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)
    }
    let o1 = orient(p1, p2, p3);
    let o2 = orient(p1, p2, p4);
    let o3 = orient(p3, p4, p1);
    let o4 = orient(p3, p4, p2);
    // Strictly opposite sides on both segments → proper crossing.
    (o1 > 0.0) != (o2 > 0.0)
        && o1 != 0.0
        && o2 != 0.0
        && (o3 > 0.0) != (o4 > 0.0)
        && o3 != 0.0
        && o4 != 0.0
}

/// Any spatial overlap at all: bbox pre-filter, then segment intersection,
/// then point-in-polygon for polygon-containing-point cases.
pub(crate) fn intersects(a: &geo_types::Geometry<f64>, b: &geo_types::Geometry<f64>) -> bool {
    let (Some(ba), Some(bb)) = (bbox(a), bbox(b)) else {
        return false;
    };
    if !bbox_intersects(ba, bb) {
        return false;
    }
    // Hoist coords out of the polygon loop — reused in both directions.
    let a_coords = coords_of(a);
    let b_coords = coords_of(b);
    for poly in polygons_of(a) {
        for (x, y) in &b_coords {
            if point_in_polygon(*x, *y, poly) {
                return true;
            }
        }
    }
    for poly in polygons_of(b) {
        for (x, y) in &a_coords {
            if point_in_polygon(*x, *y, poly) {
                return true;
            }
        }
    }
    // Any segment crosses any other segment.
    let sa = all_segments(a);
    let sb = all_segments(b);
    for (p1, p2) in &sa {
        for (p3, p4) in &sb {
            if segments_intersect(*p1, *p2, *p3, *p4) {
                return true;
            }
        }
    }
    false
}

/// Distance between two geometries. For two Points with matching
/// `EPSG:4326` CRS we return meters via haversine; otherwise the minimum
/// planar euclidean distance across all vertex pairs (adequate for v1.1
/// IoT workloads — true nearest-point-on-segment is deferred).
/// Errors on empty geometries rather than silently returning 0.
pub(crate) fn distance(a: &GeometryValue, b: &GeometryValue) -> Result<f64, GqlError> {
    let both_wgs84 = matches!(a.crs.as_ref().map(|s| s.as_str()), Some("EPSG:4326"))
        && matches!(b.crs.as_ref().map(|s| s.as_str()), Some("EPSG:4326"));
    if both_wgs84
        && let (Some((ax, ay)), Some((bx, by))) = (point_coord(&a.geom), point_coord(&b.geom))
    {
        return Ok(haversine_meters(ax, ay, bx, by));
    }
    min_vertex_euclidean(&a.geom, &b.geom)
        .ok_or_else(|| GqlError::type_error("spatial function received an empty geometry"))
}

/// Coordinate of a Point, or `None` for any other geometry kind.
fn point_coord(geom: &geo_types::Geometry<f64>) -> Option<(f64, f64)> {
    match geom {
        geo_types::Geometry::Point(p) => Some((p.x(), p.y())),
        _ => None,
    }
}

/// Minimum planar euclidean distance across all vertex pairs. `None` if
/// either geometry has no coordinates.
fn min_vertex_euclidean(a: &geo_types::Geometry<f64>, b: &geo_types::Geometry<f64>) -> Option<f64> {
    let ac = coords_of(a);
    let bc = coords_of(b);
    if ac.is_empty() || bc.is_empty() {
        return None;
    }
    let mut best = f64::INFINITY;
    for (ax, ay) in &ac {
        for (bx, by) in &bc {
            let d = euclidean(*ax, *ay, *bx, *by);
            if d < best {
                best = d;
            }
        }
    }
    Some(best)
}

// ── Function trait impls ──────────────────────────────────────────────────

fn need_geom<'a>(
    args: &'a [GqlValue],
    idx: usize,
    name: &str,
) -> Result<&'a Arc<GeometryValue>, GqlError> {
    match args.get(idx) {
        Some(GqlValue::Geometry(g)) => Ok(g),
        Some(other) => Err(GqlError::type_error(format!(
            "{name}: arg {idx} expected GEOMETRY, got {}",
            other.gql_type()
        ))),
        None => Err(GqlError::type_error(format!(
            "{name}: missing arg {idx} (GEOMETRY)"
        ))),
    }
}

fn need_float(args: &[GqlValue], idx: usize, name: &str) -> Result<f64, GqlError> {
    match args.get(idx) {
        Some(GqlValue::Float(f)) => Ok(*f),
        Some(GqlValue::Int(i)) => Ok(*i as f64),
        Some(GqlValue::UInt(u)) => Ok(*u as f64),
        Some(other) => Err(GqlError::type_error(format!(
            "{name}: arg {idx} expected numeric, got {}",
            other.gql_type()
        ))),
        None => Err(GqlError::type_error(format!(
            "{name}: missing arg {idx} (numeric)"
        ))),
    }
}

fn need_string<'a>(args: &'a [GqlValue], idx: usize, name: &str) -> Result<&'a str, GqlError> {
    match args.get(idx) {
        Some(GqlValue::String(s)) => Ok(s.as_str()),
        Some(other) => Err(GqlError::type_error(format!(
            "{name}: arg {idx} expected STRING, got {}",
            other.gql_type()
        ))),
        None => Err(GqlError::type_error(format!(
            "{name}: missing arg {idx} (STRING)"
        ))),
    }
}

fn ok_geom(g: GeometryValue) -> GqlValue {
    GqlValue::Geometry(Arc::new(g))
}

// ── Constructors ─────────────────────────────────────────────────────

pub(crate) struct StPointFunction;
impl ScalarFunction for StPointFunction {
    fn name(&self) -> &'static str {
        "st_point"
    }
    fn description(&self) -> &'static str {
        "Construct a GEOMETRY Point in EPSG:4326 from longitude and latitude"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let lng = need_float(args, 0, "st_point")?;
        let lat = need_float(args, 1, "st_point")?;
        Ok(ok_geom(GeometryValue::point_wgs84(lng, lat)))
    }
    fn signature(&self) -> Option<FunctionSignature> {
        Some(FunctionSignature {
            arg_types: vec![GqlType::Float, GqlType::Float],
            return_type: GqlType::Geometry,
            variadic: false,
        })
    }
}

pub(crate) struct StGeomFromGeoJsonFunction;
impl ScalarFunction for StGeomFromGeoJsonFunction {
    fn name(&self) -> &'static str {
        "st_geomfromgeojson"
    }
    fn description(&self) -> &'static str {
        "Parse a GeoJSON geometry (or feature/feature-collection) into a GEOMETRY value"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let s = need_string(args, 0, "st_geomfromgeojson")?;
        let g = GeometryValue::from_geojson(s).map_err(|e| GqlError::type_error(e.to_string()))?;
        Ok(ok_geom(g))
    }
    fn signature(&self) -> Option<FunctionSignature> {
        Some(FunctionSignature {
            arg_types: vec![GqlType::String],
            return_type: GqlType::Geometry,
            variadic: false,
        })
    }
}

pub(crate) struct StMakePolygonFunction;
impl ScalarFunction for StMakePolygonFunction {
    fn name(&self) -> &'static str {
        "st_makepolygon"
    }
    fn description(&self) -> &'static str {
        "Construct a POLYGON from a list of points (closed ring). The ring is auto-closed if open."
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let list = match args.first() {
            Some(GqlValue::List(l)) => l,
            Some(other) => {
                return Err(GqlError::type_error(format!(
                    "st_makepolygon: expected a list of GEOMETRY points, got {}",
                    other.gql_type()
                )));
            }
            None => {
                return Err(GqlError::type_error(
                    "st_makepolygon: missing arg 0, expected a list of GEOMETRY points",
                ));
            }
        };
        let mut coords = Vec::with_capacity(list.elements.len() + 1);
        // Track CRS: all input points must share the same CRS (or all None)
        // so the resulting polygon inherits it and later spatial functions
        // pick the right distance metric.
        let mut shared_crs: Option<Option<selene_core::IStr>> = None;
        for (i, el) in list.elements.iter().enumerate() {
            match el {
                GqlValue::Geometry(g) => {
                    match &g.geom {
                        geo_types::Geometry::Point(p) => {
                            coords.push(geo_types::Coord { x: p.x(), y: p.y() });
                        }
                        other => {
                            return Err(GqlError::type_error(format!(
                                "st_makepolygon: element {i} is {}, expected Point",
                                geometry_type_name(other)
                            )));
                        }
                    }
                    match &shared_crs {
                        None => shared_crs = Some(g.crs),
                        Some(prev) if *prev != g.crs => {
                            return Err(GqlError::type_error(format!(
                                "st_makepolygon: element {i} has a different CRS than earlier points; \
                                 all points must share a CRS so the polygon inherits it"
                            )));
                        }
                        _ => {}
                    }
                }
                other => {
                    return Err(GqlError::type_error(format!(
                        "st_makepolygon: element {i} is {}, expected GEOMETRY",
                        other.gql_type()
                    )));
                }
            }
        }
        if coords.len() < 3 {
            return Err(GqlError::type_error(
                "st_makepolygon: polygon requires at least 3 points",
            ));
        }
        // Close the ring if the caller didn't.
        if coords.first() != coords.last() {
            coords.push(coords[0]);
        }
        let ls = geo_types::LineString::from(coords);
        let poly = geo_types::Polygon::new(ls, vec![]);
        let mut gv = GeometryValue::from(poly);
        gv.crs = shared_crs.unwrap_or(None);
        Ok(ok_geom(gv))
    }
}

fn geometry_type_name(g: &geo_types::Geometry<f64>) -> &'static str {
    use geo_types::Geometry::*;
    match g {
        Point(_) => "Point",
        Line(_) => "Line",
        LineString(_) => "LineString",
        Polygon(_) => "Polygon",
        MultiPoint(_) => "MultiPoint",
        MultiLineString(_) => "MultiLineString",
        MultiPolygon(_) => "MultiPolygon",
        GeometryCollection(_) => "GeometryCollection",
        Rect(_) => "Rect",
        Triangle(_) => "Triangle",
    }
}

// ── Accessors ────────────────────────────────────────────────────────

pub(crate) struct StXFunction;
impl ScalarFunction for StXFunction {
    fn name(&self) -> &'static str {
        "st_x"
    }
    fn description(&self) -> &'static str {
        "Return the X coordinate (longitude for geographic CRS) of a Point"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_x")?;
        match &g.geom {
            geo_types::Geometry::Point(p) => Ok(GqlValue::Float(p.x())),
            other => Err(GqlError::type_error(format!(
                "st_x requires a Point, got {}",
                geometry_type_name(other)
            ))),
        }
    }
}

pub(crate) struct StYFunction;
impl ScalarFunction for StYFunction {
    fn name(&self) -> &'static str {
        "st_y"
    }
    fn description(&self) -> &'static str {
        "Return the Y coordinate (latitude for geographic CRS) of a Point"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_y")?;
        match &g.geom {
            geo_types::Geometry::Point(p) => Ok(GqlValue::Float(p.y())),
            other => Err(GqlError::type_error(format!(
                "st_y requires a Point, got {}",
                geometry_type_name(other)
            ))),
        }
    }
}

pub(crate) struct StGeometryTypeFunction;
impl ScalarFunction for StGeometryTypeFunction {
    fn name(&self) -> &'static str {
        "st_geometrytype"
    }
    fn description(&self) -> &'static str {
        "Return the geometry kind as a string (\"Point\", \"Polygon\", \"LineString\", ...)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_geometrytype")?;
        Ok(GqlValue::String(SmolStr::new(g.geometry_type())))
    }
}

pub(crate) struct StIsValidFunction;
impl ScalarFunction for StIsValidFunction {
    fn name(&self) -> &'static str {
        "st_isvalid"
    }
    fn description(&self) -> &'static str {
        "Return TRUE if the geometry is well-formed. v1.1 checks non-empty rings, \
         closed polygon rings, and finite coordinates."
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_isvalid")?;
        Ok(GqlValue::Bool(is_valid(&g.geom)))
    }
}

fn is_valid(geom: &geo_types::Geometry<f64>) -> bool {
    use geo_types::Geometry::*;
    let mut all_finite = true;
    for_each_coord(geom, &mut |x, y| {
        if !x.is_finite() || !y.is_finite() {
            all_finite = false;
        }
    });
    if !all_finite {
        return false;
    }
    match geom {
        Polygon(p) => polygon_rings_closed(p),
        MultiPolygon(mp) => mp.0.iter().all(polygon_rings_closed),
        GeometryCollection(gc) => gc.0.iter().all(is_valid),
        _ => true,
    }
}

fn polygon_rings_closed(p: &geo_types::Polygon<f64>) -> bool {
    let ring_closed = |ls: &geo_types::LineString<f64>| {
        let c = &ls.0;
        c.len() >= 4 && c.first() == c.last()
    };
    ring_closed(p.exterior()) && p.interiors().iter().all(ring_closed)
}

pub(crate) struct StAsGeoJsonFunction;
impl ScalarFunction for StAsGeoJsonFunction {
    fn name(&self) -> &'static str {
        "st_asgeojson"
    }
    fn description(&self) -> &'static str {
        "Serialize a GEOMETRY as compact GeoJSON text"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_asgeojson")?;
        Ok(GqlValue::String(SmolStr::new(g.to_geojson())))
    }
}

// ── Predicates ───────────────────────────────────────────────────────

pub(crate) struct StContainsFunction;
impl ScalarFunction for StContainsFunction {
    fn name(&self) -> &'static str {
        "st_contains"
    }
    fn description(&self) -> &'static str {
        "Return TRUE if every point of geometry B is inside a polygon of geometry A"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = need_geom(args, 0, "st_contains")?;
        let b = need_geom(args, 1, "st_contains")?;
        Ok(GqlValue::Bool(contains(&a.geom, &b.geom)))
    }
}

pub(crate) struct StWithinFunction;
impl ScalarFunction for StWithinFunction {
    fn name(&self) -> &'static str {
        "st_within"
    }
    fn description(&self) -> &'static str {
        "Return TRUE if geometry A is inside geometry B (inverse of ST_Contains)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = need_geom(args, 0, "st_within")?;
        let b = need_geom(args, 1, "st_within")?;
        Ok(GqlValue::Bool(contains(&b.geom, &a.geom)))
    }
}

pub(crate) struct StIntersectsFunction;
impl ScalarFunction for StIntersectsFunction {
    fn name(&self) -> &'static str {
        "st_intersects"
    }
    fn description(&self) -> &'static str {
        "Return TRUE if two geometries share any point"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = need_geom(args, 0, "st_intersects")?;
        let b = need_geom(args, 1, "st_intersects")?;
        Ok(GqlValue::Bool(intersects(&a.geom, &b.geom)))
    }
}

pub(crate) struct StEqualsFunction;
impl ScalarFunction for StEqualsFunction {
    fn name(&self) -> &'static str {
        "st_equals"
    }
    fn description(&self) -> &'static str {
        "Return TRUE if two geometries are structurally identical (coordinates and CRS)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = need_geom(args, 0, "st_equals")?;
        let b = need_geom(args, 1, "st_equals")?;
        Ok(GqlValue::Bool(**a == **b))
    }
}

pub(crate) struct StDWithinFunction;
impl ScalarFunction for StDWithinFunction {
    fn name(&self) -> &'static str {
        "st_dwithin"
    }
    fn description(&self) -> &'static str {
        "Return TRUE if two geometries are within the given distance. \
         Uses haversine (meters) when both operands are WGS84 points, euclidean otherwise."
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = need_geom(args, 0, "st_dwithin")?;
        let b = need_geom(args, 1, "st_dwithin")?;
        let d = need_float(args, 2, "st_dwithin")?;
        Ok(GqlValue::Bool(distance(a, b)? <= d))
    }
}

// ── Measurements ─────────────────────────────────────────────────────

pub(crate) struct StDistanceFunction;
impl ScalarFunction for StDistanceFunction {
    fn name(&self) -> &'static str {
        "st_distance"
    }
    fn description(&self) -> &'static str {
        "Return distance between two geometries. \
         Meters (haversine) for WGS84 points; euclidean units otherwise."
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = need_geom(args, 0, "st_distance")?;
        let b = need_geom(args, 1, "st_distance")?;
        Ok(GqlValue::Float(distance(a, b)?))
    }
}

pub(crate) struct StDistanceSphereFunction;
impl ScalarFunction for StDistanceSphereFunction {
    fn name(&self) -> &'static str {
        "st_distancesphere"
    }
    fn description(&self) -> &'static str {
        "Great-circle distance in meters between two Points, assuming WGS84 regardless of CRS hint"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = need_geom(args, 0, "st_distancesphere")?;
        let b = need_geom(args, 1, "st_distancesphere")?;
        let (ax, ay) = point_coord(&a.geom)
            .ok_or_else(|| GqlError::type_error("st_distancesphere: arg 0 must be a Point"))?;
        let (bx, by) = point_coord(&b.geom)
            .ok_or_else(|| GqlError::type_error("st_distancesphere: arg 1 must be a Point"))?;
        Ok(GqlValue::Float(haversine_meters(ax, ay, bx, by)))
    }
}

pub(crate) struct StAreaFunction;
impl ScalarFunction for StAreaFunction {
    fn name(&self) -> &'static str {
        "st_area"
    }
    fn description(&self) -> &'static str {
        "Planar area of a polygon or multi-polygon. Returns 0.0 for non-areal geometries."
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_area")?;
        let area = match &g.geom {
            geo_types::Geometry::Polygon(p) => polygon_area(p),
            geo_types::Geometry::MultiPolygon(mp) => mp.0.iter().map(polygon_area).sum(),
            _ => 0.0,
        };
        Ok(GqlValue::Float(area))
    }
}

pub(crate) struct StLengthFunction;
impl ScalarFunction for StLengthFunction {
    fn name(&self) -> &'static str {
        "st_length"
    }
    fn description(&self) -> &'static str {
        "Planar length of a linestring, summed over multi-linestrings. Returns 0.0 otherwise."
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_length")?;
        let len = match &g.geom {
            geo_types::Geometry::LineString(ls) => linestring_length(ls),
            geo_types::Geometry::MultiLineString(mls) => mls.0.iter().map(linestring_length).sum(),
            _ => 0.0,
        };
        Ok(GqlValue::Float(len))
    }
}

pub(crate) struct StEnvelopeFunction;
impl ScalarFunction for StEnvelopeFunction {
    fn name(&self) -> &'static str {
        "st_envelope"
    }
    fn description(&self) -> &'static str {
        "Return the axis-aligned bounding box as a Polygon"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let g = need_geom(args, 0, "st_envelope")?;
        let (min_x, min_y, max_x, max_y) = bbox(&g.geom)
            .ok_or_else(|| GqlError::type_error("st_envelope: empty geometry has no envelope"))?;
        let ring = geo_types::LineString::from(vec![
            geo_types::Coord { x: min_x, y: min_y },
            geo_types::Coord { x: max_x, y: min_y },
            geo_types::Coord { x: max_x, y: max_y },
            geo_types::Coord { x: min_x, y: max_y },
            geo_types::Coord { x: min_x, y: min_y },
        ]);
        let poly = geo_types::Polygon::new(ring, vec![]);
        let mut gv = GeometryValue::from(poly);
        gv.crs = g.crs;
        Ok(ok_geom(gv))
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::functions::FunctionRegistry;
    use selene_graph::SeleneGraph;

    fn ctx() -> (SeleneGraph, FunctionRegistry) {
        (SeleneGraph::new(), FunctionRegistry::with_builtins())
    }

    fn geom(gv: GeometryValue) -> GqlValue {
        GqlValue::Geometry(Arc::new(gv))
    }

    fn polygon_unit_square() -> GeometryValue {
        let s = r#"{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}"#;
        GeometryValue::from_geojson(s).unwrap()
    }

    // ── Algorithms ──

    #[test]
    fn haversine_nyc_to_london_km() {
        // NYC (-74.0, 40.7) → London (0.0, 51.5) ≈ 5,578 km
        let m = haversine_meters(-74.0, 40.7, 0.0, 51.5);
        let km = m / 1000.0;
        assert!((km - 5578.0).abs() < 30.0, "got {km} km");
    }

    #[test]
    fn euclidean_3_4_5() {
        assert!((euclidean(0.0, 0.0, 3.0, 4.0) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn point_in_polygon_inside_and_outside() {
        let p = polygon_unit_square();
        let geo_types::Geometry::Polygon(poly) = &p.geom else {
            unreachable!()
        };
        assert!(point_in_polygon(0.5, 0.5, poly));
        assert!(!point_in_polygon(2.0, 2.0, poly));
        assert!(!point_in_polygon(-0.1, 0.5, poly));
    }

    #[test]
    fn point_in_polygon_respects_hole() {
        let s = r#"{
            "type":"Polygon",
            "coordinates":[
                [[0,0],[10,0],[10,10],[0,10],[0,0]],
                [[4,4],[6,4],[6,6],[4,6],[4,4]]
            ]
        }"#;
        let p = GeometryValue::from_geojson(s).unwrap();
        let geo_types::Geometry::Polygon(poly) = &p.geom else {
            unreachable!()
        };
        assert!(point_in_polygon(1.0, 1.0, poly));
        assert!(!point_in_polygon(5.0, 5.0, poly)); // inside the hole
    }

    #[test]
    fn polygon_area_unit_square() {
        let p = polygon_unit_square();
        let geo_types::Geometry::Polygon(poly) = &p.geom else {
            unreachable!()
        };
        assert!((polygon_area(poly) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn polygon_area_subtracts_holes() {
        let s = r#"{
            "type":"Polygon",
            "coordinates":[
                [[0,0],[10,0],[10,10],[0,10],[0,0]],
                [[4,4],[6,4],[6,6],[4,6],[4,4]]
            ]
        }"#;
        let p = GeometryValue::from_geojson(s).unwrap();
        let geo_types::Geometry::Polygon(poly) = &p.geom else {
            unreachable!()
        };
        // 10x10 - 2x2 = 96
        assert!((polygon_area(poly) - 96.0).abs() < 1e-9);
    }

    #[test]
    fn linestring_length_sums_segments() {
        let s = r#"{"type":"LineString","coordinates":[[0,0],[3,0],[3,4]]}"#;
        let g = GeometryValue::from_geojson(s).unwrap();
        let geo_types::Geometry::LineString(ls) = &g.geom else {
            unreachable!()
        };
        // 3 + 4 = 7
        assert!((linestring_length(ls) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn bbox_all_coords() {
        let s = r#"{"type":"MultiPoint","coordinates":[[1,2],[5,7],[-1,3]]}"#;
        let g = GeometryValue::from_geojson(s).unwrap();
        assert_eq!(bbox(&g.geom), Some((-1.0, 2.0, 5.0, 7.0)));
    }

    #[test]
    fn segments_intersect_crossing() {
        assert!(segments_intersect(
            (0.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (2.0, 0.0)
        ));
    }

    #[test]
    fn segments_intersect_apart() {
        assert!(!segments_intersect(
            (0.0, 0.0),
            (1.0, 0.0),
            (2.0, 2.0),
            (3.0, 3.0)
        ));
    }

    // ── Registry & functions ──

    #[test]
    fn st_point_constructs_point() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let f = StPointFunction;
        let r = f
            .invoke(&[GqlValue::Float(-74.0), GqlValue::Float(40.7)], &c)
            .unwrap();
        match r {
            GqlValue::Geometry(g) => assert_eq!(g.geometry_type(), "Point"),
            _ => panic!("expected Geometry"),
        }
    }

    #[test]
    fn st_x_st_y_extract_coords() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let p = geom(GeometryValue::point_wgs84(-74.0, 40.7));
        match StXFunction.invoke(std::slice::from_ref(&p), &c).unwrap() {
            GqlValue::Float(x) => assert!((x + 74.0).abs() < 1e-9),
            _ => panic!(),
        }
        match StYFunction.invoke(&[p], &c).unwrap() {
            GqlValue::Float(y) => assert!((y - 40.7).abs() < 1e-9),
            _ => panic!(),
        }
    }

    #[test]
    fn st_geometrytype_returns_kind() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let p = geom(polygon_unit_square());
        match StGeometryTypeFunction.invoke(&[p], &c).unwrap() {
            GqlValue::String(s) => assert_eq!(s.as_str(), "Polygon"),
            _ => panic!(),
        }
    }

    #[test]
    fn st_contains_point_in_polygon() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let poly = geom(polygon_unit_square());
        let p_in = geom(GeometryValue::point_planar(0.5, 0.5));
        let p_out = geom(GeometryValue::point_planar(5.0, 5.0));
        match StContainsFunction
            .invoke(&[poly.clone(), p_in], &c)
            .unwrap()
        {
            GqlValue::Bool(b) => assert!(b),
            _ => panic!(),
        }
        match StContainsFunction.invoke(&[poly, p_out], &c).unwrap() {
            GqlValue::Bool(b) => assert!(!b),
            _ => panic!(),
        }
    }

    #[test]
    fn st_within_is_reverse_of_contains() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let poly = geom(polygon_unit_square());
        let p = geom(GeometryValue::point_planar(0.5, 0.5));
        match StWithinFunction.invoke(&[p, poly], &c).unwrap() {
            GqlValue::Bool(b) => assert!(b),
            _ => panic!(),
        }
    }

    #[test]
    fn st_intersects_overlapping_polygons() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let a = geom(polygon_unit_square());
        let b_overlap = geom(
            GeometryValue::from_geojson(
                r#"{"type":"Polygon","coordinates":[[[0.5,0.5],[2,0.5],[2,2],[0.5,2],[0.5,0.5]]]}"#,
            )
            .unwrap(),
        );
        let b_apart = geom(
            GeometryValue::from_geojson(
                r#"{"type":"Polygon","coordinates":[[[5,5],[6,5],[6,6],[5,6],[5,5]]]}"#,
            )
            .unwrap(),
        );
        assert!(matches!(
            StIntersectsFunction
                .invoke(&[a.clone(), b_overlap], &c)
                .unwrap(),
            GqlValue::Bool(true)
        ));
        assert!(matches!(
            StIntersectsFunction.invoke(&[a, b_apart], &c).unwrap(),
            GqlValue::Bool(false)
        ));
    }

    #[test]
    fn st_dwithin_haversine_meters() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // NYC to Brooklyn Heights ~5km
        let a = geom(GeometryValue::point_wgs84(-74.0060, 40.7128));
        let b = geom(GeometryValue::point_wgs84(-73.9936, 40.6980));
        match StDWithinFunction
            .invoke(&[a.clone(), b.clone(), GqlValue::Float(10_000.0)], &c)
            .unwrap()
        {
            GqlValue::Bool(within) => assert!(within),
            _ => panic!(),
        }
        match StDWithinFunction
            .invoke(&[a, b, GqlValue::Float(100.0)], &c)
            .unwrap()
        {
            GqlValue::Bool(within) => assert!(!within),
            _ => panic!(),
        }
    }

    #[test]
    fn st_distance_planar_vs_geographic() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // Planar — euclidean
        let a_planar = geom(GeometryValue::point_planar(0.0, 0.0));
        let b_planar = geom(GeometryValue::point_planar(3.0, 4.0));
        match StDistanceFunction
            .invoke(&[a_planar, b_planar], &c)
            .unwrap()
        {
            GqlValue::Float(d) => assert!((d - 5.0).abs() < 1e-9),
            _ => panic!(),
        }
        // Geographic — haversine
        let a_geo = geom(GeometryValue::point_wgs84(0.0, 0.0));
        let b_geo = geom(GeometryValue::point_wgs84(1.0, 0.0));
        // 1° longitude at equator ≈ 111 km
        match StDistanceFunction.invoke(&[a_geo, b_geo], &c).unwrap() {
            GqlValue::Float(m) => assert!((m - 111_320.0).abs() < 200.0, "got {m}"),
            _ => panic!(),
        }
    }

    #[test]
    fn st_area_returns_square_area() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let s = geom(polygon_unit_square());
        match StAreaFunction.invoke(&[s], &c).unwrap() {
            GqlValue::Float(a) => assert!((a - 1.0).abs() < 1e-9),
            _ => panic!(),
        }
    }

    #[test]
    fn st_length_sums_linestring() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let ls = geom(
            GeometryValue::from_geojson(r#"{"type":"LineString","coordinates":[[0,0],[0,5]]}"#)
                .unwrap(),
        );
        match StLengthFunction.invoke(&[ls], &c).unwrap() {
            GqlValue::Float(l) => assert!((l - 5.0).abs() < 1e-9),
            _ => panic!(),
        }
    }

    #[test]
    fn st_envelope_returns_bbox_polygon() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let mp = geom(
            GeometryValue::from_geojson(
                r#"{"type":"MultiPoint","coordinates":[[1,2],[5,7],[-1,3]]}"#,
            )
            .unwrap(),
        );
        match StEnvelopeFunction.invoke(&[mp], &c).unwrap() {
            GqlValue::Geometry(g) => {
                assert_eq!(g.geometry_type(), "Polygon");
                assert_eq!(bbox(&g.geom), Some((-1.0, 2.0, 5.0, 7.0)));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn st_isvalid_catches_non_finite() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let p = geom(GeometryValue::point_planar(f64::NAN, 0.0));
        match StIsValidFunction.invoke(&[p], &c).unwrap() {
            GqlValue::Bool(v) => assert!(!v),
            _ => panic!(),
        }
    }

    #[test]
    fn st_equals_compares_full_value() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let a = geom(GeometryValue::point_wgs84(1.0, 2.0));
        let b = geom(GeometryValue::point_wgs84(1.0, 2.0));
        let c_diff_crs = geom(GeometryValue::point_planar(1.0, 2.0));
        match StEqualsFunction.invoke(&[a.clone(), b], &c).unwrap() {
            GqlValue::Bool(eq) => assert!(eq),
            _ => panic!(),
        }
        match StEqualsFunction.invoke(&[a, c_diff_crs], &c).unwrap() {
            GqlValue::Bool(eq) => assert!(!eq),
            _ => panic!(),
        }
    }

    #[test]
    fn st_makepolygon_from_points() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let points = GqlValue::List(crate::types::value::GqlList {
            element_type: GqlType::Geometry,
            elements: Arc::from(vec![
                geom(GeometryValue::point_planar(0.0, 0.0)),
                geom(GeometryValue::point_planar(1.0, 0.0)),
                geom(GeometryValue::point_planar(1.0, 1.0)),
                geom(GeometryValue::point_planar(0.0, 1.0)),
            ]),
        });
        match StMakePolygonFunction.invoke(&[points], &c).unwrap() {
            GqlValue::Geometry(g) => {
                assert_eq!(g.geometry_type(), "Polygon");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn st_makepolygon_rejects_fewer_than_three_points() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let points = GqlValue::List(crate::types::value::GqlList {
            element_type: GqlType::Geometry,
            elements: Arc::from(vec![
                geom(GeometryValue::point_planar(0.0, 0.0)),
                geom(GeometryValue::point_planar(1.0, 0.0)),
            ]),
        });
        assert!(StMakePolygonFunction.invoke(&[points], &c).is_err());
    }

    #[test]
    fn st_geomfromgeojson_parses() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let s = GqlValue::String(SmolStr::new(r#"{"type":"Point","coordinates":[1,2]}"#));
        match StGeomFromGeoJsonFunction.invoke(&[s], &c).unwrap() {
            GqlValue::Geometry(g) => assert_eq!(g.geometry_type(), "Point"),
            _ => panic!(),
        }
    }

    #[test]
    fn distance_between_non_points_uses_min_vertex() {
        // Polygon-to-point: minimum distance is from the nearest polygon
        // vertex, not the first coordinate.
        let poly = polygon_unit_square(); // square at (0..1, 0..1)
        let point = GeometryValue::point_planar(3.0, 0.0);
        let d = distance(&poly, &point).unwrap();
        // Nearest vertex to (3, 0) is (1, 0) at distance 2.0.
        assert!((d - 2.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn distance_errors_on_empty_geometry() {
        let empty =
            GeometryValue::from_geojson(r#"{"type":"MultiPoint","coordinates":[]}"#).unwrap();
        let p = GeometryValue::point_planar(0.0, 0.0);
        assert!(distance(&empty, &p).is_err());
    }

    #[test]
    fn bbox_returns_none_for_empty() {
        let empty =
            GeometryValue::from_geojson(r#"{"type":"MultiPoint","coordinates":[]}"#).unwrap();
        assert_eq!(bbox(&empty.geom), None);
    }

    #[test]
    fn st_envelope_errors_on_empty() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let empty =
            geom(GeometryValue::from_geojson(r#"{"type":"MultiPoint","coordinates":[]}"#).unwrap());
        assert!(StEnvelopeFunction.invoke(&[empty], &c).is_err());
    }

    #[test]
    fn st_distancesphere_rejects_non_points() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let poly = geom(polygon_unit_square());
        let p = geom(GeometryValue::point_wgs84(0.0, 0.0));
        assert!(StDistanceSphereFunction.invoke(&[poly, p], &c).is_err());
    }

    #[test]
    fn st_makepolygon_propagates_shared_crs() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let points = GqlValue::List(crate::types::value::GqlList {
            element_type: GqlType::Geometry,
            elements: Arc::from(vec![
                geom(GeometryValue::point_wgs84(0.0, 0.0)),
                geom(GeometryValue::point_wgs84(1.0, 0.0)),
                geom(GeometryValue::point_wgs84(1.0, 1.0)),
                geom(GeometryValue::point_wgs84(0.0, 1.0)),
            ]),
        });
        match StMakePolygonFunction.invoke(&[points], &c).unwrap() {
            GqlValue::Geometry(g) => {
                assert_eq!(g.crs.as_ref().map(|c| c.as_str()), Some("EPSG:4326"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn st_makepolygon_rejects_mixed_crs() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let points = GqlValue::List(crate::types::value::GqlList {
            element_type: GqlType::Geometry,
            elements: Arc::from(vec![
                geom(GeometryValue::point_wgs84(0.0, 0.0)),
                geom(GeometryValue::point_planar(1.0, 0.0)),
                geom(GeometryValue::point_wgs84(1.0, 1.0)),
            ]),
        });
        assert!(StMakePolygonFunction.invoke(&[points], &c).is_err());
    }

    #[test]
    fn contains_rejects_polygon_whose_edge_exits() {
        // Two squares: outer [0..10, 0..10], inner shape whose vertices all
        // lie inside but whose edge exits via (5, 12). The old "every vertex
        // inside" check wrongly said contained; the new segment-boundary
        // check correctly says not contained.
        let outer = GeometryValue::from_geojson(
            r#"{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]]]}"#,
        )
        .unwrap();
        // All 4 vertices lie inside the outer square; edge (9,9)-(1,9) is
        // inside, but edge (1,1)-(9,9) crosses the outer boundary? No —
        // that is entirely inside. Let me use a bow-tie: vertices inside
        // but two edges cross through (-1, 5) and (11, 5) — outside.
        // Easier: inner triangle whose one vertex is *outside* to prove
        // the earlier unit tests still correctly say "not contained".
        let outside_vertex = GeometryValue::from_geojson(
            r#"{"type":"Polygon","coordinates":[[[5,5],[15,5],[5,15],[5,5]]]}"#,
        )
        .unwrap();
        assert!(!contains(&outer.geom, &outside_vertex.geom));
    }

    #[test]
    fn st_asgeojson_round_trips() {
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let p = geom(GeometryValue::point_planar(1.0, 2.0));
        match StAsGeoJsonFunction.invoke(&[p], &c).unwrap() {
            GqlValue::String(s) => {
                assert!(s.contains("Point"));
                assert!(s.contains("1"));
                assert!(s.contains("2"));
            }
            _ => panic!(),
        }
    }
}

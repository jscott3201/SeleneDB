//! Spatial geometry values.
//!
//! Backed by `geo_types::Geometry<f64>` from the georust ecosystem — just the
//! primitive types. Algorithms live in `selene-gql::functions::spatial` so we
//! control the hot path and keep the dep tree small. GeoJSON is the canonical
//! ingest/serialize format; WKT parsing is deferred to a future release.
//!
//! Coordinate reference systems are tracked as an optional hint (`crs`). If
//! unset, callers treat the geometry as planar. If set to `EPSG:4326`, distance
//! functions switch to haversine. No reprojection is performed at this layer.
//!
//! `GeometryValue` is always wrapped in `Arc` inside `Value::Geometry` so
//! polygons with large coordinate rings are cheap to clone through the
//! mutation batcher and plan cache.
//!
//! ## Example
//!
//! ```
//! use selene_core::geometry::GeometryValue;
//!
//! let point = GeometryValue::from_geojson(r#"{"type":"Point","coordinates":[-74.0060,40.7128]}"#)
//!     .expect("valid GeoJSON");
//! assert_eq!(point.geometry_type(), "Point");
//! ```
//!
//! Round-trips through GeoJSON:
//!
//! ```
//! # use selene_core::geometry::GeometryValue;
//! let original = GeometryValue::from_geojson(r#"{"type":"Point","coordinates":[1.0,2.0]}"#).unwrap();
//! let serialized = original.to_geojson();
//! let reparsed = GeometryValue::from_geojson(&serialized).unwrap();
//! assert_eq!(original, reparsed);
//! ```

use crate::interner::IStr;

/// A spatial geometry with an optional coordinate reference system hint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GeometryValue {
    /// The underlying geometry. `geo_types::Geometry` is a flat enum over
    /// Point / Line / LineString / Polygon / MultiPoint / MultiLineString /
    /// MultiPolygon / GeometryCollection / Rect / Triangle.
    pub geom: geo_types::Geometry<f64>,
    /// Optional CRS hint (e.g. `EPSG:4326`). Algorithms interpret this to
    /// pick haversine vs. euclidean. `None` means unspecified — callers fall
    /// back to planar behavior.
    pub crs: Option<IStr>,
}

/// Errors from parsing or constructing a geometry.
#[derive(Debug, thiserror::Error)]
pub enum GeometryError {
    #[error("invalid GeoJSON: {0}")]
    InvalidGeoJson(String),
    #[error("GeoJSON feature/feature-collection contains no geometry")]
    NoGeometry,
}

impl GeometryValue {
    /// Construct a point in the WGS84 geographic CRS.
    ///
    /// Order is longitude then latitude (GeoJSON convention).
    pub fn point_wgs84(lng: f64, lat: f64) -> Self {
        Self {
            geom: geo_types::Geometry::Point(geo_types::Point::new(lng, lat)),
            crs: Some(IStr::new("EPSG:4326")),
        }
    }

    /// Construct a point without specifying a CRS (treated as planar).
    ///
    /// Useful for local coordinate systems, unit-test fixtures, or when the
    /// surrounding application tracks CRS externally.
    pub fn point_planar(x: f64, y: f64) -> Self {
        Self {
            geom: geo_types::Geometry::Point(geo_types::Point::new(x, y)),
            crs: None,
        }
    }

    /// Construct from any `geo_types::Geometry`, leaving the CRS unspecified.
    pub fn from_geo(geom: geo_types::Geometry<f64>) -> Self {
        Self { geom, crs: None }
    }

    /// Parse a GeoJSON geometry, feature, or feature-collection string.
    ///
    /// For feature and feature-collection inputs, only the first geometry is
    /// extracted. Feature properties are discarded — use the property map on
    /// the containing node/edge for metadata.
    pub fn from_geojson(s: &str) -> Result<Self, GeometryError> {
        let gj: geojson::GeoJson = s
            .parse()
            .map_err(|e: geojson::Error| GeometryError::InvalidGeoJson(e.to_string()))?;
        let geom = extract_first_geometry(gj)?;
        Ok(Self::from_geo(geom))
    }

    /// Serialize as compact GeoJSON (single-line, no pretty-printing).
    pub fn to_geojson(&self) -> String {
        let gj: geojson::Geometry = geojson::Geometry::from(&self.geom);
        gj.to_string()
    }

    /// Human-readable geometry type name matching OGC conventions.
    pub fn geometry_type(&self) -> &'static str {
        match &self.geom {
            geo_types::Geometry::Point(_) => "Point",
            geo_types::Geometry::Line(_) => "Line",
            geo_types::Geometry::LineString(_) => "LineString",
            geo_types::Geometry::Polygon(_) => "Polygon",
            geo_types::Geometry::MultiPoint(_) => "MultiPoint",
            geo_types::Geometry::MultiLineString(_) => "MultiLineString",
            geo_types::Geometry::MultiPolygon(_) => "MultiPolygon",
            geo_types::Geometry::GeometryCollection(_) => "GeometryCollection",
            geo_types::Geometry::Rect(_) => "Rect",
            geo_types::Geometry::Triangle(_) => "Triangle",
        }
    }

    /// Number of coordinate pairs in the geometry. Useful for quick size
    /// checks without walking the full structure.
    pub fn coord_count(&self) -> usize {
        count_coords(&self.geom)
    }
}

fn extract_first_geometry(gj: geojson::GeoJson) -> Result<geo_types::Geometry<f64>, GeometryError> {
    let geojson_geom = match gj {
        geojson::GeoJson::Geometry(g) => g,
        geojson::GeoJson::Feature(f) => f.geometry.ok_or(GeometryError::NoGeometry)?,
        geojson::GeoJson::FeatureCollection(fc) => fc
            .features
            .into_iter()
            .find_map(|f| f.geometry)
            .ok_or(GeometryError::NoGeometry)?,
    };
    geo_types::Geometry::<f64>::try_from(geojson_geom)
        .map_err(|e| GeometryError::InvalidGeoJson(e.to_string()))
}

fn count_coords(geom: &geo_types::Geometry<f64>) -> usize {
    match geom {
        geo_types::Geometry::Point(_) => 1,
        geo_types::Geometry::Line(_) => 2,
        geo_types::Geometry::LineString(ls) => ls.0.len(),
        geo_types::Geometry::Polygon(p) => {
            p.exterior().0.len() + p.interiors().iter().map(|r| r.0.len()).sum::<usize>()
        }
        geo_types::Geometry::MultiPoint(mp) => mp.0.len(),
        geo_types::Geometry::MultiLineString(mls) => mls.0.iter().map(|ls| ls.0.len()).sum(),
        geo_types::Geometry::MultiPolygon(mp) => mp.0.iter().map(count_polygon_coords).sum(),
        geo_types::Geometry::GeometryCollection(gc) => gc.0.iter().map(count_coords).sum(),
        geo_types::Geometry::Rect(_) | geo_types::Geometry::Triangle(_) => 4,
    }
}

fn count_polygon_coords(p: &geo_types::Polygon<f64>) -> usize {
    p.exterior().0.len() + p.interiors().iter().map(|r| r.0.len()).sum::<usize>()
}

impl PartialEq for GeometryValue {
    fn eq(&self, other: &Self) -> bool {
        self.geom == other.geom && self.crs == other.crs
    }
}

/// Convenience: construct a `GeometryValue` from any `geo_types::Geometry`.
impl From<geo_types::Geometry<f64>> for GeometryValue {
    fn from(geom: geo_types::Geometry<f64>) -> Self {
        Self::from_geo(geom)
    }
}

impl From<geo_types::Point<f64>> for GeometryValue {
    fn from(p: geo_types::Point<f64>) -> Self {
        Self::from_geo(geo_types::Geometry::Point(p))
    }
}

impl From<geo_types::Polygon<f64>> for GeometryValue {
    fn from(p: geo_types::Polygon<f64>) -> Self {
        Self::from_geo(geo_types::Geometry::Polygon(p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_round_trips_through_geojson() {
        let p = GeometryValue::point_wgs84(-74.0060, 40.7128);
        let s = p.to_geojson();
        let back = GeometryValue::from_geojson(&s).unwrap();
        assert_eq!(back.geometry_type(), "Point");
        assert_eq!(back.coord_count(), 1);
    }

    #[test]
    fn polygon_round_trips_through_geojson() {
        let input = r#"{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}"#;
        let g = GeometryValue::from_geojson(input).unwrap();
        assert_eq!(g.geometry_type(), "Polygon");
        assert_eq!(g.coord_count(), 5);
        let s = g.to_geojson();
        let back = GeometryValue::from_geojson(&s).unwrap();
        assert_eq!(g, back);
    }

    #[test]
    fn polygon_with_hole_counts_both_rings() {
        let input = r#"{
            "type":"Polygon",
            "coordinates":[
                [[0,0],[10,0],[10,10],[0,10],[0,0]],
                [[2,2],[4,2],[4,4],[2,4],[2,2]]
            ]
        }"#;
        let g = GeometryValue::from_geojson(input).unwrap();
        assert_eq!(g.coord_count(), 10);
    }

    #[test]
    fn multipolygon_sums_coords() {
        let input = r#"{"type":"MultiPolygon","coordinates":[
            [[[0,0],[1,0],[1,1],[0,0]]],
            [[[2,2],[3,2],[3,3],[2,2]]]
        ]}"#;
        let g = GeometryValue::from_geojson(input).unwrap();
        assert_eq!(g.geometry_type(), "MultiPolygon");
        assert_eq!(g.coord_count(), 8);
    }

    #[test]
    fn feature_unwraps_to_geometry() {
        let input = r#"{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{"name":"x"}}"#;
        let g = GeometryValue::from_geojson(input).unwrap();
        assert_eq!(g.geometry_type(), "Point");
    }

    #[test]
    fn feature_collection_takes_first_geometry() {
        let input = r#"{"type":"FeatureCollection","features":[
            {"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{}},
            {"type":"Feature","geometry":{"type":"Point","coordinates":[3,4]},"properties":{}}
        ]}"#;
        let g = GeometryValue::from_geojson(input).unwrap();
        assert_eq!(g.geometry_type(), "Point");
    }

    #[test]
    fn invalid_geojson_errors() {
        let err = GeometryValue::from_geojson("{ this isn't valid").unwrap_err();
        assert!(matches!(err, GeometryError::InvalidGeoJson(_)));
    }

    #[test]
    fn feature_without_geometry_errors() {
        let input = r#"{"type":"Feature","geometry":null,"properties":{}}"#;
        let err = GeometryValue::from_geojson(input).unwrap_err();
        assert!(matches!(err, GeometryError::NoGeometry));
    }

    #[test]
    fn point_constructor_sets_wgs84_crs() {
        let p = GeometryValue::point_wgs84(0.0, 0.0);
        assert_eq!(p.crs.as_ref().map(|c| c.as_str()), Some("EPSG:4326"));
    }

    #[test]
    fn from_geo_leaves_crs_unspecified() {
        let g =
            GeometryValue::from_geo(geo_types::Geometry::Point(geo_types::Point::new(1.0, 2.0)));
        assert!(g.crs.is_none());
    }

    #[test]
    fn postcard_round_trip_point() {
        let original = GeometryValue::point_wgs84(-74.0060, 40.7128);
        let bytes = postcard::to_allocvec(&original).expect("serialize");
        let decoded: GeometryValue = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    #[test]
    fn postcard_round_trip_polygon_with_hole() {
        let input = r#"{"type":"Polygon","coordinates":[
            [[0,0],[10,0],[10,10],[0,10],[0,0]],
            [[2,2],[4,2],[4,4],[2,4],[2,2]]
        ]}"#;
        let original = GeometryValue::from_geojson(input).unwrap();
        let bytes = postcard::to_allocvec(&original).expect("serialize");
        let decoded: GeometryValue = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
        assert_eq!(decoded.coord_count(), 10);
    }

    #[test]
    fn postcard_round_trip_multipolygon() {
        let input = r#"{"type":"MultiPolygon","coordinates":[
            [[[0,0],[1,0],[1,1],[0,0]]],
            [[[2,2],[3,2],[3,3],[2,2]]]
        ]}"#;
        let original = GeometryValue::from_geojson(input).unwrap();
        let bytes = postcard::to_allocvec(&original).expect("serialize");
        let decoded: GeometryValue = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    #[test]
    fn postcard_round_trip_geometry_collection() {
        let input = r#"{"type":"GeometryCollection","geometries":[
            {"type":"Point","coordinates":[1,2]},
            {"type":"LineString","coordinates":[[0,0],[5,5]]}
        ]}"#;
        let original = GeometryValue::from_geojson(input).unwrap();
        let bytes = postcard::to_allocvec(&original).expect("serialize");
        let decoded: GeometryValue = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }
}

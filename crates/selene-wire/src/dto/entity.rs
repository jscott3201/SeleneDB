//! Entity (node/edge) DTOs for wire transfer.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use selene_core::Value;

/// A node as transferred over the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeDto {
    pub id: u64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, Value>,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u64,
}

/// An edge as transferred over the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeDto {
    pub id: u64,
    pub source: u64,
    pub target: u64,
    pub label: String,
    pub properties: HashMap<String, Value>,
    pub created_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};

    #[test]
    fn node_dto_postcard_round_trip() {
        let dto = NodeDto {
            id: 42,
            labels: vec!["sensor".into(), "temperature".into()],
            properties: {
                let mut m = HashMap::new();
                m.insert("unit".into(), Value::str("°F"));
                m.insert("value".into(), Value::Float(72.5));
                m
            },
            created_at: 1000,
            updated_at: 2000,
            version: 3,
        };

        let bytes = serialize_payload(&dto, WireFlags::empty()).unwrap();
        let decoded: NodeDto = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.labels.len(), 2);
        assert_eq!(decoded.version, 3);
    }

    #[test]
    fn edge_dto_json_round_trip() {
        let dto = EdgeDto {
            id: 1,
            source: 10,
            target: 20,
            label: "contains".into(),
            properties: HashMap::new(),
            created_at: 500,
        };

        let bytes = serialize_payload(&dto, WireFlags::JSON_FORMAT).unwrap();
        let json = std::str::from_utf8(&bytes).unwrap();
        assert!(json.contains("contains"));

        let decoded: EdgeDto = deserialize_payload(&bytes, WireFlags::JSON_FORMAT).unwrap();
        assert_eq!(decoded.source, 10);
    }
}

#![forbid(unsafe_code)]
//! selene-core: Core types for the Selene property graph runtime.

pub mod changeset;
pub mod entity;
pub mod interner;
pub mod io;
pub mod label_set;
pub mod origin;
pub mod property_map;
pub mod schema;
pub mod trigger;
pub mod value;

pub use entity::{Edge, EdgeId, Node, NodeId, now_nanos};
pub use interner::{IStr, try_intern};
pub use label_set::LabelSet;
pub use origin::Origin;
pub use property_map::PropertyMap;
pub use schema::{
    EdgeSchema, FillStrategy, NodeSchema, PropertyDef, ValidationMode, ValueEncoding, ValueType,
};
pub use trigger::{TriggerDef, TriggerEvent};
pub use value::Value;

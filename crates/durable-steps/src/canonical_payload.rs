//! Stable JSON object ordering without converting integers through floating point.

use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn encode<T: Serialize>(payload: &T) -> Result<Vec<u8>, serde_json::Error> {
    // A task's generic payload schema is intentionally unknown here. Value is
    // confined to this encoding boundary, never used as a journal/domain row.
    let payload = sort_objects(serde_json::to_value(payload)?);
    serde_json::to_vec(&payload)
}

fn sort_objects(payload: Value) -> Value {
    match payload {
        Value::Object(fields) => {
            let ordered: BTreeMap<_, _> = fields
                .into_iter()
                .map(|(key, value)| (key, sort_objects(value)))
                .collect();
            Value::Object(ordered.into_iter().collect())
        }
        Value::Array(elements) => Value::Array(elements.into_iter().map(sort_objects).collect()),
        value => value,
    }
}

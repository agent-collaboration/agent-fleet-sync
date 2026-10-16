use serde::{Deserialize, Serialize};

/// One canonical file identity. Lowercase ASCII avoids aliases on native
/// filesystems with case-insensitive names; separators cannot escape the store.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectKey(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("object key must contain lowercase ASCII letters, digits or hyphens")]
pub struct InvalidObjectKey;

impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidObjectKey> {
        let value = value.into();
        if value.is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(InvalidObjectKey);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ObjectKey {
    type Error = InvalidObjectKey;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<ObjectKey> for String {
    fn from(key: ObjectKey) -> Self {
        key.0
    }
}

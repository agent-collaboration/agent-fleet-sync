use serde::{Deserialize, Serialize};

/// The authenticated consumer identity; synchronization treats it as opaque text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ActorRef(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("actor reference must contain 1 to 256 bytes")]
pub struct ActorRefError;

impl ActorRef {
    pub fn new(value: impl Into<String>) -> Result<Self, ActorRefError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 {
            return Err(ActorRefError);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ActorRef {
    type Error = ActorRefError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<ActorRef> for String {
    fn from(value: ActorRef) -> Self {
        value.0
    }
}

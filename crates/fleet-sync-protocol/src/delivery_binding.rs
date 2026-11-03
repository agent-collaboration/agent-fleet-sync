use serde::{Deserialize, Serialize};

/// A binding lookup could not establish an owner. This is never a forgery verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SenderBindingFailure {
    Unknown,
    Conflict,
    Unavailable,
}
impl SenderBindingFailure {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Unknown => "senderBindingUnknown",
            Self::Conflict => "senderBindingConflict",
            Self::Unavailable => "senderBindingUnavailable",
        }
    }
}

#[cfg(test)]
#[path = "delivery_binding_test.rs"]
mod tests;

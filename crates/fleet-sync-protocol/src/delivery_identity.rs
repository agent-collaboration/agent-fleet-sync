use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("delivery item identity must be a UUIDv7")]
pub struct ItemIdentityError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct ItemId(Uuid);

impl ItemId {
    pub fn new(value: Uuid) -> Result<Self, ItemIdentityError> {
        (value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122)
            .then_some(Self(value))
            .ok_or(ItemIdentityError)
    }
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}
impl TryFrom<Uuid> for ItemId {
    type Error = ItemIdentityError;
    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<ItemId> for Uuid {
    fn from(value: ItemId) -> Self {
        value.0
    }
}

macro_rules! delivery_counter {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(u64);
        impl $name {
            pub const fn new(value: u64) -> Self { Self(value) }
            pub const fn value(self) -> u64 { self.0 }
            pub fn next(self) -> Option<Self> { self.0.checked_add(1).map(Self) }
        }
    )+};
}
delivery_counter!(
    DeliverySeq,
    StatusVersion,
    ReportSeq,
    StatusCursor,
    ReceiveCycle
);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ItemDigest(String);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveryItemRef {
    pub item: ItemId,
    pub digest: ItemDigest,
}

impl TryFrom<String> for ItemDigest {
    type Error = crate::DigestError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        crate::PieceDigest::try_from(value.clone())?;
        Ok(Self(value))
    }
}
impl From<ItemDigest> for String {
    fn from(value: ItemDigest) -> Self {
        value.0
    }
}
impl ItemDigest {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub(crate) fn of_content(value: &impl Serialize) -> Result<Self, crate::DigestError> {
        // Consumer bodies may contain maps. Canonicalize this temporary encoding;
        // domain values and custody rows remain typed throughout.
        let mut encoded = serde_json::to_value(value)
            .map_err(|error| crate::DigestError::Encoding(error.to_string()))?;
        encoded.sort_all_objects();
        Ok(Self(
            crate::PieceDigest::of_canonical(&encoded)?
                .as_str()
                .to_owned(),
        ))
    }
}

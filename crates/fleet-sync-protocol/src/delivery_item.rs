use crate::{DigestError, ItemDigest, ItemId, PeerId, SyncTime};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};

pub trait DeliveryTypes: Send + Sync + 'static {
    const MAX_SENDER_BYTES: u32;
    const MAX_ADDRESS_BYTES: u32;
    const MAX_ACCEPTANCE_BYTES: u32;
    /// Maximum canonical JSON bytes of the consumer's local-only observation.
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    /// Maximum canonical JSON bytes of the host-owned status address.
    const MAX_STATUS_ROUTE_BYTES: u32;
    type Body: Serialize + DeserializeOwned + Send + Sync;
    type Sender: Serialize + DeserializeOwned + Send + Sync;
    type Address: Serialize + DeserializeOwned + Send + Sync;
    type Acceptance: Serialize + DeserializeOwned + Send + Sync;
    /// Consumer-owned evidence retained only in the local delivery journal.
    type LocalObservation: Serialize + DeserializeOwned + Send + Sync;
    /// Verified submitter address carried unchanged through both publication hops.
    type StatusRoute: Clone + Serialize + DeserializeOwned + Send + Sync + 'static;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NoDelivery;
impl DeliveryTypes for NoDelivery {
    const MAX_SENDER_BYTES: u32 = 0;
    const MAX_ADDRESS_BYTES: u32 = 0;
    const MAX_ACCEPTANCE_BYTES: u32 = 0;
    const MAX_STATUS_ROUTE_BYTES: u32 = 4;
    type Body = ();
    type Sender = ();
    type Address = ();
    type Acceptance = ();
    type LocalObservation = ();
    type StatusRoute = ();
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
/// The recipient retains this identity to authorize cancels after releasing a body.
pub struct DeliveryHeader<D: DeliveryTypes> {
    pub id: ItemId,
    pub sender: D::Sender,
    pub recipient: Recipient<D::Address>,
    pub valid_until: Option<SyncTime>,
    pub digest: ItemDigest,
}
impl<D: DeliveryTypes> DeliveryHeader<D> {
    pub const fn id(&self) -> ItemId {
        self.id
    }
    pub fn sender(&self) -> &D::Sender {
        &self.sender
    }
    pub fn recipient(&self) -> &Recipient<D::Address> {
        &self.recipient
    }
    pub const fn valid_until(&self) -> Option<SyncTime> {
        self.valid_until
    }
    pub fn digest(&self) -> &ItemDigest {
        &self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Recipient<A> {
    pub peer: PeerId,
    pub address: A,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CancelBody {
    pub target: ItemId,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", bound = "")]
pub struct DeliveryItem<D: DeliveryTypes> {
    id: ItemId,
    sender: D::Sender,
    recipient: Recipient<D::Address>,
    valid_until: Option<SyncTime>,
    body: D::Body,
    digest: ItemDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ItemFields<B, S, A> {
    id: ItemId,
    sender: S,
    recipient: Recipient<A>,
    valid_until: Option<SyncTime>,
    body: B,
    digest: ItemDigest,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ItemContent<'a, B, S, A> {
    id: ItemId,
    sender: &'a S,
    recipient: &'a Recipient<A>,
    valid_until: Option<SyncTime>,
    body: &'a B,
}

impl<D: DeliveryTypes> DeliveryItem<D> {
    pub fn new(
        id: ItemId,
        sender: D::Sender,
        recipient: Recipient<D::Address>,
        valid_until: Option<SyncTime>,
        body: D::Body,
    ) -> Result<Self, DigestError> {
        let digest = ItemDigest::of_content(&(
            "item",
            ItemContent {
                id,
                sender: &sender,
                recipient: &recipient,
                valid_until,
                body: &body,
            },
        ))?;
        Ok(Self {
            id,
            sender,
            recipient,
            valid_until,
            body,
            digest,
        })
    }
    pub const fn id(&self) -> ItemId {
        self.id
    }
    pub fn sender(&self) -> &D::Sender {
        &self.sender
    }
    pub fn recipient(&self) -> &Recipient<D::Address> {
        &self.recipient
    }
    pub const fn valid_until(&self) -> Option<SyncTime> {
        self.valid_until
    }
    pub fn body(&self) -> &D::Body {
        &self.body
    }
    pub fn digest(&self) -> &ItemDigest {
        &self.digest
    }
}
impl<'de, D: DeliveryTypes> Deserialize<'de> for DeliveryItem<D> {
    fn deserialize<T: Deserializer<'de>>(deserializer: T) -> Result<Self, T::Error> {
        let fields = ItemFields::<D::Body, D::Sender, D::Address>::deserialize(deserializer)?;
        let item = Self::new(
            fields.id,
            fields.sender,
            fields.recipient,
            fields.valid_until,
            fields.body,
        )
        .map_err(serde::de::Error::custom)?;
        if fields.digest != item.digest {
            return Err(serde::de::Error::custom("delivery item digest mismatch"));
        }
        Ok(item)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", bound = "")]
pub struct DeliveryCancel<D: DeliveryTypes> {
    id: ItemId,
    sender: D::Sender,
    recipient: Recipient<D::Address>,
    valid_until: Option<SyncTime>,
    body: CancelBody,
    digest: ItemDigest,
}
impl<D: DeliveryTypes> DeliveryCancel<D> {
    pub fn new(
        id: ItemId,
        sender: D::Sender,
        recipient: Recipient<D::Address>,
        valid_until: Option<SyncTime>,
        body: CancelBody,
    ) -> Result<Self, DigestError> {
        let digest = ItemDigest::of_content(&(
            "cancel",
            ItemContent {
                id,
                sender: &sender,
                recipient: &recipient,
                valid_until,
                body: &body,
            },
        ))?;
        Ok(Self {
            id,
            sender,
            recipient,
            valid_until,
            body,
            digest,
        })
    }
    pub const fn id(&self) -> ItemId {
        self.id
    }
    pub fn sender(&self) -> &D::Sender {
        &self.sender
    }
    pub fn recipient(&self) -> &Recipient<D::Address> {
        &self.recipient
    }
    pub const fn valid_until(&self) -> Option<SyncTime> {
        self.valid_until
    }
    pub const fn target(&self) -> ItemId {
        self.body.target
    }
    pub fn digest(&self) -> &ItemDigest {
        &self.digest
    }
}
impl<'de, D: DeliveryTypes> Deserialize<'de> for DeliveryCancel<D> {
    fn deserialize<T: Deserializer<'de>>(deserializer: T) -> Result<Self, T::Error> {
        let fields = ItemFields::<CancelBody, D::Sender, D::Address>::deserialize(deserializer)?;
        let item = Self::new(
            fields.id,
            fields.sender,
            fields.recipient,
            fields.valid_until,
            fields.body,
        )
        .map_err(serde::de::Error::custom)?;
        if fields.digest != item.digest {
            return Err(serde::de::Error::custom("delivery cancel digest mismatch"));
        }
        Ok(item)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum DeliveryEnvelope<D: DeliveryTypes> {
    Item(DeliveryItem<D>),
    Cancel(DeliveryCancel<D>),
}
impl<D: DeliveryTypes> DeliveryEnvelope<D> {
    pub fn id(&self) -> ItemId {
        match self {
            Self::Item(item) => item.id(),
            Self::Cancel(item) => item.id(),
        }
    }
    pub fn sender(&self) -> &D::Sender {
        match self {
            Self::Item(item) => item.sender(),
            Self::Cancel(item) => item.sender(),
        }
    }
    pub fn recipient(&self) -> &Recipient<D::Address> {
        match self {
            Self::Item(item) => item.recipient(),
            Self::Cancel(item) => item.recipient(),
        }
    }
    pub fn digest(&self) -> &ItemDigest {
        match self {
            Self::Item(item) => item.digest(),
            Self::Cancel(item) => item.digest(),
        }
    }
    pub fn valid_until(&self) -> Option<SyncTime> {
        match self {
            Self::Item(item) => item.valid_until(),
            Self::Cancel(item) => item.valid_until(),
        }
    }
    pub const fn stream(&self) -> crate::DeliveryStream {
        match self {
            Self::Item(_) => crate::DeliveryStream::Items,
            Self::Cancel(_) => crate::DeliveryStream::Controls,
        }
    }
}

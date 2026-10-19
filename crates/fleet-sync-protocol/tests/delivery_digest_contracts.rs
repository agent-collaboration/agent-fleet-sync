#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeMap};
use std::collections::BTreeMap;

// Both encodings carry exactly {a:1,b:2}; decoding deliberately reverses field
// order to expose a digest that depends on a consumer's map iteration order.
struct ConsumerMap {
    reversed: bool,
}
impl Serialize for ConsumerMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        if self.reversed {
            map.serialize_entry("b", &2)?;
            map.serialize_entry("a", &1)?;
        } else {
            map.serialize_entry("a", &1)?;
            map.serialize_entry("b", &2)?;
        }
        map.end()
    }
}
impl<'de> Deserialize<'de> for ConsumerMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let _: BTreeMap<String, u64> = BTreeMap::deserialize(deserializer)?;
        Ok(Self { reversed: true })
    }
}
struct MapDelivery;
impl DeliveryTypes for MapDelivery {
    const MAX_SENDER_BYTES: u32 = 1024;
    const MAX_ADDRESS_BYTES: u32 = 1024;
    const MAX_ACCEPTANCE_BYTES: u32 = 1024;
    const MAX_STATUS_ROUTE_BYTES: u32 = 4;
    type Body = ConsumerMap;
    type Sender = ();
    type Address = ();
    type Acceptance = ();
    type LocalObservation = ();
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    type StatusRoute = ();
}
#[test]
fn consumer_map_field_order_does_not_change_item_identity() {
    let item = DeliveryItem::<MapDelivery>::new(
        ItemId::new("01900000-0000-7000-8000-000000000001".parse().unwrap()).unwrap(),
        (),
        Recipient {
            peer: PeerId::new("019eef00-0000-7000-8000-000000000002".parse().unwrap()).unwrap(),
            address: (),
        },
        None,
        ConsumerMap { reversed: false },
    )
    .unwrap();
    let decoded: DeliveryItem<MapDelivery> =
        serde_json::from_str(&serde_json::to_string(&item).unwrap()).unwrap();
    assert_eq!(item.digest(), decoded.digest());
}

#[test]
fn decoding_rejects_changed_content_with_the_original_valid_digest() {
    let item = DeliveryItem::<NoDelivery>::new(
        ItemId::new("01900000-0000-7000-8000-000000000001".parse().unwrap()).unwrap(),
        (),
        Recipient {
            peer: PeerId::new("019eef00-0000-7000-8000-000000000002".parse().unwrap()).unwrap(),
            address: (),
        },
        None,
        (),
    )
    .unwrap();
    let mut wire = serde_json::to_value(&item).unwrap();
    assert!(serde_json::from_value::<DeliveryItem<NoDelivery>>(wire.clone()).is_ok());
    wire["validUntil"] = serde_json::json!(1234);
    assert!(
        matches!(serde_json::from_value::<DeliveryItem<NoDelivery>>(wire),Err(error) if error.to_string().contains("digest mismatch"))
    );
}

#[test]
fn item_identity_rejects_other_uuid_versions_before_custody() {
    assert!(serde_json::from_str::<ItemId>("\"00000000-0000-4000-8000-000000000001\"").is_err());
    assert!(serde_json::from_str::<ItemId>("\"01900000-0000-7000-8000-000000000001\"").is_ok());
}

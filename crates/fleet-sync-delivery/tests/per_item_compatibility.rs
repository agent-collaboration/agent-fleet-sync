#![allow(clippy::unwrap_used)]
#[path = "support/per_item.rs"]
mod per_item;
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use per_item::*;
use sqlx_core::row::Row;
use support::*;

struct LegacyTypes;
impl DeliveryTypes for LegacyTypes {
    const MAX_SENDER_BYTES: u32 = 64;
    const MAX_ADDRESS_BYTES: u32 = 64;
    const MAX_ACCEPTANCE_BYTES: u32 = 1_049_600;
    const MAX_STATUS_ROUTE_BYTES: u32 = 64;
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = Acceptance;
    type LocalObservation = ();
    type StatusRoute = String;
    // Ignored without the explicit opt-in, including fetch selection.
    fn max_acceptance_bytes_for(_body: &String) -> u32 {
        64
    }
}
fn legacy_item(identity: u64) -> DeliveryItem<LegacyTypes> {
    DeliveryItem::new(
        item_id(identity),
        "sender".into(),
        Recipient {
            peer: peer(2),
            address: "inbox".into(),
        },
        None,
        "small".into(),
    )
    .unwrap()
}
fn legacy_limits(bytes: u64) -> DeliveryLimits {
    DeliveryLimits::new::<LegacyTypes>(
        bytes,
        20,
        bytes,
        20,
        bytes,
        minimum_settlement_allowance::<LegacyTypes>().unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn default_consumer_preserves_legacy_reserve_json_and_fetch_selection() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope| Box::pin(async move {
        let minimum=minimum_settlement_allowance::<LegacyTypes>().unwrap();
        assert_eq!(minimum,minimum_settlement_allowance::<VariableTypes>().unwrap());
        assert!(matches!(Outbox::<_,LegacyTypes>::new(scope,legacy_limits(50_000)).enqueue(legacy_item(1),peer(3)).await.map_err(storage_error)?,OutboxAdmission::Refused {reason:DeliveryRefusal::MailboxFull}));
        let limits=legacy_limits(100_000_000);
        Outbox::<_,LegacyTypes>::new(scope,limits).enqueue(legacy_item(1),peer(3)).await.map_err(storage_error)?;
        for identity in [1,2] { Mailbox::<_,LegacyTypes>::new(scope,peer(2),limits).submit(peer(1),DeliveryEnvelope::Item(legacy_item(identity)),now(),"route".into(),None).await.map_err(storage_error)?; }
        let row=sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record,settlement_bytes FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("settlement_bytes")?,i64::from(minimum));
        let record:serde_json::Value=serde_json::from_str(&row.try_get::<String,_>("record")?).unwrap();
        let expected=serde_json::json!({"kind":"item","value":{
            "id":item_id(1),"digest":legacy_item(1).digest(),"relay":peer(3),"recipient":peer(2),"validUntil":null,
            "status":{"kind":"queued"},"version":0,"submitAttempted":false,"cancel":null,"cancelTarget":null,"settlementBytes":minimum,"capacitySlot":true
        }});
        assert_eq!(record,expected,"default persisted shape is exactly the original header");
        let request=FetchInput {stream:DeliveryStream::Items,after:DeliverySeq::new(0),max_items:10,max_bytes:10_000,inbox_room_bytes:Some(0)};
        let batch=Mailbox::<_,LegacyTypes>::new(scope,peer(2),limits).fetch_with_budget(peer(2),request,now()).await.map_err(storage_error)?;
        assert_eq!(batch.entries.len(),2,"legacy consumers keep payload-only fetch selection");
        let mut inbox=Inbox::<_,LegacyTypes>::new(scope,peer(2),limits);
        let cycle=inbox.begin_receive(DeliveryStream::Items).await.map_err(storage_error)?.unwrap();
        let budget=inbox.receive_request(cycle,10,100_000_000).await.map_err(storage_error)?;
        assert_eq!(budget.max_items,1);
        assert_eq!(budget.max_bytes,100_000_000-u64::from(minimum));
        assert_eq!(budget.inbox_room_bytes,None);
        let encoded=serde_json::to_value(budget).unwrap();
        assert_eq!(encoded,serde_json::json!({"stream":"items","after":0,"maxItems":1,"maxBytes":100_000_000-u64::from(minimum)}));
        Ok(())
    })).await.unwrap();
}

#[test]
fn optional_fetch_budget_accepts_legacy_wire_and_round_trips_new_wire() {
    let old = serde_json::json!({"stream":"items","after":0,"maxItems":5,"maxBytes":4096});
    let decoded: FetchInput = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(decoded.inbox_room_bytes, None);
    assert_eq!(serde_json::to_value(decoded).unwrap(), old);
    let new = FetchInput {
        inbox_room_bytes: Some(20_000),
        ..decoded
    };
    let restored: FetchInput = serde_json::from_value(serde_json::to_value(new).unwrap()).unwrap();
    assert_eq!(restored.inbox_room_bytes, Some(20_000));
    let request = DeliveryRequest::<VariableTypes>::Fetch {
        call: call("capacity-fetch"),
        stream: decoded.stream,
        after: decoded.after,
        max_items: decoded.max_items,
        max_bytes: decoded.max_bytes,
        inbox_room_bytes: None,
    };
    let old_request = serde_json::to_value(request).unwrap();
    assert!(old_request["value"].get("inboxRoomBytes").is_none());
    let restored: DeliveryRequest<VariableTypes> = serde_json::from_value(old_request).unwrap();
    assert!(matches!(
        restored,
        DeliveryRequest::Fetch {
            inbox_room_bytes: None,
            ..
        }
    ));
}

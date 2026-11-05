UPDATE _fleet_sync_inbox_items SET payload=NULL,payload_bytes=0,settlement_charge_bytes=0,live=0,updated_at=CAST(?2 AS INTEGER) WHERE item_id=?1 AND legacy_read_only=0 AND progress='final';

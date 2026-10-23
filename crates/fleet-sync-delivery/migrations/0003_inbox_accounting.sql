CREATE INDEX _fleet_sync_inbox_accounting
ON _fleet_sync_inbox_items (live, payload_bytes, settlement_bytes);

SELECT CAST(COUNT(*) AS TEXT) AS items, CAST(COALESCE(SUM(payload_bytes + settlement_bytes),0) AS TEXT) AS bytes FROM _fleet_sync_inbox_items WHERE live = 1;

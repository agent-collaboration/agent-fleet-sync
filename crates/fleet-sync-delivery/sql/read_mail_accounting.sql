SELECT CAST(COALESCE(SUM(capacity_slot),0) AS TEXT) AS items, CAST(COALESCE(SUM(payload_bytes + settlement_bytes),0) AS TEXT) AS bytes FROM _fleet_sync_mail_items WHERE live = 1;

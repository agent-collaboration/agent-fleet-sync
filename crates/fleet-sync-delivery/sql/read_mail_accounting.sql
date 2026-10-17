SELECT
    CAST(COALESCE(SUM(CASE WHEN live = 1 THEN capacity_slot ELSE 0 END), 0) AS TEXT) AS items,
    CAST(CASE WHEN COALESCE(MAX(CASE WHEN
        typeof(live) = 'integer' AND live IN (0, 1) AND
        typeof(capacity_slot) = 'integer' AND capacity_slot IN (0, 1) AND
        typeof(payload_bytes) = 'integer' AND payload_bytes >= 0 AND
        typeof(settlement_bytes) = 'integer' AND settlement_bytes >= 0
        THEN 0 ELSE 1 END), 0) = 0
    THEN CAST(COALESCE(SUM(CASE WHEN live = 1 THEN payload_bytes + settlement_bytes ELSE 0 END), 0) AS TEXT)
    ELSE NULL END AS TEXT) AS bytes
FROM _fleet_sync_mail_items;

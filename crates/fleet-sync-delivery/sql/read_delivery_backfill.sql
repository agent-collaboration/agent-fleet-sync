SELECT CAST(CASE WHEN typeof(id)='text' THEN id END AS TEXT) AS id,name FROM _fleet_sync_delivery_backfills WHERE name=?1;

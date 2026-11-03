DELETE FROM _fleet_sync_status_events WHERE machine_id = ?2 AND cursor <= CAST(?1 AS INTEGER);

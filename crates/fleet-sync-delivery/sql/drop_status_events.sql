DELETE FROM _fleet_sync_status_events WHERE cursor <= CAST(?1 AS INTEGER);

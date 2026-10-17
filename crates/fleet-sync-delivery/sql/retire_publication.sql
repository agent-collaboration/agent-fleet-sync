DELETE FROM _fleet_sync_status_owed WHERE item_id = ?1 AND target = ?2 AND version = CAST(?3 AS INTEGER);

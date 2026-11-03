DELETE FROM _fleet_sync_status_events WHERE submitter_kind=?2 AND submitter_id=?3 AND cursor <= CAST(?1 AS INTEGER);

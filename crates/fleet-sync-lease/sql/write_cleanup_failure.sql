UPDATE _fleet_sync_transfers SET cleanup_failed=?1 WHERE attempt=?2 AND cleanup_failed IS NULL AND outcome IS NULL

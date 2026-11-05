UPDATE _fleet_sync_transfers SET new_epoch=NULLIF(?1,'null'),updated_at=CAST(?3 AS INTEGER) WHERE attempt=?2 AND outcome_kind IS NULL

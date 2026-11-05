SELECT attempt FROM _fleet_sync_transfers WHERE database=?1 AND new_epoch=NULLIF(?2,'null') LIMIT 2

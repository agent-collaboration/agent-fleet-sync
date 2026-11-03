SELECT attempt,cancelled_by,reason,CAST(cancelled_at AS TEXT) AS cancelled_at FROM _fleet_sync_transfer_cancels WHERE attempt=?1

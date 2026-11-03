SELECT database,epoch,holder,CAST(granted_at AS TEXT) AS granted_at,ended_epoch,ended_sequence FROM _fleet_sync_epoch_history WHERE database=?1 AND epoch=?2

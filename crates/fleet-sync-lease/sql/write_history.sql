INSERT INTO _fleet_sync_epoch_history(database,epoch,record) VALUES(?1,?2,?3) ON CONFLICT(database,epoch) DO UPDATE SET record=excluded.record

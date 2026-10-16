INSERT INTO _fleet_sync_epoch_floor(database,epoch) VALUES(?1,?2) ON CONFLICT(database) DO UPDATE SET epoch=excluded.epoch

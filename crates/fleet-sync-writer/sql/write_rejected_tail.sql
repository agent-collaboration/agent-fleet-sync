INSERT INTO _fleet_sync_rejected_tails(epoch,record) VALUES(?,?) ON CONFLICT(epoch) DO NOTHING

INSERT INTO _fleet_sync_copies(copy_id,record) VALUES(?,?) ON CONFLICT(copy_id) DO UPDATE SET record=excluded.record

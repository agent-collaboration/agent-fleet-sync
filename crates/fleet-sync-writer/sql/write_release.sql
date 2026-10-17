INSERT INTO _fleet_sync_release(database_id, call_id, record) VALUES(?, ?, ?)
ON CONFLICT(call_id) DO UPDATE SET record=excluded.record

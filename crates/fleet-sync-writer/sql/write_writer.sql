INSERT INTO _fleet_sync_writer(singleton, record) VALUES(1, ?)
ON CONFLICT(singleton) DO UPDATE SET record=excluded.record

INSERT INTO _fleet_sync_copy(singleton, record) VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET record = excluded.record

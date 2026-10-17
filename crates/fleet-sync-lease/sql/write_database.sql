INSERT INTO _fleet_sync_databases(database,record) VALUES(?1,?2) ON CONFLICT(database) DO UPDATE SET record=excluded.record

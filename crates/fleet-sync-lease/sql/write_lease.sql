INSERT INTO _fleet_sync_leases(database,record) VALUES(?1,?2) ON CONFLICT(database) DO UPDATE SET record=excluded.record

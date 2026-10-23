INSERT INTO _fleet_sync_schema_support(peer,record) VALUES(?1,?2) ON CONFLICT(peer) DO UPDATE SET record=excluded.record

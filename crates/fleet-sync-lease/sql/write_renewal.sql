INSERT INTO _fleet_sync_renewal_attempts(database,attempt,record) VALUES(?1,?2,?3) ON CONFLICT(database,attempt) DO NOTHING

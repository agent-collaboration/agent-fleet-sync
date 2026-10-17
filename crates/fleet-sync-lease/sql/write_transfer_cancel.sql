INSERT INTO _fleet_sync_transfer_cancels(attempt,record) VALUES(?1,?2) ON CONFLICT(attempt) DO NOTHING

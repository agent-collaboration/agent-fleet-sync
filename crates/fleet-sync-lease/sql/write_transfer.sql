INSERT INTO _fleet_sync_transfers(attempt,database,intent,outcome) VALUES(?1,?2,?3,?4) ON CONFLICT(attempt) DO UPDATE SET outcome=excluded.outcome

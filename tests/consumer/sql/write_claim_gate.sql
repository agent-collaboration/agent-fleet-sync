INSERT INTO claim_gate(attempt,record) VALUES(?1,?2) ON CONFLICT(attempt) DO UPDATE SET record=excluded.record

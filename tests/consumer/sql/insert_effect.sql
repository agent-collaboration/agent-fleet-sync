INSERT INTO effect_receipts(call_id,record) VALUES(?1,?2) ON CONFLICT(call_id) DO NOTHING

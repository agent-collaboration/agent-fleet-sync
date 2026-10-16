INSERT INTO _fleet_sync_sessions(peer, session_id, record) VALUES (?, ?, ?)
ON CONFLICT(peer, session_id) DO UPDATE SET record = excluded.record

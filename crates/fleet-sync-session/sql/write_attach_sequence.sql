INSERT INTO _fleet_sync_attach_sequences(peer,sequence) VALUES(?1,?2) ON CONFLICT(peer) DO UPDATE SET sequence=excluded.sequence

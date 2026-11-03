INSERT INTO _fleet_sync_status_state(machine_id,record) VALUES(?2,?1) ON CONFLICT(machine_id) DO UPDATE SET record=excluded.record;

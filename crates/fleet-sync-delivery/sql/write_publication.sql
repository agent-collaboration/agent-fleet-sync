INSERT INTO _fleet_sync_status_owed(item_id,target,version,record) VALUES(?1,?2,CAST(?3 AS INTEGER),?4) ON CONFLICT(item_id,target) DO UPDATE SET version=excluded.version,record=excluded.record;

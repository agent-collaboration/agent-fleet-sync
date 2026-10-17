INSERT INTO _fleet_sync_installs(install_id,target_key,record) VALUES(?,?,?)
ON CONFLICT(install_id) DO UPDATE SET record = excluded.record;

INSERT INTO _fleet_sync_install_current(target_key,install_id) VALUES(?,?)
ON CONFLICT(target_key) DO UPDATE SET install_id = excluded.install_id;

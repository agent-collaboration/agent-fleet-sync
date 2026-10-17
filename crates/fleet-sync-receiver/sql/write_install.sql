INSERT INTO _fleet_sync_install(singleton,record) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET record=excluded.record

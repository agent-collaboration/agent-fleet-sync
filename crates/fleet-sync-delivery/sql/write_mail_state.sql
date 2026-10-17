INSERT INTO _fleet_sync_mail_state(singleton,record) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET record=excluded.record;

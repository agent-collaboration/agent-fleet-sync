INSERT INTO client_session(singleton,record) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET record=excluded.record

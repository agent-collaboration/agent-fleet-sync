SELECT CAST(name AS TEXT) AS column_name,CAST(type AS TEXT) AS declared_type FROM pragma_table_xinfo('_fleet_sync_migrations') ORDER BY cid

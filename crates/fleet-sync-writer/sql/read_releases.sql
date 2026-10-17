SELECT CAST(database_id AS TEXT) AS database_id, typeof(database_id) AS database_class,
       CAST(call_id AS TEXT) AS call_id, typeof(call_id) AS call_class,
       CAST(record AS TEXT) AS record, typeof(record) AS record_class
FROM _fleet_sync_release
ORDER BY call_id

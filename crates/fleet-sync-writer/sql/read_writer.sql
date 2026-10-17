SELECT CAST(singleton AS TEXT) AS singleton, typeof(singleton) AS singleton_class,
       CAST(record AS TEXT) AS record, typeof(record) AS record_class
FROM _fleet_sync_writer

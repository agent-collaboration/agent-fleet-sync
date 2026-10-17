SELECT CAST(name AS TEXT) AS table_name, typeof(name) AS name_class,
       CAST(seq AS TEXT) AS sequence_value, typeof(seq) AS sequence_class
FROM sqlite_sequence ORDER BY name

SELECT CAST(type AS TEXT) AS object_kind, typeof(type) AS kind_class,
       CAST(name AS TEXT) AS object_name, typeof(name) AS name_class,
       CAST(tbl_name AS TEXT) AS table_name, typeof(tbl_name) AS table_class,
       CAST(sql AS TEXT) AS statement, typeof(sql) AS statement_class
FROM sqlite_schema
WHERE sql IS NOT NULL
ORDER BY type, name

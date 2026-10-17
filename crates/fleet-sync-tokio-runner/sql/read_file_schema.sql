SELECT CAST(type AS TEXT) AS object_kind, typeof(type) AS kind_class,
       CAST(name AS TEXT) AS object_name, typeof(name) AS name_class,
       CAST(tbl_name AS TEXT) AS owner_table, typeof(tbl_name) AS owner_class,
       CAST(sql AS TEXT) AS create_sql, typeof(sql) AS sql_class
FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY type,name

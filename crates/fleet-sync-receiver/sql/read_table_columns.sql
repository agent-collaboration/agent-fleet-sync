SELECT CAST(name AS TEXT) AS column_name, typeof(name) AS column_class,
       CAST(pk AS TEXT) AS key_order, typeof(pk) AS key_class,
       CAST(type AS TEXT) AS declared_type, typeof(type) AS type_class,
       CAST("notnull" AS TEXT) AS not_null, typeof("notnull") AS not_null_class
FROM pragma_table_info(?) ORDER BY cid

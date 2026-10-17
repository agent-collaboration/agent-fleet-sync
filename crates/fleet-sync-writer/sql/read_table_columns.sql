SELECT CAST(cid AS TEXT) AS column_index, typeof(cid) AS index_class,
       CAST(name AS TEXT) AS column_name, typeof(name) AS name_class,
       CAST(type AS TEXT) AS declared_type, typeof(type) AS type_class,
       CAST("notnull" AS TEXT) AS not_null, typeof("notnull") AS not_null_class,
       CAST(pk AS TEXT) AS key_order, typeof(pk) AS key_class
FROM pragma_table_info(?)
ORDER BY cid

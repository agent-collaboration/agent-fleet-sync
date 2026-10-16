SELECT CAST(name AS TEXT) AS column_name, typeof(name) AS column_class,
       CAST(pk AS TEXT) AS key_order, typeof(pk) AS key_class
FROM pragma_table_info(?)

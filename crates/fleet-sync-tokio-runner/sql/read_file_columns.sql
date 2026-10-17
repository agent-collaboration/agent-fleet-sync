SELECT CAST(cid AS TEXT) AS column_index, typeof(cid) AS index_class,
       CAST(name AS TEXT) AS column_name, typeof(name) AS name_class
FROM pragma_table_info(?1) ORDER BY cid

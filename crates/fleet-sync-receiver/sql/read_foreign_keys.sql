SELECT CAST(id AS TEXT) AS foreign_key_id, typeof(id) AS id_class,
       CAST(seq AS TEXT) AS column_order, typeof(seq) AS order_class,
       CAST("table" AS TEXT) AS parent_table, typeof("table") AS table_class,
       CAST("from" AS TEXT) AS child_column, typeof("from") AS child_class,
       CAST("to" AS TEXT) AS parent_column, typeof("to") AS parent_class,
       CAST(on_update AS TEXT) AS update_action, typeof(on_update) AS update_class,
       CAST(on_delete AS TEXT) AS delete_action, typeof(on_delete) AS delete_class
FROM pragma_foreign_key_list(?)
ORDER BY id, seq

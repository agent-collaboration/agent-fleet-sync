SELECT CAST(change_id AS TEXT) AS change_id, typeof(change_id) AS id_class,
       CAST(change_txn_id AS TEXT) AS transaction_id, typeof(change_txn_id) AS transaction_class,
       CAST(change_type AS TEXT) AS change_kind, typeof(change_type) AS kind_class,
       CAST(table_name AS TEXT) AS table_name, typeof(table_name) AS table_class,
       before AS before_image, after AS after_image
FROM turso_cdc WHERE change_id > ? ORDER BY change_id

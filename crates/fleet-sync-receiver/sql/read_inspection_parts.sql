SELECT CAST(transaction_key AS TEXT) AS transaction_key, typeof(transaction_key) AS key_class,
       CAST(part_index AS TEXT) AS part_index, typeof(part_index) AS index_class,
       CAST(staged_at AS TEXT) AS staged_at, typeof(staged_at) AS time_class
FROM _fleet_sync_staged_parts ORDER BY transaction_key, part_index

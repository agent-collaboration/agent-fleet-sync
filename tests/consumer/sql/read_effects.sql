SELECT CAST(record AS TEXT) AS record, typeof(record) AS record_class, CAST(attempts AS TEXT) AS attempts, typeof(attempts) AS attempts_class FROM effect_receipts ORDER BY call_id

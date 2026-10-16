SELECT typeof(amount) AS amount_class, CAST(amount AS TEXT) AS amount_text
FROM example WHERE record_id = ?

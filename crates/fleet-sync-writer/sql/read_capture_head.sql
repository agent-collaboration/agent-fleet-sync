SELECT CAST(max(change_id) AS TEXT) AS marker, typeof(max(change_id)) AS marker_class FROM turso_cdc WHERE change_type=2

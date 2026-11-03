SELECT CAST(CASE WHEN typeof(id)='text' THEN id END AS TEXT) AS id,typeof(cursor) AS cursor_class,CAST(cursor AS TEXT) AS cursor FROM _fleet_sync_outbox_status WHERE singleton=1;

SELECT CAST(count(*) AS TEXT) AS event_count, typeof(count(*)) AS count_class FROM hook_events

SELECT typeof(seq) AS seq_class, CAST(seq AS TEXT) AS seq, record FROM _fleet_sync_mail_controls WHERE seq > CAST(?1 AS INTEGER) ORDER BY seq LIMIT CAST(?2 AS INTEGER);

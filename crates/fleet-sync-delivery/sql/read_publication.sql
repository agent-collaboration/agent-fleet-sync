SELECT typeof(version) AS version_class,CAST(version AS TEXT) AS version,record FROM _fleet_sync_status_owed WHERE item_id = ?1 AND target = ?2;

SELECT item_id,target,typeof(version) AS version_class,CAST(version AS TEXT) AS version,record FROM _fleet_sync_status_owed ORDER BY item_id,target;

SELECT request_key AS "request_key!" FROM _fleet_sync_operations WHERE state = 'running' AND kind = ?1;

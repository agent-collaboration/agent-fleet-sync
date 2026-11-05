SELECT database,holder,epoch,CAST(expires_at AS TEXT) AS expires_at,CAST(last_renewed_at AS TEXT) AS last_renewed_at FROM _fleet_sync_leases WHERE database=?1

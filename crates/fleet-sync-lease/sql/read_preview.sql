SELECT token,database,fence,acked_epoch,acked_sequence,holder,CAST(last_seen AS TEXT) AS last_seen FROM _fleet_sync_takeover_previews WHERE token=?1

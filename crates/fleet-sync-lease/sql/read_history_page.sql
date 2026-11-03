SELECT CAST(CASE WHEN typeof(id)='text' THEN id END AS TEXT) AS id,database,epoch,holder,CAST(granted_at AS TEXT) AS granted_at,ended_epoch,ended_sequence FROM _fleet_sync_epoch_history
WHERE database=?1
AND (length(epoch)>length(?2) OR (length(epoch)=length(?2) AND epoch>?2))
AND (length(epoch)<length(?3) OR (length(epoch)=length(?3) AND epoch<?3))
ORDER BY length(epoch),epoch LIMIT CAST(?4 AS INTEGER)

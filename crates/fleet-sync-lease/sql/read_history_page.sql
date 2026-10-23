SELECT epoch,record FROM _fleet_sync_epoch_history
WHERE database=?1
AND (length(epoch)>length(?2) OR (length(epoch)=length(?2) AND epoch>?2))
AND (length(epoch)<length(?3) OR (length(epoch)=length(?3) AND epoch<?3))
ORDER BY length(epoch),epoch LIMIT CAST(?4 AS INTEGER)

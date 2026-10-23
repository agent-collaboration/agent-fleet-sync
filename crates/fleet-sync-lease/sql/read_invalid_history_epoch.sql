SELECT epoch FROM _fleet_sync_epoch_history WHERE database=?1
AND (typeof(epoch)<>'text' OR epoch='' OR epoch GLOB '*[^0-9]*'
OR substr(epoch,1,1)='0' OR length(epoch)>20
OR (length(epoch)=20 AND epoch>'18446744073709551615')) LIMIT 1

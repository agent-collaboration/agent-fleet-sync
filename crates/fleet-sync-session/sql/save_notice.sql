INSERT INTO _fleet_sync_notices(peer,session_id,position,notice_kind,database_id,consumer_body,created_at,updated_at)
VALUES(?1,?2,?3,json_extract(?4,'$.kind'),CASE WHEN json_extract(?4,'$.kind')='leaseChanged' THEN json_extract(?4,'$.value.database') END,CASE WHEN json_extract(?4,'$.kind')='consumer' THEN ?4 -> '$.value' END,CAST(?5 AS INTEGER),CAST(?6 AS INTEGER))

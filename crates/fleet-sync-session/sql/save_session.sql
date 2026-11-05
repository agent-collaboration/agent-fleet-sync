INSERT INTO _fleet_sync_sessions(peer,session_id,state_kind,connection_id,detached_since,end_reason,notice_head,created_at,updated_at)
VALUES(?1,?2,json_extract(?3,'$.state.kind'),json_extract(?3,'$.state.connection'),json_extract(?3,'$.state.since'),json_extract(?3,'$.state.reason'),?3 -> '$.noticeHead',CAST(?4 AS INTEGER),CAST(?5 AS INTEGER))
ON CONFLICT(peer,session_id) DO UPDATE SET state_kind=excluded.state_kind,connection_id=excluded.connection_id,detached_since=excluded.detached_since,end_reason=excluded.end_reason,notice_head=excluded.notice_head,updated_at=excluded.updated_at

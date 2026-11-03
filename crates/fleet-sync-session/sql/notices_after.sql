SELECT position,notice_kind,database_id,consumer_body FROM _fleet_sync_notices WHERE peer=?1 AND session_id=?2 ORDER BY length(position),position

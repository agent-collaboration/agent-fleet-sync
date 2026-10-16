SELECT position, body FROM _fleet_sync_notices WHERE peer = ? AND session_id = ?
ORDER BY length(position), position

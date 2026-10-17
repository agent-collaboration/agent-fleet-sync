CREATE TABLE _fleet_sync_sessions (
    peer TEXT NOT NULL,
    session_id TEXT NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY (peer, session_id)
);
CREATE TABLE _fleet_sync_notices (
    peer TEXT NOT NULL,
    session_id TEXT NOT NULL,
    position TEXT NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (peer, session_id, position)
);

CREATE TABLE _fleet_sync_writer (
    singleton INTEGER PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_copies (
    copy_id TEXT PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_capture_bookkeeping (
    commit_id TEXT PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_release (
    database_id TEXT NOT NULL,
    call_id TEXT PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_rejected_tails (
    epoch TEXT PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_schema_steps (
    version INTEGER PRIMARY KEY,
    statements TEXT NOT NULL,
    schema_layout TEXT NOT NULL
);

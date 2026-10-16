CREATE TABLE _fleet_sync_operations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    kind_version INTEGER NOT NULL,
    request_key TEXT NOT NULL,
    input BLOB,
    input_digest BLOB NOT NULL,
    state TEXT NOT NULL,
    result BLOB,
    failure TEXT,
    finished_at INTEGER,
    next_attempt_at INTEGER,
    attempt INTEGER NOT NULL,
    drive_token TEXT,
    drive_until INTEGER,
    UNIQUE(kind, request_key)
);
CREATE INDEX _fleet_sync_operations_due ON _fleet_sync_operations(state, next_attempt_at);
CREATE TABLE _fleet_sync_steps (
    operation_id TEXT NOT NULL REFERENCES _fleet_sync_operations(operation_id),
    position INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    intent TEXT,
    output BLOB,
    first_attempt_at INTEGER,
    may_have_reached INTEGER CHECK (may_have_reached IN (0, 1)),
    PRIMARY KEY(operation_id, position),
    UNIQUE(operation_id, step_name)
);
CREATE TABLE _fleet_sync_schema (version INTEGER PRIMARY KEY NOT NULL);
INSERT INTO _fleet_sync_schema(version) VALUES (1);

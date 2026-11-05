-- Preserve operation identities, serialized task bytes and call histories.
ALTER TABLE _fleet_sync_operations ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE _fleet_sync_operations ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0;
UPDATE _fleet_sync_operations SET created_at = coalesce(
    (SELECT min(first_attempt_at) FROM _fleet_sync_steps WHERE operation_id = _fleet_sync_operations.operation_id),
    finished_at, next_attempt_at, CAST(unixepoch('subsec')*1000 AS INTEGER)
);
UPDATE _fleet_sync_operations SET updated_at = max(created_at, CAST(unixepoch('subsec')*1000 AS INTEGER));

CREATE TABLE _fleet_sync_steps_metadata (
    id TEXT PRIMARY KEY NOT NULL DEFAULT (printf('%08x-%04x-7%03x-%04x-%012x',CAST(unixepoch('subsec')*1000 AS INTEGER)>>16,CAST(unixepoch('subsec')*1000 AS INTEGER)&65535,random()&4095,(random()&16383)|32768,random()&281474976710655)),
    operation_id TEXT NOT NULL REFERENCES _fleet_sync_operations(operation_id),
    position INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    intent TEXT,
    output BLOB,
    first_attempt_at INTEGER,
    may_have_reached INTEGER CHECK (may_have_reached IN (0, 1)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(operation_id, position),
    UNIQUE(operation_id, step_name)
);
INSERT INTO _fleet_sync_steps_metadata(operation_id,position,step_name,kind,intent,output,first_attempt_at,may_have_reached,created_at,updated_at)
SELECT steps.operation_id,position,step_name,steps.kind,intent,output,first_attempt_at,may_have_reached,
       coalesce(first_attempt_at,operations.created_at,CAST(unixepoch('subsec')*1000 AS INTEGER)),
       max(coalesce(first_attempt_at,operations.created_at,CAST(unixepoch('subsec')*1000 AS INTEGER)),CAST(unixepoch('subsec')*1000 AS INTEGER))
FROM _fleet_sync_steps AS steps
LEFT JOIN _fleet_sync_operations AS operations ON operations.operation_id = steps.operation_id;
DROP TABLE _fleet_sync_steps;
ALTER TABLE _fleet_sync_steps_metadata RENAME TO _fleet_sync_steps;

CREATE TABLE _fleet_sync_schema_metadata (
    id TEXT PRIMARY KEY NOT NULL DEFAULT (printf('%08x-%04x-7%03x-%04x-%012x',CAST(unixepoch('subsec')*1000 AS INTEGER)>>16,CAST(unixepoch('subsec')*1000 AS INTEGER)&65535,random()&4095,(random()&16383)|32768,random()&281474976710655)),
    version INTEGER NOT NULL UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER))
);
INSERT INTO _fleet_sync_schema_metadata(version) SELECT version + 1 FROM _fleet_sync_schema;
DROP TABLE _fleet_sync_schema;
ALTER TABLE _fleet_sync_schema_metadata RENAME TO _fleet_sync_schema;

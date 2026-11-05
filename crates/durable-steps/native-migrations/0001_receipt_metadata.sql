-- Native SQLx receipts keep their public SQLx columns and version uniqueness.
-- The DO bootstrap owns no migration receipts and never executes this file.
CREATE TABLE _fleet_sync_migrations_metadata (
    id TEXT PRIMARY KEY NOT NULL DEFAULT (printf('%08x-%04x-7%03x-%04x-%012x',CAST(unixepoch('subsec')*1000 AS INTEGER)>>16,CAST(unixepoch('subsec')*1000 AS INTEGER)&65535,random()&4095,(random()&16383)|32768,random()&281474976710655)),
    version BIGINT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL CHECK (success IN (0, 1)),
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER))
);
INSERT INTO _fleet_sync_migrations_metadata(version,description,installed_on,success,checksum,execution_time,created_at,updated_at)
SELECT version,description,installed_on,success,checksum,execution_time,
       coalesce(CAST(strftime('%s',installed_on) AS INTEGER)*1000,CAST(unixepoch('subsec')*1000 AS INTEGER)),
       max(coalesce(CAST(strftime('%s',installed_on) AS INTEGER)*1000,CAST(unixepoch('subsec')*1000 AS INTEGER)),CAST(unixepoch('subsec')*1000 AS INTEGER))
FROM _fleet_sync_migrations;
DROP TABLE _fleet_sync_migrations;
ALTER TABLE _fleet_sync_migrations_metadata RENAME TO _fleet_sync_migrations;

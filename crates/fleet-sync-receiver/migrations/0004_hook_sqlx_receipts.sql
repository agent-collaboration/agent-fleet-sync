ALTER TABLE _fleet_sync_hook_migrations RENAME TO _fleet_sync_legacy_hook_migrations;
CREATE TABLE _fleet_sync_hook_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);

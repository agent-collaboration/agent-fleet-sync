CREATE TABLE _fleet_sync_standard_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
INSERT INTO _fleet_sync_standard_migrations(version,description,installed_on,success,checksum,execution_time)
SELECT version,description,installed_on,success,checksum,execution_time FROM _fleet_sync_migrations;
DROP TABLE _fleet_sync_migrations;
ALTER TABLE _fleet_sync_standard_migrations RENAME TO _fleet_sync_migrations;

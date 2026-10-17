-- Description-only shape of the immutable receiver receipts carried until the
-- writer's ready transaction. Runtime creation belongs to receiver migrations.
CREATE TABLE _fleet_sync_install(singleton INTEGER PRIMARY KEY, record TEXT NOT NULL);
CREATE TABLE _fleet_sync_install_pages(install_id TEXT NOT NULL, page INTEGER NOT NULL, record TEXT NOT NULL, PRIMARY KEY(install_id,page));
CREATE TABLE _fleet_sync_copy_tables(name TEXT PRIMARY KEY);
CREATE TABLE _fleet_sync_copy_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);

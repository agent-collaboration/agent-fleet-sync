CREATE TABLE _fleet_sync_schema_support (
    peer TEXT NOT NULL PRIMARY KEY,
    record TEXT NOT NULL
);
ALTER TABLE _fleet_sync_transfer_admissions ADD COLUMN target_schema TEXT NOT NULL DEFAULT 'null';

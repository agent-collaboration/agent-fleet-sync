CREATE TABLE _fleet_sync_databases (
    database TEXT NOT NULL PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_leases (
    database TEXT NOT NULL PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_epoch_floor (
    database TEXT NOT NULL PRIMARY KEY,
    epoch TEXT NOT NULL
);
CREATE TABLE _fleet_sync_renewal_attempts (
    database TEXT NOT NULL,
    attempt TEXT NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY(database,attempt)
);
CREATE TABLE _fleet_sync_epoch_history (
    database TEXT NOT NULL,
    epoch TEXT NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY(database,epoch)
);
CREATE TABLE _fleet_sync_transfers (
    attempt TEXT NOT NULL PRIMARY KEY,
    database TEXT NOT NULL,
    intent TEXT NOT NULL,
    outcome TEXT
);
CREATE UNIQUE INDEX _fleet_sync_one_transfer ON _fleet_sync_transfers(database) WHERE outcome IS NULL;
CREATE TABLE _fleet_sync_takeover_previews (
    token TEXT NOT NULL PRIMARY KEY,
    database TEXT NOT NULL,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_lifecycle_attempts (
    attempt TEXT NOT NULL PRIMARY KEY,
    database TEXT NOT NULL,
    intent TEXT NOT NULL,
    outcome TEXT
);
CREATE UNIQUE INDEX _fleet_sync_one_lifecycle_attempt ON _fleet_sync_lifecycle_attempts(database) WHERE outcome IS NULL;

ALTER TABLE _fleet_sync_transfers ADD COLUMN phase TEXT;
ALTER TABLE _fleet_sync_transfers ADD COLUMN phase_history TEXT;
CREATE TABLE _fleet_sync_transfer_admissions (
    attempt TEXT NOT NULL PRIMARY KEY,
    operation TEXT NOT NULL UNIQUE,
    request TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    admitted_at TEXT NOT NULL,
    follow_up TEXT
);
CREATE TABLE _fleet_sync_transfer_cancels (
    attempt TEXT NOT NULL PRIMARY KEY,
    record TEXT NOT NULL
);

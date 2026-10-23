CREATE TABLE _fleet_sync_copy (
    singleton INTEGER PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_receipts (
    identity TEXT NOT NULL PRIMARY KEY,
    digest TEXT NOT NULL
);
CREATE TABLE _fleet_sync_staged_parts (
    transaction_key TEXT NOT NULL,
    part_index INTEGER NOT NULL,
    staged_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(transaction_key, part_index)
);
CREATE TABLE _fleet_sync_fence_receipts (
    transfer TEXT NOT NULL PRIMARY KEY,
    request TEXT NOT NULL,
    old_epoch TEXT NOT NULL UNIQUE,
    old_holder TEXT,
    result TEXT NOT NULL
);
CREATE TABLE _fleet_sync_schema_steps (
    version TEXT NOT NULL PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_seed (
    singleton INTEGER PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_seed_pages (
    seed TEXT NOT NULL,
    page INTEGER NOT NULL,
    digest TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(seed, page)
);
CREATE TABLE _fleet_sync_seed_attempts (
    attempt TEXT NOT NULL PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_creation (singleton INTEGER PRIMARY KEY, request TEXT NOT NULL);

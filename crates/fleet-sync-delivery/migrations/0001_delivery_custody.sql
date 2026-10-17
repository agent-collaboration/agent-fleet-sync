CREATE TABLE _fleet_sync_mail_items (
    item_id TEXT PRIMARY KEY,
    stream TEXT,
    seq INTEGER,
    live INTEGER NOT NULL CHECK (live IN (0, 1)),
    capacity_slot INTEGER NOT NULL CHECK (capacity_slot IN (0, 1)),
    payload_bytes INTEGER NOT NULL,
    settlement_bytes INTEGER NOT NULL,
    record TEXT NOT NULL,
    UNIQUE(stream, seq)
);
CREATE TABLE _fleet_sync_mail_controls (
    seq INTEGER PRIMARY KEY,
    record TEXT NOT NULL
);
CREATE TABLE _fleet_sync_mail_state (singleton INTEGER PRIMARY KEY, record TEXT NOT NULL);
CREATE TABLE _fleet_sync_outbox_items (item_id TEXT PRIMARY KEY, capacity_slot INTEGER NOT NULL CHECK (capacity_slot IN (0, 1)), live INTEGER NOT NULL CHECK (live IN (0, 1)), payload_bytes INTEGER NOT NULL, settlement_bytes INTEGER NOT NULL, record TEXT NOT NULL, payload TEXT);
CREATE TABLE _fleet_sync_inbox_items (item_id TEXT PRIMARY KEY, live INTEGER NOT NULL CHECK (live IN (0, 1)), payload_bytes INTEGER NOT NULL, settlement_bytes INTEGER NOT NULL, record TEXT NOT NULL, payload TEXT);
CREATE TABLE _fleet_sync_inbox_state (singleton INTEGER PRIMARY KEY, record TEXT NOT NULL);
CREATE TABLE _fleet_sync_reports (item_id TEXT NOT NULL, report_seq INTEGER NOT NULL, record TEXT NOT NULL, PRIMARY KEY(item_id, report_seq));
CREATE TABLE _fleet_sync_status_owed (item_id TEXT NOT NULL, target TEXT NOT NULL, version INTEGER NOT NULL, record TEXT NOT NULL, PRIMARY KEY(item_id, target));
CREATE TABLE _fleet_sync_status_events (cursor INTEGER PRIMARY KEY, item_id TEXT NOT NULL, version INTEGER NOT NULL, record TEXT NOT NULL, UNIQUE(item_id, version));
CREATE TABLE _fleet_sync_status_state (singleton INTEGER PRIMARY KEY, record TEXT NOT NULL);
CREATE TABLE _fleet_sync_delivery_receipts (call_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL, answer TEXT NOT NULL);
CREATE TABLE _fleet_sync_mail_cancel_links (target TEXT PRIMARY KEY, cancel_item TEXT NOT NULL UNIQUE);
CREATE TABLE _fleet_sync_outbox_cancel_links (target TEXT PRIMARY KEY, cancel_item TEXT NOT NULL UNIQUE);
CREATE TABLE _fleet_sync_outbox_status (singleton INTEGER PRIMARY KEY, record TEXT NOT NULL);
CREATE TABLE _fleet_sync_inbox_cancel_links (target TEXT PRIMARY KEY, cancel_item TEXT NOT NULL UNIQUE);

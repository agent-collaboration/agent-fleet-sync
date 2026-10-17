-- Description-only schema for the pinned engine's native CDC table. Runtime
-- tests create this table with capture_data_changes_conn, never this fixture.
CREATE TABLE turso_cdc (
    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
    change_time INTEGER,
    change_txn_id INTEGER,
    change_type INTEGER,
    table_name TEXT,
    id,
    before BLOB,
    after BLOB,
    updates BLOB
);

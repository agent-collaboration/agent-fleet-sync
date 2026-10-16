CREATE TABLE exact_values (
    id INTEGER PRIMARY KEY,
    real_value REAL,
    text_value TEXT,
    blob_value BLOB,
    null_value TEXT
);
CREATE TABLE composite_keys (
    second_key INTEGER,
    body TEXT,
    first_key TEXT,
    PRIMARY KEY(first_key, second_key)
);
CREATE TABLE "odd""table" (
    "odd""key" TEXT PRIMARY KEY,
    body TEXT
);

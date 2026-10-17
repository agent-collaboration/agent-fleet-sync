CREATE TABLE exact_values (
    id INTEGER PRIMARY KEY,
    real_value REAL,
    text_value TEXT,
    blob_value BLOB,
    null_value TEXT
);
CREATE TABLE composite_keys (
    second_key TEXT NOT NULL,
    body TEXT,
    first_key TEXT NOT NULL,
    PRIMARY KEY(first_key, second_key)
);
CREATE TABLE "odd""table" (
    "odd""key" TEXT NOT NULL PRIMARY KEY,
    body TEXT
);

CREATE TABLE real_values (id TEXT NOT NULL PRIMARY KEY,real_value REAL,text_value TEXT,blob_value BLOB,null_value TEXT);

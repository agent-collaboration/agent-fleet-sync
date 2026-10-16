CREATE TABLE text_records(id TEXT NOT NULL PRIMARY KEY, amount INTEGER, label TEXT);
CREATE TABLE composite_records(account TEXT NOT NULL, sequence INTEGER NOT NULL, payload BLOB, PRIMARY KEY(account,sequence));
CREATE TABLE integer_records(id INTEGER PRIMARY KEY, ratio REAL, absent TEXT);

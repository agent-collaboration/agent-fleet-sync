CREATE TABLE text_records(id TEXT PRIMARY KEY, amount INTEGER, label TEXT);
CREATE TABLE composite_records(account TEXT, sequence INTEGER, payload BLOB, PRIMARY KEY(account,sequence));
CREATE TABLE integer_records(id INTEGER PRIMARY KEY, ratio REAL, absent TEXT);

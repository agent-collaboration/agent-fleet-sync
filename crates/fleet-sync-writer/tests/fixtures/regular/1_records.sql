CREATE TABLE text_records(id TEXT NOT NULL PRIMARY KEY, amount INTEGER, label TEXT);
CREATE TABLE composite_records(account TEXT NOT NULL, sequence TEXT NOT NULL, payload BLOB, PRIMARY KEY(account,sequence));
CREATE TABLE real_records(id TEXT NOT NULL PRIMARY KEY, ratio REAL, absent TEXT);

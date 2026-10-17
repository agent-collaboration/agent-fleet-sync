CREATE TABLE samples(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL, exact_value INTEGER, payload BLOB);
CREATE TABLE readings(id INTEGER PRIMARY KEY, measured REAL, absent TEXT);
CREATE TABLE samples_by_source(source TEXT NOT NULL, sequence INTEGER NOT NULL, sample_id TEXT REFERENCES samples(id), PRIMARY KEY(source,sequence));

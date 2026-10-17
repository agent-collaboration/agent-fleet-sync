CREATE TABLE samples(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL, exact_value INTEGER, payload BLOB);
CREATE TABLE readings(id TEXT NOT NULL PRIMARY KEY, measured REAL, absent TEXT);
CREATE TABLE samples_by_source(source_id TEXT NOT NULL, event_id TEXT NOT NULL, sample_id TEXT REFERENCES samples(id), PRIMARY KEY(source_id,event_id));

CREATE TABLE IF NOT EXISTS source_records (source_id INTEGER PRIMARY KEY, source_name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dependent_records (request_name TEXT PRIMARY KEY, source_name TEXT NOT NULL);
INSERT OR IGNORE INTO source_records VALUES (1, 'ready');
CREATE TABLE IF NOT EXISTS numeric_samples (sample_name TEXT PRIMARY KEY, sample_value);

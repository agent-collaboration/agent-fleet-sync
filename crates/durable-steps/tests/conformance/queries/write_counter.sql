INSERT INTO conformance_counters(name, value) VALUES (?, ?)
ON CONFLICT(name) DO UPDATE SET value = excluded.value

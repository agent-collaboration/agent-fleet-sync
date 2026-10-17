CREATE TABLE hook_events(event_id TEXT NOT NULL PRIMARY KEY, annotation TEXT NOT NULL);
CREATE INDEX hook_annotation ON hook_events(annotation);

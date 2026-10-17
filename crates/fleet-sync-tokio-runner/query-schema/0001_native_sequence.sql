-- Materialize the pinned engine's actual sqlite_sequence for description.
-- This table exists only in the query generator's scratch database.
CREATE TABLE _fleet_sync_sequence_description(id INTEGER PRIMARY KEY AUTOINCREMENT);

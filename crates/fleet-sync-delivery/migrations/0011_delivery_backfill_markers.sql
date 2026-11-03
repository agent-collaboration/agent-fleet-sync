-- Startup owns both backfills in the same transaction as its application state.
CREATE TABLE _fleet_sync_delivery_backfills (
    id TEXT NOT NULL PRIMARY KEY DEFAULT (printf('%08x-%04x-7%03x-%04x-%012x',CAST(unixepoch('subsec')*1000 AS INTEGER)>>16,CAST(unixepoch('subsec')*1000 AS INTEGER)&65535,random()&4095,(random()&16383)|32768,random()&281474976710655)),
    name TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

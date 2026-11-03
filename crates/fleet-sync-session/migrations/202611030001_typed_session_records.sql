-- Backfill from the detached time when recorded; otherwise no historical row time exists.
-- Session state and library notice tags are typed; only opaque consumer notices remain JSON.
ALTER TABLE _fleet_sync_sessions RENAME TO _fleet_sync_sessions_legacy;
CREATE TABLE _fleet_sync_sessions (
 id TEXT NOT NULL PRIMARY KEY DEFAULT (printf('%08x-%04x-7%03x-%04x-%012x',CAST(unixepoch('subsec')*1000 AS INTEGER)>>16,CAST(unixepoch('subsec')*1000 AS INTEGER)&65535,random()&4095,(random()&16383)|32768,random()&281474976710655)),peer TEXT NOT NULL,session_id TEXT NOT NULL,
 state_kind TEXT NOT NULL,connection_id TEXT,detached_since INTEGER,end_reason TEXT,notice_head TEXT NOT NULL,
 created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)),UNIQUE(peer,session_id)
);
INSERT INTO _fleet_sync_sessions(peer,session_id,state_kind,connection_id,detached_since,end_reason,notice_head,created_at,updated_at)
 SELECT peer,session_id,json_extract(record,'$.state.kind'),json_extract(record,'$.state.connection'),json_extract(record,'$.state.since'),json_extract(record,'$.state.reason'),record -> '$.noticeHead',coalesce(json_extract(record,'$.state.since'),(CAST(unixepoch('subsec')*1000 AS INTEGER))),coalesce(json_extract(record,'$.state.since'),(CAST(unixepoch('subsec')*1000 AS INTEGER)))
 FROM _fleet_sync_sessions_legacy;
DROP TABLE _fleet_sync_sessions_legacy;
ALTER TABLE _fleet_sync_notices RENAME TO _fleet_sync_notices_legacy;
CREATE TABLE _fleet_sync_notices (
 id TEXT NOT NULL PRIMARY KEY DEFAULT (printf('%08x-%04x-7%03x-%04x-%012x',CAST(unixepoch('subsec')*1000 AS INTEGER)>>16,CAST(unixepoch('subsec')*1000 AS INTEGER)&65535,random()&4095,(random()&16383)|32768,random()&281474976710655)),peer TEXT NOT NULL,session_id TEXT NOT NULL,position TEXT NOT NULL,
 notice_kind TEXT NOT NULL,database_id TEXT,consumer_body TEXT,created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)),UNIQUE(peer,session_id,position)
);
INSERT INTO _fleet_sync_notices(peer,session_id,position,notice_kind,database_id,consumer_body)
 SELECT peer,session_id,position,json_extract(body,'$.kind'),CASE WHEN json_extract(body,'$.kind')='leaseChanged' THEN json_extract(body,'$.value.database') END,CASE WHEN json_extract(body,'$.kind')='consumer' THEN body -> '$.value' END
 FROM _fleet_sync_notices_legacy;
DROP TABLE _fleet_sync_notices_legacy;
ALTER TABLE _fleet_sync_attach_sequences RENAME TO _fleet_sync_attach_sequences_legacy;
CREATE TABLE _fleet_sync_attach_sequences(peer TEXT NOT NULL PRIMARY KEY,sequence TEXT NOT NULL,created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000 AS INTEGER)));
INSERT INTO _fleet_sync_attach_sequences(peer,sequence) SELECT peer,sequence FROM _fleet_sync_attach_sequences_legacy;
DROP TABLE _fleet_sync_attach_sequences_legacy;

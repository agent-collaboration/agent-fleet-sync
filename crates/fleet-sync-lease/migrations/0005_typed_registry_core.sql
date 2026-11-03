-- Normalize registry facts; SQL CHECK is reserved for booleans. Full u64 stays decimal TEXT.
ALTER TABLE _fleet_sync_databases RENAME TO _fleet_sync_databases_legacy;
CREATE TABLE _fleet_sync_databases (
 database TEXT NOT NULL PRIMARY KEY,
 lifecycle_kind TEXT NOT NULL,
 origin_kind TEXT,
 origin_epoch TEXT,
 origin_sequence TEXT,
 import_digest TEXT,
 retiring_attempt TEXT,
 retired_at INTEGER,
 revision_epoch TEXT NOT NULL,
 revision_change TEXT NOT NULL,
 changed_at INTEGER NOT NULL,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_databases(database,lifecycle_kind,origin_kind,origin_epoch,origin_sequence,import_digest,retiring_attempt,retired_at,revision_epoch,revision_change,changed_at,created_at,updated_at) SELECT CASE WHEN database=json_extract(record,'$.database') THEN database END,json_extract(record,'$.lifecycle.state'),json_extract(record,'$.lifecycle.origin.kind'),NULLIF(record -> '$.lifecycle.origin.value.position.epoch','null'),NULLIF(record -> '$.lifecycle.origin.value.position.sequence','null'),coalesce(json_extract(record,'$.lifecycle.importedFrom.digest'),json_extract(record,'$.lifecycle.importDigest')),json_extract(record,'$.lifecycle.attempt'),json_extract(record,'$.lifecycle.at'),NULLIF(record -> '$.revision.epoch','null'),NULLIF(record -> '$.revision.change','null'),json_extract(record,'$.changed_at'),json_extract(record,'$.changed_at'),json_extract(record,'$.changed_at') FROM _fleet_sync_databases_legacy;
DROP TABLE _fleet_sync_databases_legacy;
ALTER TABLE _fleet_sync_leases RENAME TO _fleet_sync_leases_legacy;
CREATE TABLE _fleet_sync_leases (
 database TEXT NOT NULL PRIMARY KEY,
 holder TEXT NOT NULL,
 epoch TEXT NOT NULL,
 expires_at INTEGER NOT NULL,
 last_renewed_at INTEGER NOT NULL,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_leases(database,holder,epoch,expires_at,last_renewed_at,created_at,updated_at) SELECT database,json_extract(record,'$.holder'),NULLIF(record -> '$.epoch','null'),json_extract(record,'$.expires_at'),json_extract(record,'$.last_renewed_at'),coalesce((SELECT json_extract(h.record,'$.granted_at') FROM _fleet_sync_epoch_history AS h WHERE h.database=l.database AND h.epoch=(l.record -> '$.epoch')),json_extract(record,'$.last_renewed_at')),json_extract(record,'$.last_renewed_at') FROM _fleet_sync_leases_legacy AS l;
DROP TABLE _fleet_sync_leases_legacy;
ALTER TABLE _fleet_sync_epoch_floor RENAME TO _fleet_sync_epoch_floor_legacy;
CREATE TABLE _fleet_sync_epoch_floor (
 database TEXT NOT NULL PRIMARY KEY,
 epoch TEXT NOT NULL,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_epoch_floor(database,epoch,created_at,updated_at) SELECT database,epoch,(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) FROM _fleet_sync_epoch_floor_legacy;
DROP TABLE _fleet_sync_epoch_floor_legacy;
ALTER TABLE _fleet_sync_epoch_history RENAME TO _fleet_sync_epoch_history_legacy;
CREATE TABLE _fleet_sync_epoch_history (
 id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(printf('%08x-%04x-7%s-%s%s-%s',((CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) >> 16),((CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) & 65535),substr(hex(randomblob(2)),2,3),substr('89ab',(abs(random()%4)+1),1),substr(hex(randomblob(2)),2,3),hex(randomblob(6))))),
 database TEXT NOT NULL,
 epoch TEXT NOT NULL,
 holder TEXT NOT NULL,
 granted_at INTEGER NOT NULL,
 ended_epoch TEXT,
 ended_sequence TEXT,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 UNIQUE(database,epoch)
);
INSERT INTO _fleet_sync_epoch_history(database,epoch,holder,granted_at,ended_epoch,ended_sequence,created_at,updated_at) SELECT CASE WHEN database=json_extract(record,'$.database') AND epoch=(record -> '$.epoch') THEN database END,epoch,json_extract(record,'$.holder'),json_extract(record,'$.granted_at'),NULLIF(record -> '$.ended_at.epoch','null'),NULLIF(record -> '$.ended_at.sequence','null'),json_extract(record,'$.granted_at'),json_extract(record,'$.granted_at') FROM _fleet_sync_epoch_history_legacy;
DROP TABLE _fleet_sync_epoch_history_legacy;
ALTER TABLE _fleet_sync_takeover_previews RENAME TO _fleet_sync_takeover_previews_legacy;
CREATE TABLE _fleet_sync_takeover_previews (
 token TEXT NOT NULL PRIMARY KEY,
 database TEXT NOT NULL,
 fence TEXT NOT NULL,
 acked_epoch TEXT NOT NULL,
 acked_sequence TEXT NOT NULL,
 holder TEXT NOT NULL,
 last_seen INTEGER,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_takeover_previews(token,database,fence,acked_epoch,acked_sequence,holder,last_seen,created_at,updated_at) SELECT CASE WHEN token=json_extract(record,'$.preview.token') AND database=json_extract(record,'$.database') THEN token END,database,NULLIF(record -> '$.preview.fence','null'),NULLIF(record -> '$.preview.acked.epoch','null'),NULLIF(record -> '$.preview.acked.sequence','null'),json_extract(record,'$.preview.holder'),json_extract(record,'$.preview.lastSeen'),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) FROM _fleet_sync_takeover_previews_legacy;
DROP TABLE _fleet_sync_takeover_previews_legacy;
ALTER TABLE _fleet_sync_transfer_cancels RENAME TO _fleet_sync_transfer_cancels_legacy;
CREATE TABLE _fleet_sync_transfer_cancels (
 attempt TEXT NOT NULL PRIMARY KEY,
 cancelled_by TEXT NOT NULL,
 reason TEXT NOT NULL,
 cancelled_at INTEGER NOT NULL,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_transfer_cancels(attempt,cancelled_by,reason,cancelled_at,created_at,updated_at) SELECT attempt,json_extract(record,'$.by'),json_extract(record,'$.reason'),json_extract(record,'$.at'),json_extract(record,'$.at'),json_extract(record,'$.at') FROM _fleet_sync_transfer_cancels_legacy;
DROP TABLE _fleet_sync_transfer_cancels_legacy;
ALTER TABLE _fleet_sync_schema_support RENAME TO _fleet_sync_schema_support_legacy;
CREATE TABLE _fleet_sync_schema_support (
 peer TEXT NOT NULL PRIMARY KEY,
 session_id TEXT NOT NULL,
 attach_seq TEXT NOT NULL,
 max_version TEXT NOT NULL,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_schema_support(peer,session_id,attach_seq,max_version,created_at,updated_at) SELECT peer,json_extract(record,'$.sessionId'),NULLIF(record -> '$.attachSeq','null'),NULLIF(record -> '$.support.maxVersion','null'),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) FROM _fleet_sync_schema_support_legacy;
DROP TABLE _fleet_sync_schema_support_legacy;

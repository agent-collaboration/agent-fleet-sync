ALTER TABLE _fleet_sync_reports RENAME TO _fleet_sync_reports_json;
CREATE TABLE _fleet_sync_reports (item_id TEXT NOT NULL PRIMARY KEY,report_seq INTEGER NOT NULL,created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
    outcome_kind TEXT NOT NULL,
    outcome_acceptance TEXT,
    refusal_kind TEXT,
    refusal_code TEXT,
    refusal_failure TEXT,
    refusal_size_field TEXT,
    refusal_digest TEXT,
    refusal_item_id TEXT,
    outcome_cancel_item_id TEXT,
    outcome_too_late_at TEXT
);
INSERT INTO _fleet_sync_reports(item_id,report_seq,created_at,updated_at,outcome_kind,outcome_acceptance,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,outcome_cancel_item_id,outcome_too_late_at) SELECT item_id,report_seq,created_at,updated_at,json_extract(record,'$.outcome.kind'),CASE WHEN json_type(record,'$.outcome.value.acceptance') IS NOT NULL THEN record -> '$.outcome.value.acceptance' END,json_extract(record,'$.outcome.value.reason.kind'),json_extract(record,'$.outcome.value.reason.value.code'),json_extract(record,'$.outcome.value.reason.value.kind'),json_extract(record,'$.outcome.value.reason.value.field'),CASE WHEN json_extract(record,'$.outcome.value.reason.kind')='itemConflict' THEN json_extract(record,'$.outcome.value.reason.value.existing') ELSE json_extract(record,'$.outcome.value.reason.value.existing.digest') END,json_extract(record,'$.outcome.value.reason.value.existing.item'),json_extract(record,'$.outcome.value.cancel'),json_extract(record,'$.outcome.value.at') FROM _fleet_sync_reports_json;
DROP TABLE _fleet_sync_reports_json;
ALTER TABLE _fleet_sync_outbox_status RENAME TO _fleet_sync_outbox_status_json;
CREATE TABLE _fleet_sync_outbox_status(id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),1,8)||'-'||substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),9,4)||'-7'||substr(hex(randomblob(2)),2,3)||'-'||substr('89ab',(abs(random()%4)+1),1)||substr(hex(randomblob(2)),2,3)||'-'||hex(randomblob(6)))),singleton INTEGER NOT NULL UNIQUE,cursor INTEGER NOT NULL,created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)));
INSERT INTO _fleet_sync_outbox_status(id,singleton,cursor,created_at,updated_at) SELECT id,singleton,CAST(record AS INTEGER),created_at,updated_at FROM _fleet_sync_outbox_status_json;
DROP TABLE _fleet_sync_outbox_status_json;

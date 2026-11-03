-- Routes and acceptance values are consumer-defined; every library-owned field is scalar.
ALTER TABLE _fleet_sync_status_state RENAME TO _fleet_sync_status_state_json;
CREATE TABLE _fleet_sync_status_state (id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),1,8)||'-'||substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),9,4)||'-7'||substr(hex(randomblob(2)),2,3)||'-'||substr('89ab',(abs(random()%4)+1),1)||substr(hex(randomblob(2)),2,3)||'-'||hex(randomblob(6)))),
    created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),machine_id TEXT NOT NULL UNIQUE,owner_route TEXT NOT NULL,head INTEGER NOT NULL,floor INTEGER NOT NULL);
INSERT INTO _fleet_sync_status_state(machine_id,owner_route,head,floor) SELECT machine_id,record -> '$.ownerRoute',json_extract(record,'$.head'),json_extract(record,'$.floor') FROM _fleet_sync_status_state_json;
DROP TABLE _fleet_sync_status_state_json;
ALTER TABLE _fleet_sync_status_events RENAME TO _fleet_sync_status_events_json;
CREATE TABLE _fleet_sync_status_events (
    id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),1,8)||'-'||substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),9,4)||'-7'||substr(hex(randomblob(2)),2,3)||'-'||substr('89ab',(abs(random()%4)+1),1)||substr(hex(randomblob(2)),2,3)||'-'||hex(randomblob(6)))),
    created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
    machine_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    item_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    status_kind TEXT NOT NULL,
    status_acceptance TEXT,
    refusal_kind TEXT,
    refusal_code TEXT,
    refusal_failure TEXT,
    refusal_size_field TEXT,
    refusal_digest TEXT,
    refusal_item_id TEXT,
    cancel_outcome TEXT,
    cancel_too_late_at TEXT,
    UNIQUE(machine_id,cursor),UNIQUE(machine_id,item_id,version)
);
INSERT INTO _fleet_sync_status_events (machine_id,cursor,item_id,version,status_kind,status_acceptance,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,cancel_outcome,cancel_too_late_at)
SELECT machine_id,
       cursor,
       item_id,
       version,
       json_extract(record,'$.status.kind'),
       CASE WHEN json_type(record,'$.status.value.acceptance') IS NOT NULL THEN record -> '$.status.value.acceptance' END,
       json_extract(record,'$.status.value.reason.kind'),
       json_extract(record,'$.status.value.reason.value.code'),
       json_extract(record,'$.status.value.reason.value.kind'),
       json_extract(record,'$.status.value.reason.value.field'),
       CASE WHEN json_extract(record,'$.status.value.reason.kind')='itemConflict' THEN json_extract(record,'$.status.value.reason.value.existing') ELSE json_extract(record,'$.status.value.reason.value.existing.digest') END,
       json_extract(record,'$.status.value.reason.value.existing.item'),
       json_extract(record,'$.cancel.kind'),
       json_extract(record,'$.cancel.value.at') FROM _fleet_sync_status_events_json;
DROP TABLE _fleet_sync_status_events_json;
ALTER TABLE _fleet_sync_status_owed RENAME TO _fleet_sync_status_owed_json;
CREATE TABLE _fleet_sync_status_owed (
    id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),1,8)||'-'||substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),9,4)||'-7'||substr(hex(randomblob(2)),2,3)||'-'||substr('89ab',(abs(random()%4)+1),1)||substr(hex(randomblob(2)),2,3)||'-'||hex(randomblob(6)))),
    created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
    machine_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    status_route TEXT NOT NULL,
    version INTEGER NOT NULL,
    status_kind TEXT NOT NULL,
    status_acceptance TEXT,
    refusal_kind TEXT,
    refusal_code TEXT,
    refusal_failure TEXT,
    refusal_size_field TEXT,
    refusal_digest TEXT,
    refusal_item_id TEXT,
    cancel_outcome TEXT,
    cancel_too_late_at TEXT,
    UNIQUE(item_id,target_kind,machine_id,status_route)
);
INSERT INTO _fleet_sync_status_owed (machine_id,item_id,target_kind,status_route,version,status_kind,status_acceptance,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,cancel_outcome,cancel_too_late_at)
SELECT machine_id,
       item_id,
       json_extract(target,'$.kind'),
       target -> '$.value.route',
       version,
       json_extract(record,'$.status.kind'),
       CASE WHEN json_type(record,'$.status.value.acceptance') IS NOT NULL THEN record -> '$.status.value.acceptance' END,
       json_extract(record,'$.status.value.reason.kind'),
       json_extract(record,'$.status.value.reason.value.code'),
       json_extract(record,'$.status.value.reason.value.kind'),
       json_extract(record,'$.status.value.reason.value.field'),
       CASE WHEN json_extract(record,'$.status.value.reason.kind')='itemConflict' THEN json_extract(record,'$.status.value.reason.value.existing') ELSE json_extract(record,'$.status.value.reason.value.existing.digest') END,
       json_extract(record,'$.status.value.reason.value.existing.item'),
       json_extract(record,'$.cancel.kind'),
       json_extract(record,'$.cancel.value.at') FROM _fleet_sync_status_owed_json;
DROP TABLE _fleet_sync_status_owed_json;

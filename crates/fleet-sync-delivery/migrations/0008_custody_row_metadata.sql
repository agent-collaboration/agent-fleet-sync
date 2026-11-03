-- Keep existing UUIDv7 primary keys; timestamp every surviving custody table.
ALTER TABLE _fleet_sync_outbox_items RENAME TO _fleet_sync_outbox_items_metadata_upgrade;
CREATE TABLE _fleet_sync_outbox_items (
    item_id TEXT NOT NULL PRIMARY KEY,
    live INTEGER NOT NULL CHECK (live IN (0,1)),
    payload_bytes INTEGER NOT NULL,
    settlement_charge_bytes INTEGER NOT NULL,
    payload TEXT,
    capacity_slot INTEGER NOT NULL CHECK (capacity_slot IN (0,1)),
    record_kind TEXT NOT NULL,
    digest TEXT,
    relay_machine_id TEXT,
    submitter_machine_id TEXT,
    recipient_machine_id TEXT,
    valid_until INTEGER,
    status_kind TEXT,
    status_sequence INTEGER,
    status_acceptance TEXT,
    refusal_kind TEXT,
    refusal_code TEXT,
    refusal_failure TEXT,
    refusal_size_field TEXT,
    refusal_digest TEXT,
    refusal_item_id TEXT,
    status_version INTEGER,
    last_report_seq INTEGER NOT NULL,
    submit_attempted INTEGER CHECK (submit_attempted IN (0,1)),
    cancel_outcome TEXT,
    cancel_too_late_at TEXT,
    cancel_target_item_id TEXT,
    acceptance_bound INTEGER,
    settlement_bytes INTEGER,
    failure_kind TEXT,
    created_at INTEGER NOT NULL,
    admitted_at INTEGER,
    handed_off_at INTEGER,
    status_route TEXT,
    updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_outbox_items (item_id,live,payload_bytes,settlement_charge_bytes,payload,capacity_slot,record_kind,digest,relay_machine_id,submitter_machine_id,recipient_machine_id,valid_until,status_kind,status_sequence,status_acceptance,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,status_version,last_report_seq,submit_attempted,cancel_outcome,cancel_too_late_at,cancel_target_item_id,acceptance_bound,settlement_bytes,failure_kind,created_at,admitted_at,handed_off_at,status_route,updated_at) SELECT item_id,live,payload_bytes,settlement_charge_bytes,payload,capacity_slot,record_kind,digest,relay_machine_id,submitter_machine_id,recipient_machine_id,valid_until,status_kind,status_sequence,status_acceptance,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,status_version,last_report_seq,submit_attempted,cancel_outcome,cancel_too_late_at,cancel_target_item_id,acceptance_bound,settlement_bytes,failure_kind,created_at,admitted_at,handed_off_at,status_route,coalesce(handed_off_at,admitted_at,created_at) FROM _fleet_sync_outbox_items_metadata_upgrade;
DROP TABLE _fleet_sync_outbox_items_metadata_upgrade;
ALTER TABLE _fleet_sync_inbox_items RENAME TO _fleet_sync_inbox_items_metadata_upgrade;
CREATE TABLE _fleet_sync_inbox_items (
    item_id TEXT NOT NULL PRIMARY KEY,
    live INTEGER NOT NULL CHECK (live IN (0,1)),
    payload_bytes INTEGER NOT NULL,
    settlement_charge_bytes INTEGER NOT NULL,
    payload TEXT,
    digest TEXT NOT NULL,
    sender TEXT NOT NULL,
    recipient_machine_id TEXT NOT NULL,
    recipient_address TEXT NOT NULL,
    relay_machine_id TEXT,
    cancel_target_item_id TEXT,
    valid_until INTEGER,
    stream TEXT NOT NULL,
    route TEXT NOT NULL,
    progress TEXT NOT NULL,
    attempt INTEGER,
    not_before INTEGER,
    outcome_kind TEXT,
    outcome_acceptance TEXT,
    outcome_cancel_item_id TEXT,
    outcome_too_late_at TEXT,
    refusal_kind TEXT,
    refusal_code TEXT,
    refusal_failure TEXT,
    refusal_size_field TEXT,
    refusal_digest TEXT,
    refusal_item_id TEXT,
    report_seq INTEGER NOT NULL,
    acceptance_bound INTEGER,
    settlement_bytes INTEGER NOT NULL,
    release_requested INTEGER NOT NULL CHECK (release_requested IN (0,1)),
    created_at INTEGER NOT NULL,
    accepted_at INTEGER,
    delivered_at INTEGER,
    updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_inbox_items (item_id,live,payload_bytes,settlement_charge_bytes,payload,digest,sender,recipient_machine_id,recipient_address,relay_machine_id,cancel_target_item_id,valid_until,stream,route,progress,attempt,not_before,outcome_kind,outcome_acceptance,outcome_cancel_item_id,outcome_too_late_at,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,report_seq,acceptance_bound,settlement_bytes,release_requested,created_at,accepted_at,delivered_at,updated_at) SELECT item_id,live,payload_bytes,settlement_charge_bytes,payload,digest,sender,recipient_machine_id,recipient_address,relay_machine_id,cancel_target_item_id,valid_until,stream,route,progress,attempt,not_before,outcome_kind,outcome_acceptance,outcome_cancel_item_id,outcome_too_late_at,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,report_seq,acceptance_bound,settlement_bytes,release_requested,created_at,accepted_at,delivered_at,coalesce(delivered_at,accepted_at,created_at) FROM _fleet_sync_inbox_items_metadata_upgrade;
DROP TABLE _fleet_sync_inbox_items_metadata_upgrade;
CREATE INDEX _fleet_sync_inbox_accounting ON _fleet_sync_inbox_items(live,payload_bytes,settlement_charge_bytes);
ALTER TABLE _fleet_sync_outbox_cancel_links RENAME TO _fleet_sync_outbox_cancel_links_metadata_upgrade;
CREATE TABLE _fleet_sync_outbox_cancel_links (target TEXT NOT NULL PRIMARY KEY,cancel_item TEXT NOT NULL UNIQUE,created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)));
INSERT INTO _fleet_sync_outbox_cancel_links (target,cancel_item,created_at,updated_at) SELECT target,cancel_item,created_at,created_at FROM _fleet_sync_outbox_cancel_links_metadata_upgrade;
DROP TABLE _fleet_sync_outbox_cancel_links_metadata_upgrade;
ALTER TABLE _fleet_sync_inbox_cancel_links RENAME TO _fleet_sync_inbox_cancel_links_metadata_upgrade;
CREATE TABLE _fleet_sync_inbox_cancel_links (target TEXT NOT NULL PRIMARY KEY,cancel_item TEXT NOT NULL UNIQUE,created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)));
INSERT INTO _fleet_sync_inbox_cancel_links (target,cancel_item,created_at,updated_at) SELECT target,cancel_item,(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),1,1))-1)*17592186044416+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),2,1))-1)*1099511627776+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),3,1))-1)*68719476736+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),4,1))-1)*4294967296+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),5,1))-1)*268435456+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),6,1))-1)*16777216+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),7,1))-1)*1048576+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),8,1))-1)*65536+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),9,1))-1)*4096+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),10,1))-1)*256+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),11,1))-1)*16+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),12,1))-1)*1,(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),1,1))-1)*17592186044416+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),2,1))-1)*1099511627776+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),3,1))-1)*68719476736+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),4,1))-1)*4294967296+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),5,1))-1)*268435456+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),6,1))-1)*16777216+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),7,1))-1)*1048576+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),8,1))-1)*65536+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),9,1))-1)*4096+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),10,1))-1)*256+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),11,1))-1)*16+(instr('0123456789abcdef',substr(replace(cancel_item,'-',''),12,1))-1)*1 FROM _fleet_sync_inbox_cancel_links_metadata_upgrade;
DROP TABLE _fleet_sync_inbox_cancel_links_metadata_upgrade;
ALTER TABLE _fleet_sync_reports RENAME TO _fleet_sync_reports_metadata_upgrade;
CREATE TABLE _fleet_sync_reports (item_id TEXT NOT NULL PRIMARY KEY,report_seq INTEGER NOT NULL,record TEXT NOT NULL,created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)));
INSERT INTO _fleet_sync_reports (item_id,report_seq,record,created_at,updated_at) SELECT item_id,report_seq,record,(instr('0123456789abcdef',substr(replace(item_id,'-',''),1,1))-1)*17592186044416+(instr('0123456789abcdef',substr(replace(item_id,'-',''),2,1))-1)*1099511627776+(instr('0123456789abcdef',substr(replace(item_id,'-',''),3,1))-1)*68719476736+(instr('0123456789abcdef',substr(replace(item_id,'-',''),4,1))-1)*4294967296+(instr('0123456789abcdef',substr(replace(item_id,'-',''),5,1))-1)*268435456+(instr('0123456789abcdef',substr(replace(item_id,'-',''),6,1))-1)*16777216+(instr('0123456789abcdef',substr(replace(item_id,'-',''),7,1))-1)*1048576+(instr('0123456789abcdef',substr(replace(item_id,'-',''),8,1))-1)*65536+(instr('0123456789abcdef',substr(replace(item_id,'-',''),9,1))-1)*4096+(instr('0123456789abcdef',substr(replace(item_id,'-',''),10,1))-1)*256+(instr('0123456789abcdef',substr(replace(item_id,'-',''),11,1))-1)*16+(instr('0123456789abcdef',substr(replace(item_id,'-',''),12,1))-1)*1,(instr('0123456789abcdef',substr(replace(item_id,'-',''),1,1))-1)*17592186044416+(instr('0123456789abcdef',substr(replace(item_id,'-',''),2,1))-1)*1099511627776+(instr('0123456789abcdef',substr(replace(item_id,'-',''),3,1))-1)*68719476736+(instr('0123456789abcdef',substr(replace(item_id,'-',''),4,1))-1)*4294967296+(instr('0123456789abcdef',substr(replace(item_id,'-',''),5,1))-1)*268435456+(instr('0123456789abcdef',substr(replace(item_id,'-',''),6,1))-1)*16777216+(instr('0123456789abcdef',substr(replace(item_id,'-',''),7,1))-1)*1048576+(instr('0123456789abcdef',substr(replace(item_id,'-',''),8,1))-1)*65536+(instr('0123456789abcdef',substr(replace(item_id,'-',''),9,1))-1)*4096+(instr('0123456789abcdef',substr(replace(item_id,'-',''),10,1))-1)*256+(instr('0123456789abcdef',substr(replace(item_id,'-',''),11,1))-1)*16+(instr('0123456789abcdef',substr(replace(item_id,'-',''),12,1))-1)*1 FROM _fleet_sync_reports_metadata_upgrade;
DROP TABLE _fleet_sync_reports_metadata_upgrade;
ALTER TABLE _fleet_sync_inbox_state RENAME TO _fleet_sync_inbox_state_metadata_upgrade;
CREATE TABLE _fleet_sync_inbox_state (singleton INTEGER NOT NULL UNIQUE,record TEXT NOT NULL,id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),1,8)||'-'||substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),9,4)||'-7'||substr(hex(randomblob(2)),2,3)||'-'||substr('89ab',(abs(random()%4)+1),1)||substr(hex(randomblob(2)),2,3)||'-'||hex(randomblob(6)))),created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)));
INSERT INTO _fleet_sync_inbox_state (singleton,record,created_at,updated_at) SELECT singleton,record,(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) FROM _fleet_sync_inbox_state_metadata_upgrade;
DROP TABLE _fleet_sync_inbox_state_metadata_upgrade;
ALTER TABLE _fleet_sync_outbox_status RENAME TO _fleet_sync_outbox_status_metadata_upgrade;
CREATE TABLE _fleet_sync_outbox_status (singleton INTEGER NOT NULL UNIQUE,record TEXT NOT NULL,id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),1,8)||'-'||substr(printf('%012x',(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),9,4)||'-7'||substr(hex(randomblob(2)),2,3)||'-'||substr('89ab',(abs(random()%4)+1),1)||substr(hex(randomblob(2)),2,3)||'-'||hex(randomblob(6)))),created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)));
INSERT INTO _fleet_sync_outbox_status (singleton,record,created_at,updated_at) SELECT singleton,record,(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) FROM _fleet_sync_outbox_status_metadata_upgrade;
DROP TABLE _fleet_sync_outbox_status_metadata_upgrade;

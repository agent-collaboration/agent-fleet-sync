-- Normalize workflow receipts and bounded phase history; counters remain exact decimal text.
CREATE TABLE _fleet_sync_transfer_phase_history(id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(printf('%08x-%04x-7%s-%s%s-%s',((CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) >> 16),((CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) & 65535),substr(hex(randomblob(2)),2,3),substr('89ab',(abs(random()%4)+1),1),substr(hex(randomblob(2)),2,3),hex(randomblob(6))))),attempt TEXT NOT NULL,position INTEGER NOT NULL,phase_kind TEXT NOT NULL,final_epoch TEXT,final_sequence TEXT,entered_at INTEGER NOT NULL,created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),UNIQUE(attempt,position));
ALTER TABLE _fleet_sync_renewal_attempts RENAME TO _fleet_sync_renewal_attempts_legacy;
CREATE TABLE _fleet_sync_renewal_attempts(
 id TEXT NOT NULL PRIMARY KEY DEFAULT (lower(printf('%08x-%04x-7%s-%s%s-%s',((CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) >> 16),((CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) & 65535),substr(hex(randomblob(2)),2,3),substr('89ab',(abs(random()%4)+1),1),substr(hex(randomblob(2)),2,3),hex(randomblob(6))))),
 database TEXT NOT NULL,
 attempt TEXT NOT NULL,
 request_epoch TEXT NOT NULL,
 holder TEXT NOT NULL,
 answer_kind TEXT NOT NULL,
 recorded_epoch TEXT,
 recorded_expires_at INTEGER,
 current_database TEXT,
 current_holder TEXT,
 current_state TEXT,
 current_epoch TEXT,
 current_remaining_ms TEXT,
 current_expires_at INTEGER,
 transfer_in_progress TEXT,
 replayed INTEGER CHECK (replayed IN (0,1)),
 refusal_kind TEXT,
 refusal_current_epoch TEXT,
 refusal_ended_epoch TEXT,
 refusal_ended_sequence TEXT,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),
 UNIQUE(database,attempt)
);
INSERT INTO _fleet_sync_renewal_attempts(database,attempt,request_epoch,holder,answer_kind,recorded_epoch,recorded_expires_at,current_database,current_holder,current_state,current_epoch,current_remaining_ms,current_expires_at,transfer_in_progress,replayed,refusal_kind,refusal_current_epoch,refusal_ended_epoch,refusal_ended_sequence,created_at,updated_at) SELECT database,attempt,NULLIF(record -> '$.request.epoch','null'),json_extract(record,'$.holder'),json_extract(record,'$.answer.kind'),NULLIF(record -> '$.answer.value.recorded.epoch','null'),json_extract(record,'$.answer.value.recorded.expiresAt'),json_extract(record,'$.answer.value.current.database'),json_extract(record,'$.answer.value.current.holder'),json_extract(record,'$.answer.value.current.state'),NULLIF(record -> '$.answer.value.current.epoch','null'),NULLIF(record -> '$.answer.value.current.remainingMs','null'),json_extract(record,'$.answer.value.current.expiresAt'),json_extract(record,'$.answer.value.transferInProgress'),json_extract(record,'$.answer.value.replayed'),json_extract(record,'$.answer.value.kind'),coalesce(record -> '$.answer.value.value.currentEpoch',NULLIF(record -> '$.answer.value.value.current','null')),NULLIF(record -> '$.answer.value.value.epochEndedAt.epoch','null'),NULLIF(record -> '$.answer.value.value.epochEndedAt.sequence','null'),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) FROM _fleet_sync_renewal_attempts_legacy;
DROP TABLE _fleet_sync_renewal_attempts_legacy;
ALTER TABLE _fleet_sync_transfer_admissions RENAME TO _fleet_sync_transfer_admissions_legacy;
CREATE TABLE _fleet_sync_transfer_admissions(
 attempt TEXT NOT NULL PRIMARY KEY,
 operation TEXT NOT NULL UNIQUE,
 request_kind TEXT NOT NULL,
 database TEXT NOT NULL,
 from_epoch TEXT,
 to_peer TEXT,
 claimant TEXT,
 reason TEXT,
 preview_token TEXT,
 preview_holder TEXT,
 preview_fence TEXT,
 preview_acked_epoch TEXT,
 preview_acked_sequence TEXT,
 preview_last_seen INTEGER,
 requested_by TEXT NOT NULL,
 admitted_at INTEGER NOT NULL,
 target_schema_max_version TEXT,
 follow_up TEXT,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_transfer_admissions(attempt,operation,request_kind,database,from_epoch,to_peer,claimant,reason,preview_token,preview_holder,preview_fence,preview_acked_epoch,preview_acked_sequence,preview_last_seen,requested_by,admitted_at,target_schema_max_version,follow_up,created_at,updated_at) SELECT attempt,json_extract(operation,'$'),json_extract(request,'$.kind'),json_extract(request,'$.value.database'),NULLIF(request -> '$.value.fromEpoch','null'),json_extract(request,'$.value.to'),json_extract(request,'$.value.claimant'),json_extract(request,'$.value.reason'),json_extract(request,'$.value.preview.token'),json_extract(request,'$.value.preview.holder'),NULLIF(request -> '$.value.preview.fence','null'),NULLIF(request -> '$.value.preview.acked.epoch','null'),NULLIF(request -> '$.value.preview.acked.sequence','null'),json_extract(request,'$.value.preview.lastSeen'),json_extract(requested_by,'$'),json_extract(admitted_at,'$'),NULLIF(target_schema -> '$.maxVersion','null'),json_extract(follow_up,'$'),json_extract(admitted_at,'$'),json_extract(admitted_at,'$') FROM _fleet_sync_transfer_admissions_legacy;
DROP TABLE _fleet_sync_transfer_admissions_legacy;
ALTER TABLE _fleet_sync_transfers RENAME TO _fleet_sync_transfers_legacy;
CREATE TABLE _fleet_sync_transfers(
 attempt TEXT NOT NULL PRIMARY KEY,
 database TEXT NOT NULL,
 new_epoch TEXT,
 outcome_kind TEXT,
 cancelled_by TEXT,
 granted_epoch TEXT,
 granted_start_epoch TEXT,
 granted_start_sequence TEXT,
 granted_expires_at INTEGER,
 pending_transfer TEXT,
 failure_reason TEXT,
 refusal_kind TEXT,
 refusal_last_renewed_at INTEGER,
 refusal_last_seen INTEGER,
 refusal_gate_code TEXT,
 refusal_gate_detail TEXT,
 refusal_transfer TEXT,
 refusal_migration_state TEXT,
 refusal_store_fingerprint TEXT,
 refusal_holder TEXT,
 refusal_superseded_by TEXT,
 refusal_migration TEXT,
 refusal_store_version TEXT,
 refusal_supported_version TEXT,
 phase_kind TEXT NOT NULL,
 phase_final_epoch TEXT,
 phase_final_sequence TEXT,
 cleanup_failure_reason TEXT,
 cleanup_failed_at INTEGER,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_transfers(attempt,database,new_epoch,outcome_kind,cancelled_by,granted_epoch,granted_start_epoch,granted_start_sequence,granted_expires_at,pending_transfer,failure_reason,refusal_kind,refusal_last_renewed_at,refusal_last_seen,refusal_gate_code,refusal_gate_detail,refusal_transfer,refusal_migration_state,refusal_store_fingerprint,refusal_holder,refusal_superseded_by,refusal_migration,refusal_store_version,refusal_supported_version,phase_kind,phase_final_epoch,phase_final_sequence,cleanup_failure_reason,cleanup_failed_at,created_at,updated_at) SELECT attempt,database,NULLIF(intent,'null'),json_extract(outcome,'$.kind'),json_extract(outcome,'$.value.by'),NULLIF(outcome -> '$.value.epoch','null'),NULLIF(outcome -> '$.value.start.epoch','null'),NULLIF(outcome -> '$.value.start.sequence','null'),json_extract(outcome,'$.value.expiresAt'),json_extract(outcome,'$.value.transfer'),json_extract(outcome,'$.value.reason'),json_extract(outcome,'$.value.kind'),json_extract(outcome,'$.value.value.lastRenewedAt'),json_extract(outcome,'$.value.value.lastSeen'),json_extract(outcome,'$.value.value.reason.code'),json_extract(outcome,'$.value.value.reason.detail'),json_extract(outcome,'$.value.value.transfer'),json_extract(outcome,'$.value.value.state'),json_extract(outcome,'$.value.value.store.fingerprint'),json_extract(outcome,'$.value.value.holder'),json_extract(outcome,'$.value.value.by'),NULLIF(outcome -> '$.value.value.migration','null'),NULLIF(outcome -> '$.value.value.store.version','null'),NULLIF(outcome -> '$.value.value.supported','null'),json_extract(phase,'$.phase'),NULLIF(phase -> '$.finalPosition.epoch','null'),NULLIF(phase -> '$.finalPosition.sequence','null'),json_extract(cleanup_failed,'$.reason'),json_extract(cleanup_failed,'$.at'),coalesce(json_extract(phase_history,'$[0].enteredAt'),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))),max(coalesce(json_extract(cleanup_failed,'$.at'),coalesce(json_extract(phase_history,'$[0].enteredAt'),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)))),coalesce(json_extract(phase_history,'$[#-1].enteredAt'),coalesce(json_extract(phase_history,'$[0].enteredAt'),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))))) FROM _fleet_sync_transfers_legacy;
INSERT INTO _fleet_sync_transfer_phase_history(attempt,position,phase_kind,final_epoch,final_sequence,entered_at,created_at,updated_at) SELECT t.attempt,j.key,json_extract(j.value,'$.phase.phase'),NULLIF(j.value -> '$.phase.finalPosition.epoch','null'),NULLIF(j.value -> '$.phase.finalPosition.sequence','null'),json_extract(j.value,'$.enteredAt'),json_extract(j.value,'$.enteredAt'),json_extract(j.value,'$.enteredAt') FROM _fleet_sync_transfers_legacy AS t,json_each(t.phase_history) AS j;
DROP TABLE _fleet_sync_transfers_legacy;
CREATE UNIQUE INDEX _fleet_sync_one_transfer ON _fleet_sync_transfers(database) WHERE outcome_kind IS NULL;
ALTER TABLE _fleet_sync_lifecycle_attempts RENAME TO _fleet_sync_lifecycle_attempts_legacy;
CREATE TABLE _fleet_sync_lifecycle_attempts(
 attempt TEXT NOT NULL PRIMARY KEY,
 database TEXT NOT NULL,
 intent_kind TEXT NOT NULL,
 writer TEXT,
 origin_kind TEXT,
 import_digest TEXT,
 origin_epoch TEXT,
 origin_sequence TEXT,
 expect_fence TEXT,
 expect_acked_epoch TEXT,
 expect_acked_sequence TEXT,
 outcome_kind TEXT,
 answer_kind TEXT,
 failure_reason TEXT,
 lease_database TEXT,
 lease_holder TEXT,
 lease_state TEXT,
 lease_epoch TEXT,
 lease_remaining_ms TEXT,
 lease_expires_at INTEGER,
 replayed INTEGER CHECK (replayed IN (0,1)),
 refusal_kind TEXT,
 gate_code TEXT,
 gate_detail TEXT,
 existing_digest TEXT,
 offered_digest TEXT,
 current_epoch TEXT,
 retired_at INTEGER,
 mismatch_fence TEXT,
 mismatch_acked_epoch TEXT,
 mismatch_acked_sequence TEXT,
 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER))
);
INSERT INTO _fleet_sync_lifecycle_attempts(attempt,database,intent_kind,writer,origin_kind,import_digest,origin_epoch,origin_sequence,expect_fence,expect_acked_epoch,expect_acked_sequence,outcome_kind,answer_kind,failure_reason,lease_database,lease_holder,lease_state,lease_epoch,lease_remaining_ms,lease_expires_at,replayed,refusal_kind,gate_code,gate_detail,existing_digest,offered_digest,current_epoch,retired_at,mismatch_fence,mismatch_acked_epoch,mismatch_acked_sequence,created_at,updated_at) SELECT attempt,database,json_extract(intent,'$.kind'),json_extract(intent,'$.request.writer'),json_extract(intent,'$.request.origin.kind'),json_extract(intent,'$.request.digest'),NULLIF(intent -> '$.request.origin.value.position.epoch','null'),NULLIF(intent -> '$.request.origin.value.position.sequence','null'),NULLIF(intent -> '$.request.expectFence','null'),NULLIF(intent -> '$.request.expectAcked.epoch','null'),NULLIF(intent -> '$.request.expectAcked.sequence','null'),json_extract(outcome,'$.kind'),json_extract(outcome,'$.answer.kind'),json_extract(outcome,'$.answer.reason'),json_extract(outcome,'$.answer.value.lease.database'),json_extract(outcome,'$.answer.value.lease.holder'),json_extract(outcome,'$.answer.value.lease.state'),NULLIF(outcome -> '$.answer.value.lease.epoch','null'),NULLIF(outcome -> '$.answer.value.lease.remainingMs','null'),json_extract(outcome,'$.answer.value.lease.expiresAt'),json_extract(outcome,'$.answer.value.replayed'),json_extract(outcome,'$.answer.value.kind'),json_extract(outcome,'$.answer.value.value.reason.code'),json_extract(outcome,'$.answer.value.value.reason.detail'),json_extract(outcome,'$.answer.value.existing'),json_extract(outcome,'$.answer.value.offered'),NULLIF(outcome -> '$.answer.value.value.current','null'),json_extract(outcome,'$.answer.value.at'),NULLIF(outcome -> '$.answer.value.value.fence','null'),NULLIF(outcome -> '$.answer.value.value.acked.epoch','null'),NULLIF(outcome -> '$.answer.value.value.acked.sequence','null'),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)),(CAST(strftime('%s','now') AS INTEGER)*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER)) FROM _fleet_sync_lifecycle_attempts_legacy;
DROP TABLE _fleet_sync_lifecycle_attempts_legacy;
CREATE UNIQUE INDEX _fleet_sync_one_lifecycle_attempt ON _fleet_sync_lifecycle_attempts(database) WHERE outcome_kind IS NULL;

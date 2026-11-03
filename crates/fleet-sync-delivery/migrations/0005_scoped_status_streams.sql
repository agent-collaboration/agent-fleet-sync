ALTER TABLE _fleet_sync_status_events RENAME TO _fleet_sync_status_events_single_sender;
ALTER TABLE _fleet_sync_status_state RENAME TO _fleet_sync_status_state_single_sender;

CREATE TABLE _fleet_sync_status_state (
    submitter_kind TEXT NOT NULL,
    submitter_id TEXT NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY(submitter_kind, submitter_id)
);
INSERT INTO _fleet_sync_status_state(submitter_kind, submitter_id, record)
SELECT 'machine', json_extract(record, '$.sender'), record
FROM _fleet_sync_status_state_single_sender;

CREATE TABLE _fleet_sync_status_events (
    submitter_kind TEXT NOT NULL,
    submitter_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    item_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY(submitter_kind, submitter_id, cursor),
    UNIQUE(submitter_kind, submitter_id, item_id, version)
);
INSERT INTO _fleet_sync_status_events(submitter_kind, submitter_id, cursor, item_id, version, record)
SELECT 'machine', (SELECT json_extract(record, '$.sender') FROM _fleet_sync_status_state_single_sender WHERE singleton = 1),
    cursor, item_id, version, record
FROM _fleet_sync_status_events_single_sender;
DROP TABLE _fleet_sync_status_events_single_sender;
DROP TABLE _fleet_sync_status_state_single_sender;

ALTER TABLE _fleet_sync_status_owed RENAME TO _fleet_sync_status_owed_unscoped;
CREATE TABLE _fleet_sync_status_owed (
    item_id TEXT NOT NULL,
    target TEXT NOT NULL,
    version INTEGER NOT NULL,
    record TEXT NOT NULL,
    submitter_kind TEXT NOT NULL,
    submitter_id TEXT NOT NULL,
    PRIMARY KEY(item_id, target)
);
INSERT INTO _fleet_sync_status_owed(item_id,target,version,record,submitter_kind,submitter_id)
SELECT item_id,target,version,record,'machine',COALESCE(
    json_extract(target, '$.value.sender'), json_extract(target, '$.value.peer')
) FROM _fleet_sync_status_owed_unscoped;
DROP TABLE _fleet_sync_status_owed_unscoped;
CREATE INDEX _fleet_sync_status_owed_submitter ON _fleet_sync_status_owed(submitter_kind, submitter_id, item_id);

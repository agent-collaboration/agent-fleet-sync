-- Frozen v1 task encoding: input and local output are JSON 7 (hex 37).
-- The SHA-256 digest and canonical call target are fixed historical bytes.
CREATE TABLE effects(value INTEGER NOT NULL);
INSERT INTO effects VALUES(1);
INSERT INTO _fleet_sync_operations(operation_id,kind,kind_version,request_key,input,input_digest,state,next_attempt_at,attempt)
VALUES('0197abc0-def0-7123-8123-abcdef012345','metadata-replay',1,'old-request',X'37',X'7902699be42c8a8e46fbbb4501726517e86b22c56a189f7625a6da49081b2451','running',1000,0);
INSERT INTO _fleet_sync_steps(operation_id,position,step_name,kind,output)
VALUES('0197abc0-def0-7123-8123-abcdef012345',0,'local','local',X'37');
INSERT INTO _fleet_sync_steps(operation_id,position,step_name,kind,intent,first_attempt_at,may_have_reached)
VALUES('0197abc0-def0-7123-8123-abcdef012345',1,'remote','call_pending','{"intent":{"target":"[\"metadata-double\",null]","request_digest":[121,2,105,155,228,44,138,142,70,251,187,69,1,114,101,23,232,107,34,197,106,24,159,118,37,166,218,73,8,27,36,81],"replay":"deduplicated","unsent_for":null},"reach":"never_sent","last_seen":null}',250,0);

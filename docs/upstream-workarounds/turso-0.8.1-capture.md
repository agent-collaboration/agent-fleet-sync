# Turso 0.8.1 capture and foreign keys

The workspace pins Turso and its engine to 0.8.1. These notes distinguish
current workarounds from historical probes. Recheck their real-file tests when
changing that version.

## Key-changing UPSERT loses its old key

An UPSERT whose DO UPDATE assigns a TEXT or composite primary-key column can
record its new key in both capture images. Replaying that record cannot remove
the old key. Ordinary UPDATE of the same key captures both keys correctly.

Every primary-key column of a replicated table is now TEXT NOT NULL holding a
canonical UUIDv7. The writer refuses both UPDATE and UPSERT assignments to any
key column, including an assignment of the same value. This is the immutable-key
contract (decision 51), independent of the engine defect. Payload updates,
DO NOTHING, REPLACE INTO and INSERT OR REPLACE remain allowed. Current real-file
tests cover single and composite UUIDv7 keys and both REPLACE spellings,
including conflicts with two existing rows.

The real-file SG11 probe ran 24 initial cases, then 33 cases with minimal SET
clauses and controls. The latter reconstructed 29 row sets and detected four
TEXT/composite key-changing UPSERT mismatches. Equal raw before/after bytes ruled
out the JSON diagnostic formatter as the source of the lost key.

```sql
CREATE TABLE records(id TEXT PRIMARY KEY, slot TEXT UNIQUE, payload TEXT);
INSERT INTO records VALUES ('old','same','value');
PRAGMA capture_data_changes_conn('full');
INSERT INTO records VALUES ('new','same','value')
ON CONFLICT(slot) DO UPDATE SET id=excluded.id;
```

The guard remains even if upstream capture changes. The historical probe above
explains the earlier UPSERT-only refusal; it does not define the supported key
contract. No upstream fix or issue is claimed here.

## Historical INTEGER PRIMARY KEY capture

Changing an INTEGER PRIMARY KEY emits DELETE then INSERT, but the latter can
carry its row in before with after NULL. Replicated integer keys are refused
under decision 51, and the decoder has no fallback for that image. A missing
INSERT after-image fails closed. Local and hook tables keep their own key types.

## foreign_key_check returns no evidence

Turso 0.8.1 silently returns no rows for PRAGMA foreign_key_check, including for a
persisted orphan. The writer therefore relies on enforced foreign keys for
ordinary writes. Migrations that disable enforcement need an explicit orphan
query built from foreign_key_list and the schema before commit. Copies apply
whole writer transactions with enforcement off and do not run an orphan check.

The native probe verified enforced refusal, metadata and a persisted orphan
after reopening for explicit single-column, explicit composite and implicit
parent-key references. For an implicit REFERENCES target, foreign_key_list's
target name is an empty string. Resolve it from the parent's declared primary
key in table_info key order; refuse a missing key or mismatched column count.
Valid children and children with a NULL key are controls for the orphan query.

Remove the explicit check only after a real-file test shows that the upstream
PRAGMA finds persisted orphans and the migration proof still rejects an orphan
before committing. An empty PRAGMA result alone is never that evidence.

## Keep the acknowledged commit marker when pruning

Turso 0.8.1's WAL CDC allocator uses `NewRowid` without an AUTOINCREMENT floor. Deleting the final CDC row allows later capture IDs to restart, including after closing and reopening the file. The capture table's AUTOINCREMENT declaration does not prevent this path.

Retention deletes rows strictly before the acknowledged commit marker. Keeping this one marker preserves the allocation floor without keeping acknowledged row images. `capture_watermark.rs` reproduces ID reuse with an empty table and verifies monotonic IDs with the retained marker across reopen. Remove this workaround when the pinned engine preserves capture allocation after every row is deleted, and update the regression then.

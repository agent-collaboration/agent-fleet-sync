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

## Keep the orphan statement shallow

The explicit orphan check uses a keyed `LEFT JOIN`. Its `ON` contains only
`parent.key = child.key` comparisons, with the parent operand first to retain
its collation. Every child key's non-null guard and the first mapped parent
key's `IS NULL` anti-match stay in `WHERE`. A checked child has no null key
column, so a matched parent's first key cannot be null. Implicit references
use the metadata-resolved parent key; every identifier is quoted.

This replaces correlated `NOT EXISTS`, whose nested shape exhausted Turso
0.8.1's recursive parser stack in a consumer's debug open → migrate path.
Historical parser/native-prepare measurements used the pinned engine,
standalone default dev profile, 16 KiB brackets and 1,214 bounded trials with
no timeouts. Native prepare needed 560 KiB for the old single-column shape
and 624 KiB at widths 2–98; the keyed join needed 288 and 352 KiB respectively.
These are measured brackets, not a guarantee about every host's stack.
There is no new width cap or stack setting.

The seven-case, before/after-reopen comparison with a partial child index
retained the old meaning for the keyed join. `EXCEPT` and row `NOT IN` each
changed both NOCASE/BINARY cross-collation outcomes and were rejected.
Keeping left-only predicates out of `ON` also avoids the separate Turso 0.8.1
partial-index/reopen outer-row defect. Permanent writer tests repeat the
meaning cases at widths 1, 2 and 98 through the production generator, and
exercise real public open and migration refusal with rollback.

Reassess this statement on an engine upgrade. Restore a correlated query only
when the consumer's formerly aborting test completes at its real open → migrate
depth on the normal default thread, and the production-generator null,
collation, composite-key, partial-index and reopen cases all retain their
literal outcomes. Successful preparation alone is insufficient. The consumer
depth check remains separate from the library's meaning tests.

## Keep the acknowledged commit marker when pruning

Turso 0.8.1's WAL CDC allocator uses `NewRowid` without an AUTOINCREMENT floor. Deleting the final CDC row allows later capture IDs to restart, including after closing and reopening the file. The capture table's AUTOINCREMENT declaration does not prevent this path.

Retention deletes rows strictly before the acknowledged commit marker. Keeping this one marker preserves the allocation floor without keeping acknowledged row images. `capture_watermark.rs` reproduces ID reuse with an empty table and verifies monotonic IDs with the retained marker across reopen. Remove this workaround when the pinned engine preserves capture allocation after every row is deleted, and update the regression then.

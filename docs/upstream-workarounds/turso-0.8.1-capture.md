# Turso 0.8.1 capture and foreign keys

The workspace pins Turso and its engine to 0.8.1. These workarounds belong to
the captured writer. Recheck their real-file tests when changing that version.

## Key-changing UPSERT loses its old key

An UPSERT whose DO UPDATE assigns a TEXT or composite primary-key column can
record its new key in both capture images. Replaying that record cannot remove
the old key. Ordinary UPDATE of the same key captures both keys correctly.

The writer refuses UPSERT assignments to any declared primary-key column of a
replicated table before execution. It parses SQL with the pinned parser; payload
updates, DO NOTHING, ordinary UPDATE, REPLACE INTO and INSERT OR REPLACE remain
allowed. The two REPLACE spellings captured all implicit deletes in the probe,
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

Remove the refusal only after the upstream capture records the old declared key
for this UPSERT, the pinned-behavior reproduction detects that change, and the
real writer-to-copy test passes without the guard. No upstream fix or issue is
claimed here.

## INTEGER PRIMARY KEY UPDATE puts an INSERT image in before

Changing an INTEGER PRIMARY KEY emits DELETE then INSERT, but the latter can
carry its row in before with after NULL. The decoder accepts this fallback only
for a table with an INTEGER primary-key alias. Other missing INSERT images fail
closed. The real capture test checks the old-key deletion and every new-row
storage class, including a wide integer and a blob.

Remove this fallback when the pinned engine emits the INSERT row in after and
the version-change reproduction confirms the corrected behavior.

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

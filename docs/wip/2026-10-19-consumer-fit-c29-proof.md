# C29: retained file origin

This slice follows C30 base d02024053f647efafecc90a9ac9103ababf17fc8 and implements current program design §13a W-C31 and canonical r4 row C29. It exposes only `RetainedFile::origin() -> RetainedFileOrigin { database, epoch, start, kind }`. A consumer compares database, epoch and start before using Created as a genesis root; kind alone or sequence zero alone is insufficient.

## Source and model

The existing private WriterFileRecord already stores all four facts, including a required explicit Created/Adopted/Bootstrap enum. Its strict decoder refuses missing or unknown origins and validates the record's identities and positions. RetainedFile already reads that validated record when opening its passive read-only pool and caches its authority and original registration result. The new metadata uses that same read, with a synchronous copy accessor and no additional SQL, storage or write-capable handle.

The origin kind keeps one authoritative enum: the formerly private WriterFileOrigin is renamed and exported as RetainedFileOriginKind, and the private bookkeeping continues to use that same type. Its existing serde variants and representation are unchanged. No duplicate enum, alias, fallback or compatibility shim is introduced; no persisted field, schema or migration changes. Only the requested metadata is exposed, leaving the writer record private.

Permanent scenarios were written before the accessor implementation. The previous public API has no accessor, so no pre-fix behavioral red has been observed or claimed. The initial drafts were source-only while the paired verifier observed C30. After the explicit slot return, scoped helper reanchor builds compiled the new API; the four behavior cases have now passed, exit 0, at this checkpoint. No generation command has run. After the slot returned, both controls compiled and ran unchanged permanent tests: inferring Created from sequence zero fails the actual-adoption case 0/1, exit 101, on Created versus Adopted; a missing-origin default fails the corruption case 0/1, exit 101, on an accepted missing origin. Both product files were restored byte-for-byte and test hashes remained unchanged. The restored full writer/runner suite passes 141/0/0, exit 0. Each Cargo target has one result summary; successful re-exec child output is not added to the parent count.

## Proof map

| Obligation | Real path and independent expectation | Result |
| --- | --- | --- |
| Created origin | Actual managed writer; factory identity and literal epoch-one/zero start; two retained reopens | Supported by the four actual-Turso cases |
| Read-only operation | Real files in a read-only directory; unchanged entry hashes, sizes, modes and file/directory mtimes | Supported by the four actual-Turso cases |
| Adopted zero is not Created | Existing actual pre-capture adoption, migration and copy-seed journey; adopted kind with literal zero start after two reopens | Supported by the four actual-Turso cases |
| Bootstrap origin | Existing real snapshot conversion, publication, ready fence and later migration journey; epoch two, original supplied snapshot start and Bootstrap kind after two retained reopens | Supported by the four actual-Turso cases |
| Corrupt origin refusal | Actual missing, unknown and null stored kinds; typed Corrupt at read-only open and exact file preservation | Supported by the four actual-Turso cases |
| Sensitivity | A sequence-zero-to-Created inference and a missing-origin-to-Created default must be caught | Both permanent controls fail 0/1, exit 101 |

Native file and bootstrap observations remain distinct from deployed DO behavior and real remote holder drain. No acceptance or whole-PR readiness is claimed. Local build/runtime stays with the paired verifier until an explicit settled-job return. Queue after C29: C32, C31, C18a/C18b, then C24 only from its future neutral brief.

## Validation and limits

- `cargo test -p fleet-sync-writer -p fleet-sync-tokio-runner --all-features --locked -j 4 -- --test-threads=4`: exit 0, 141 passed, 0 failed, 0 ignored, after exact control restoration. This includes 25 runner cases, 80 writer unit cases and 36 writer integration cases. The four directly affected cases are a subset, not additional passes.
- `cargo clippy -p fleet-sync-writer -p fleet-sync-tokio-runner --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0.
- `cargo fmt -p fleet-sync-writer -p fleet-sync-tokio-runner -- --check` and `git diff --check`: exit 0.
- This is the native writer/runner surface. No portable protocol or Wasm implementation changed. No local workspace-wide command, generator invocation, migration or corpus edit was needed; strict build-time checked-query freshness remained active.
- Independent observation and exact-head CI remain pending at declaration. No deployed DO, real remote holder drain, consumer board integration or whole-PR readiness claim is added.

The prior verifier checkout dependency was reanchored through the query-generator library and its six consumer build helpers. Checking the CLI binary alone was insufficient because the library embeds its generator workspace at compile time. The final scoped clean/build passed, and a separately inspected scan of 190 helper/CLI executables plus two generator libraries found zero retained-checkout references before an explicit dependency release. Failed preliminary scan and corrected release receipts remain in the ignored trace; product source and metadata needed no workaround. Normal build output is retained for the queued slices.

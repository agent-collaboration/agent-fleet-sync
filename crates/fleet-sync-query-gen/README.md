# fleet-sync-query-gen

This host tool describes exact SQL with the owned Turso driver, then generates typed calls over `durable_steps::storage::TransactionScope`. The generated calls work with the host's borrowed SQLx executor; the tool owns no runtime storage interface.

Run from the workspace root:

```sh
cargo run -p fleet-sync-query-gen --features native -- --manifest crates/fleet-sync-query-gen/checked-test-queries.json
cargo run -p fleet-sync-query-gen --features native -- --manifest crates/fleet-sync-query-gen/checked-test-queries.json --check
cargo test -p fleet-sync-query-gen --features native
```

On macOS, an inherited Homebrew C compiler may need to be replaced for the command with `CC=/usr/bin/clang CXX=/usr/bin/clang++`.

Each consumer supplies a manifest like [the executable test corpus](checked-test-queries.json). Paths are relative to its package. Migrations build a fresh native schema; query files contain exactly the statements used by the generated functions. Results are `many`, `optional`, `one`, `execute`, or `exists`. Parameter kinds are `text`, `integer`, `real`, and `blob`, with explicit nullability. These are bind types, not result-type overrides.

An `exists` query declares `row: null` and returns `bool` from row presence. It still prepares and describes the exact statement against the migrated schema, but accepts empty or unknown result metadata and decodes no columns. This permits direct PRAGMA statements; misspelled tables in queries still fail generation. Preparation does not certify engine feature support: Turso 0.8.1 silently treats unsupported `PRAGMA foreign_key_check` as a no-op, so this mode alone cannot establish an FK-validation gate.

Project plain columns, or use the two expression families proved from the parser AST: `typeof(x)` yields TEXT, and `CAST(x AS INTEGER | REAL | TEXT | BLOB)` yields that storage class. Other unnamed types are refused. Metadata records whether a type came from the driver or one of those AST rules. Exact integer reads across the Durable Object driver use `typeof(col), CAST(col AS TEXT)`; domain decoding checks the class before parsing the decimal value.

Unknown nullability stays optional, and domain decoding checks required fields. SQL bind slots come from the parser, so question marks in comments and strings do not change arity. Each output column is read by its name into a private row type.

Consumers add this crate as a build dependency without the `native` feature and call `fleet_sync_query_gen::verify_manifest("checked-queries.json")` from `build.rs`. That verifies the SQL, migration set, manifest, generator, owned drivers, toolchain, lockfile, and generated source hashes. A new migration or a modified generated file fails the check. There is no environment switch that bypasses it; regenerate with the standalone CLI when inputs change.

The generation receipt also requires `queries_digest`, a SHA-256 over the typed serialization of the complete ordered query descriptions. The emitter and portable verifier use one helper. Editing any description field, deleting or reordering queries or columns, or changing the digest fails the public build gate. JSON whitespace and object-key order do not change those typed descriptions. A missing digest is refused without a legacy fallback; regenerate older corpora. This binds descriptions to their generation receipt; native generation remains responsible for describing SQL correctly.

Consumer inputs keep corpus-relative keys. Generator inputs use portable keys such as `generator:fleet-sync-query-gen/src/lib.rs`, `generator:workspace/Cargo.lock` and `generator:workspace/rust-toolchain.toml`. Reading and Cargo watches use their actual filesystem paths. Repeated references to the same canonical input coalesce; distinct inputs or origins sharing a logical key, including the generated output, are refused even when their bytes match. Regenerate existing corpora once for this key scheme; legacy keys have no fallback.

Generated functions return an explicit `Future + Send`: this preserves borrowed-scope calls inside spawned Tokio hosts, where inferred async-function futures hit a Rust GAT lifetime-generality error. The executable corpus includes that host path.

The native tests cover real description, generated execution with exact values and rollback, bind arity, unknown types, and freshness failures. Their starting references were the receiver's query-generator prototype and the one-core probe's generator.

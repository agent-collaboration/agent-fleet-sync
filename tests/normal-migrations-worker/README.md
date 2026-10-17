# Table rename replay in workerd

This retains the normal-migration harness used to investigate table rename admission. It applies the native consumer journey's exact exported Piece JSON through the receiver's Durable Object adapter. Checkpoints compare typed rows, column layouts, schema identity and acknowledged position. The native journey also checks literal UUID, wide integer, blob and NULL values.

Run from the repository root with the pinned Rust toolchain, WASM target, pnpm and **wasm-bindgen CLI 0.2.129** available. On macOS, set `CC=/usr/bin/clang CXX=/usr/bin/clang++` for Cargo commands. Set `WASM_BINDGEN` to that exact CLI; the generated bindings must match the Rust dependency.

```sh
pnpm install --frozen-lockfile
NORMAL_RUN_ROOT="$PWD/tmp/d91-native" NORMAL_FIXTURE_ROOT="$PWD/tmp/d91-fixtures" \
  CARGO_TARGET_DIR=target cargo test --manifest-path tests/consumer/Cargo.toml \
  --all-features --locked --test native_migration_rename --test native_rename_bootstrap
CARGO_TARGET_DIR=target/d91-worker cargo build \
  --manifest-path tests/normal-migrations-worker/Cargo.toml \
  --target wasm32-unknown-unknown --locked
mkdir -p tests/normal-migrations-worker/build
"$WASM_BINDGEN" target/d91-worker/wasm32-unknown-unknown/debug/normal_migrations_worker.wasm \
  --target bundler --out-name index --out-dir tests/normal-migrations-worker/build
cp tests/normal-migrations-worker/shim.mjs tests/normal-migrations-worker/build/shim.mjs
python3 tests/normal-migrations-worker/prepare-matrix.py \
  tmp/d91-workerd-manifest.json tmp/d91-fixtures
node tests/normal-migrations-worker/run-matrix.mjs tmp/d91-workerd-manifest.json d91
```

Use fresh evidence directories on subsequent runs. The native suite must report five passing migration tests plus one bootstrap journey, and every workerd result must be `passed`: warm rename, reopen with the new bundle, fresh full bundle, the fresh `a → b → c` chain, and local history created/written in v1 then dropped in v3. `prefixOnly` means the writer did not produce the target stream and is not a passing rename case. The five workerd cases together have eighteen checkpoints. The dropped-local case checks the actual native receiver catalog inside its owning hook transaction after every applied piece, scans every exported Piece for local data/schema, and adds explicit catalog-absence queries to its six workerd checkpoints. The separate native bootstrap journey publishes the v1 prefix under the v2 rename bundle, migrates to tasks, writes, and verifies the authoritative copy follows; it does not export a workerd bootstrap case. Evidence is written under `tests/normal-migrations-worker/evidence/<run>/results.json`.

The runtime pins are Miniflare 4.20260730.0, workerd 1.20260730.1 and compatibility date 2026-07-29. This is local workerd evidence; it does not establish hosted deployment behavior. Build output and runtime state are ignored by Git.

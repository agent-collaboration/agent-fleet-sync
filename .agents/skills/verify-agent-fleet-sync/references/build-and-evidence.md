# Build target and retained evidence

Coordinate the exclusive build/observation slot with the Implementer. Never
build alongside it. Set `VERIFICATION_WORKTREE_ROOT` to the canonical assigned
worktree root before entering the pinned source archive, then select only the
worktree's normal Cargo target:

```bash
: "${VERIFICATION_WORKTREE_ROOT:?set the assigned worktree root}"
export CARGO_TARGET_DIR="$VERIFICATION_WORKTREE_ROOT/target"
verification_target="$CARGO_TARGET_DIR"
verification_worker_target="$CARGO_TARGET_DIR"
```

Native executables land in `target/debug`; wasm uses Cargo's normal target-triple
layout beneath this same target. The session launcher already honors
`CARGO_TARGET_DIR`; export it when driving an archive so its fallback cannot
create an archive-local target. No `.runs/*/target` or scratch target copies.
Permanent fixtures keep their own transient child-build and cleanup behavior.

Independence comes from the pinned source archive, checked source/configuration
fingerprints, owning build logs and recorded executable/wasm hashes. Build from
that archive even when Cargo reuses cached compilation; a cache or binary hash
alone does not establish source provenance. Before reusing `--no-build`, match
the saved provenance to the declared target. If an artifact embeds an older
archive's compile-time path, keep the setup failure, invalidate/rebuild the
affected owning packages in this same normal target and rerun the unchanged
gate. Never bypass freshness checks or create another target to hide it.

After all jobs and fixture children settle, keep every evidence summary, log,
hash inventory, receipt and command file outside build output. Keep runtime
fixture databases until their evidence consumer releases them. While still
holding the build slot, confirm no process uses the normal target, inspect a
`cargo clean --target-dir "$CARGO_TARGET_DIR" --dry-run --verbose` preview, then
run `cargo clean --target-dir "$CARGO_TARGET_DIR"`. Retain both logs and the
disk-space change before returning the slot. No completed run retains a target.

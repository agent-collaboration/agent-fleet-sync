# C26b: retained authority semantic checks

C26 at `634f546e` passed local and remote CI and independent observations of its original cases. It is not accepted against amended W-C28: the Lead added refusal of disagreeing saved cutoffs and same-epoch other-holder renewal recognition after an independent source check. These two outcomes are the C26b scope; C23 drafts are preserved in ignored checkout scratch and removed from the active source diff.

Pre-edit source inspection confirms both findings. The stored observation's cutoff and file's saved end are decoded separately without comparison. The holder recognizes another peer at the same epoch, while the retained projection only recognizes a higher epoch. Expected outcomes come from amended W-C28, not from the previous implementation.

Proof route: extend the permanent real-Turso retained-authority tests, then run them before implementation. The same-epoch case persists the public registration and renewal observations and checks both Superseded projection and the unchanged Ready registration. The cutoff case starts from a real StaleEpoch observation, changes only one well-typed cutoff (or removes the saved cutoff), closes writable handles, marks files and directory read-only, and requires Corrupt from the public opener without byte or mtime change. The existing fenced restart, empty-tail and other authority cases remain unchanged.

The Verifier explicitly returned the local build/runtime slot. The first focused red is running. C23, C18a and C18b have no build or runtime claims. No stand-ins are used.

The focused red ran against the previous implementation: 3 parent tests passed, 2 failed, exit 101. The cutoff test reached the real opener and reported acceptance of a structurally valid but disagreeing end. Projection case 4 reported None instead of Superseded at the same epoch for another peer. Both are pre-fix behavior failures; the existing two fence/restart children still passed.

Implementation choice: extract the existing epoch/peer relationship decision into the lease owner's pure `current_lease_relation`, with explicit OlderEpoch, SameWriter, NewerEpoch and OtherWriter cases. Both live holder restoration and retained inspection use it. This preserves the live holder's distinct NewerEpoch and NotWriter outcomes while preventing another inspection-only predicate from drifting. The helper makes no database or timing claim; its callers retain those checks. The writer decoder rejects a StaleEpoch end that differs from its own saved end, including a missing saved end. No persisted shape, wire format, table or migration changes. The added lease surface requires its own native suite and wasm gate alongside the writer gates.

The query-generator archive dependency was cleared with a targeted clean of the generator and its three consumer packages, then an all-feature build of those four packages, exit 0. A binary-reference scan of normal-target build helpers found zero references to the old archive, exit 0, and the Verifier was told its dependency is released. The first red had started before that rebuild instruction arrived; all later gates use rebuilt current-tree helpers.

## Local result

- `cargo test -p fleet-sync-writer retained_authority_tests --lib --locked -j 4 -- --nocapture`: 5 parent tests passed, zero failed, exit 0; both recursive restart children passed separately. The two new semantic requirements are supported by the cases that failed above. Both a different saved end and a missing saved end are refused without file changes.
- `cargo fmt --all -- --check`: exit 0.
- `cargo clippy -p fleet-sync-writer -p fleet-sync-lease --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0, zero Clippy warnings.
- `cargo test -p fleet-sync-writer -p fleet-sync-lease --all-features --locked -j 4`: 195 parent tests passed, zero failed or ignored, exit 0. Lease: 81 across two binaries. Writer: 114 across nine binaries. Child reexecutions are not added to those totals.
- `cargo check --target wasm32-unknown-unknown -p fleet-sync-lease --all-features --locked -j 4`: exit 0. The writer remains native-only.
- `git diff --check`: exit 0.

The changes introduce no cast, unchecked production unwrap, unsafe code, SQL, migration or stored-format change. The extracted relation is a typed comparison at the existing lease owner, not an authority or timing grant. Ready still describes registration; the retained authority documentation explicitly says absence of a loss observation does not prove a current write lease.

The original Verifier evidence was opened: focused and full native logs, its divergent inspection predicate failure, caller result, actual read-only database/WAL descriptors and settled controller/CI outputs support the claims made for the original target. Its two C26b exclusions remain explicit. A new immutable correction target, exact-head CI and independent C26b observation are declared directly to the Lead and paired Verifier; this document claims the local observations above, not independent acceptance or board integration.

#!/usr/bin/env bash
# L5: real Turso relay/Machine lifecycle, Local-to-Push and cancel-report rules.
set -euo pipefail
verification_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
evidence_directory=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)
      printf '%s\n' 'L5 delivery lifecycle: real public APIs, Turso stores and durable engines.'
      printf '%s\n' 'Usage: bash scripts/verification/delivery-lifecycle.sh [--evidence-dir DIR]'
      printf '%s\n' 'DIR must not exist; relative paths are resolved from the repository root.'
      printf '%s\n' 'Uses only the worktree target, checks 40 GiB free, runs one delivery package.'
      printf '%s\n' 'Configure the Cargo runner to sign launched binaries where required.'
      printf '%s\n' 'Keeps raw logs and source state; does not clean the shared build target.'
      printf '%s\n' 'Exit 0 observed; 1 Cargo/test failure; 2 setup failure; 3 missing coverage.'
      exit 0 ;;
    --evidence-dir) evidence_directory="${2:?missing evidence directory}"; shift 2 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done
cd "$verification_root"
if [[ -z "$evidence_directory" ]]; then
  mkdir -p scripts/verification/.runs
  evidence_directory="$(mktemp -d scripts/verification/.runs/delivery-lifecycle-XXXXXX)"
else
  mkdir "$evidence_directory" || exit 2
fi
evidence_directory="$(cd "$evidence_directory" && pwd)"
printf 'evidence: %s\n' "$evidence_directory"
git rev-parse HEAD > "$evidence_directory/source-head.txt"
git status --porcelain=v1 > "$evidence_directory/source-status.txt"
git diff HEAD -- crates tests/consumer Cargo.toml Cargo.lock .cargo rust-toolchain.toml > "$evidence_directory/product.diff"
if [[ -s "$evidence_directory/product.diff" ]]; then
  printf '%s\n' 'Product source differs from HEAD; declare a target before observing.' >&2
  exit 2
fi
if [[ "${CARGO_TARGET_DIR:-$verification_root/target}" != "$verification_root/target" ]]; then
  printf '%s\n' 'CARGO_TARGET_DIR must be the assigned worktree target.' >&2
  exit 2
fi
export CARGO_TARGET_DIR="$verification_root/target"
mkdir "$evidence_directory/runtime-tmp"
export TMPDIR="$evidence_directory/runtime-tmp"
df -h . > "$evidence_directory/disk-before.txt"
if [[ "$(df -k . | awk 'NR==2 {print $4}')" -lt 41943040 ]]; then
  printf '%s\n' 'Build refused: less than 40 GiB free.' >&2
  exit 2
fi
printf '%s\n' 'cargo test -p fleet-sync-delivery --all-features --locked' > "$evidence_directory/command.txt"
set +e
cargo test -p fleet-sync-delivery --all-features --locked > "$evidence_directory/delivery-test.log" 2>&1
cargo_exit=$?
set -e
printf '%s\n' "$cargo_exit" > "$evidence_directory/delivery-test.exit"
git rev-parse HEAD > "$evidence_directory/source-head-after.txt"
git diff HEAD -- crates tests/consumer Cargo.toml Cargo.lock .cargo rust-toolchain.toml > "$evidence_directory/product-after.diff"
if ! cmp -s "$evidence_directory/source-head.txt" "$evidence_directory/source-head-after.txt" || [[ -s "$evidence_directory/product-after.diff" ]]; then
  printf '%s\n' 'Target changed during observation.' >&2
  exit 2
fi
if [[ "$cargo_exit" -ne 0 ]]; then
  tail -n 60 "$evidence_directory/delivery-test.log"
  exit 1
fi
for required_test in \
  relay_schema::tests::relay_retirement_preserves_relay_api_calls_across_reopen \
  relay_schema::tests::relay_retirement_is_transactional_and_leaves_other_tables_unchanged \
  relay_schema::tests::relay_retirement_accepts_a_store_with_no_delivery_tables \
  relay_schema::tests::machine_startup_preserves_custody_bytes_and_removal_still_cleans_both_homes \
  local_cancel_routes::push_promotes_a_prevented_local_original_and_reports_once_across_reopen \
  local_cancel_routes::failed_preserved_outcome_projection_rolls_back_push_promotion_and_report_debt \
  relay_outbox::unadmitted_cancel_reports_return_typed_refusal_without_any_transaction_write; do
  if ! grep -Fxq "test $required_test ... ok" "$evidence_directory/delivery-test.log"; then
    printf 'required test not observed: %s\n' "$required_test" >&2
    exit 3
  fi
done
grep '^test result:' "$evidence_directory/delivery-test.log" > "$evidence_directory/counts.txt"
cat "$evidence_directory/counts.txt"
printf '%s\n' 'L5 lifecycle journeys observed; retain evidence before owned target cleanup.'

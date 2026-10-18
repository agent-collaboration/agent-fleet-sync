#!/usr/bin/env bash
# Run one consumer-fit journey using the existing native consumer harness.
# Exit 0: all obligations observed; 1: product mismatch; 2: harness failure;
# 3: required coverage is unavailable. Evidence survives every exit.
set -euo pipefail
verification_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
journey_name="${1:-}"
if [[ $# -gt 0 ]]; then shift; fi
case "$journey_name" in
  holder-refusals|target-capability|frame-shapes|copy-containment) ;;
  *) printf 'usage: scripts/verification/<journey>.sh [--help] [--no-build] [--evidence-dir DIR] [--target native|workerd]\n' >&2; exit 2 ;;
esac
build_requested=true
runtime_target=native
evidence_directory=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)
      printf '%s\n' "$journey_name: real native consumer-fit observation."
      printf '%s\n' 'Run from any directory. Coordinate exclusive Cargo access first.'
      printf '%s\n' 'Options: --no-build, --evidence-dir DIR (must not exist), --target native|workerd.'
      printf '%s\n' 'Default evidence: scripts/verification/.runs/<journey>-<unique>.'
      printf '%s\n' 'Exit 0 complete; 1 contract mismatch; 2 harness/setup failure; 3 coverage gap.'
      printf '%s\n' 'Native sessions only. C3/C6 fault controls are staged at their declared heads.'
      exit 0 ;;
    --no-build) build_requested=false; shift ;;
    --evidence-dir) evidence_directory="${2:?missing evidence directory}"; shift 2 ;;
    --target) runtime_target="${2:?missing target}"; shift 2 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done
cd "$verification_root"
if [[ -n "$evidence_directory" ]]; then
  mkdir "$evidence_directory"
  evidence_directory="$(cd "$evidence_directory" && pwd)"
else
  mkdir -p scripts/verification/.runs
  evidence_directory="$(mktemp -d "scripts/verification/.runs/${journey_name}-XXXXXX")"
  evidence_directory="$(cd "$evidence_directory" && pwd)"
fi
printf 'evidence: %s\n' "$evidence_directory"
record_source() {
  local suffix="$1"
  if [[ -f .verification-source-revision ]]; then
    cat .verification-source-revision > "$evidence_directory/source-head${suffix}.txt"
    printf '%s\n' 'git archive; see source fingerprint' > "$evidence_directory/source-status${suffix}.txt"
  elif [[ "$(git rev-parse --show-toplevel)" == "$verification_root" ]]; then
    git rev-parse HEAD > "$evidence_directory/source-head${suffix}.txt"
    git status --porcelain > "$evidence_directory/source-status${suffix}.txt"
  else
    printf 'source is neither a repository root nor an identified archive\n' >&2
    return 2
  fi
  find crates tests/consumer scripts/verification/native -type f \( -name '*.rs' -o -name '*.sql' -o -name Cargo.toml -o -name Cargo.lock \) -print0 |
    LC_ALL=C sort -z | xargs -0 shasum -a 256 > "$evidence_directory/source-fingerprint${suffix}.txt"
}
record_source ''
case "$runtime_target" in
  native) ;;
  workerd)
    printf '%s\n' '{"status":"gap","case":"workerd-host","detail":"No real fleet session host is exposed by the existing workerd fixture. SQL-driver and HTTP apply fixtures do not observe session containment."}' | tee "$evidence_directory/observations.jsonl"
    exit 3 ;;
  *) printf 'unsupported target: %s\n' "$runtime_target" >&2; exit 2 ;;
esac
native_manifest=scripts/verification/native/Cargo.toml
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$verification_root/target}"
consumer_binary="$CARGO_TARGET_DIR/debug/fleet-sync-test-consumer"
if "$build_requested"; then
  df -h . > "$evidence_directory/disk-before-consumer.txt"
  if ! cargo build --manifest-path tests/consumer/Cargo.toml --locked --bin fleet-sync-test-consumer > "$evidence_directory/build-consumer.log" 2>&1; then
    printf 'consumer build failed; see %s/build-consumer.log\n' "$evidence_directory" >&2
    exit 2
  fi
  df -h . > "$evidence_directory/disk-before-driver.txt"
  if ! env "CARGO_BIN_EXE_fleet-sync-test-consumer=$consumer_binary" cargo build --manifest-path "$native_manifest" --locked > "$evidence_directory/build-driver.log" 2>&1; then
    printf 'journey build failed; see %s/build-driver.log\n' "$evidence_directory" >&2
    exit 2
  fi
fi
journey_binary="$CARGO_TARGET_DIR/debug/consumer-fit-journeys"
if [[ ! -x "$consumer_binary" || ! -x "$journey_binary" ]]; then
  printf 'missing built consumer or journey executable; see --help\n' >&2
  exit 2
fi
shasum -a 256 "$consumer_binary" "$journey_binary" > "$evidence_directory/binary-sha256.txt"
set +e
"$journey_binary" "$journey_name" "$evidence_directory" 2>&1 | tee "$evidence_directory/observations.jsonl"
journey_exit=${PIPESTATUS[0]}
set -e
printf '%s\n' "$journey_exit" > "$evidence_directory/exit-code.txt"
record_source '-after'
if ! cmp -s "$evidence_directory/source-head.txt" "$evidence_directory/source-head-after.txt" || ! cmp -s "$evidence_directory/source-status.txt" "$evidence_directory/source-status-after.txt" || ! cmp -s "$evidence_directory/source-fingerprint.txt" "$evidence_directory/source-fingerprint-after.txt"; then
  printf '%s\n' '{"status":"gap","case":"source-changed","detail":"Source changed during this observation; declare a stable target before reusing it."}' | tee -a "$evidence_directory/observations.jsonl"
  exit 3
fi
exit "$journey_exit"

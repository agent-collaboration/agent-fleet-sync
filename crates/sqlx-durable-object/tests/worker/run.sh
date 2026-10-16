#!/usr/bin/env bash
# Builds the test Worker, serves it with `wrangler dev --local` on fresh local
# Durable Object storage, and runs `tests/driver.rs` against it. Part of what
# `make test-worker` runs.
#
# Fresh means a temporary `--persist-to` directory: the scenarios assume the
# schema in schema.sql and nothing else, and storage left over from the last
# run would be neither. The object creates that schema itself -- there is no
# migrations step to run first.
set -euo pipefail

cd "$(dirname "$0")"
port="${DO_WORKER_PORT:-8788}"
state="$(mktemp -d)"
export WRANGLER_SEND_METRICS=false

worker-build --release

wrangler dev --local --persist-to "$state" --port "$port" >"$state/wrangler.log" 2>&1 &
wrangler=$!
trap 'kill "$wrangler" 2>/dev/null || true; rm -rf "$state"' EXIT

for _ in $(seq 1 60); do
  if curl -sf "http://localhost:$port/" >/dev/null; then
    break
  fi
  if ! kill -0 "$wrangler" 2>/dev/null; then
    cat "$state/wrangler.log"
    exit 1
  fi
  sleep 1
done

cd ../..
DO_WORKER_URL="http://localhost:$port" cargo test --test driver -- --nocapture

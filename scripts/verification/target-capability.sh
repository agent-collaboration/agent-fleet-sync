#!/usr/bin/env bash
# SO53: target schema declarations, refusal replay, and fresh admission control.
set -euo pipefail
exec bash "$(dirname "${BASH_SOURCE[0]}")/run-native.sh" target-capability "$@"

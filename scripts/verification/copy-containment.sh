#!/usr/bin/env bash
# SO57: a damaged copy and healthy copy share one real native connection.
set -euo pipefail
exec bash "$(dirname "${BASH_SOURCE[0]}")/run-native.sh" copy-containment "$@"

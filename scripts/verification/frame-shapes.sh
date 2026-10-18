#!/usr/bin/env bash
# SO56: correlated malformed-body answer, same-session recovery, envelope close.
set -euo pipefail
exec bash "$(dirname "${BASH_SOURCE[0]}")/run-native.sh" frame-shapes "$@"

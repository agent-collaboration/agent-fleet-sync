#!/usr/bin/env bash
# SO52: recorded holder refusal detail and replay through the real registry.
set -euo pipefail
exec bash "$(dirname "${BASH_SOURCE[0]}")/run-native.sh" holder-refusals "$@"

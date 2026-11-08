#!/usr/bin/env bash
set -euo pipefail
SPIKE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPIKE_ARTIFACTS="${SPIKE_ARTIFACTS:-$SPIKE_ROOT/../artifacts/v-sdk-spike}"
mkdir -p "$SPIKE_ARTIFACTS"
SPIKE_ARTIFACTS="$(cd "$SPIKE_ARTIFACTS" && pwd)"
export SPIKE_ARTIFACTS
SPIKE_V="$SPIKE_ARTIFACTS/tools/v/v"
SPIKE_RUST="$SPIKE_ARTIFACTS/tools/sdk-rust"
export VMODULES="$SPIKE_ARTIFACTS/vmodules"
export CARGO_TARGET_DIR="$SPIKE_ARTIFACTS/rust-target"
export V_C_ERROR_BUG_REPORT_DISABLED=1

# Keep generated schema builds independent of stateful compiler module caches.
# The full generic checker remains enabled and needs its own build memory.
export VFLAGS="${VFLAGS:-} -nocache"

# Never download or invoke an unpinned compatibility compiler after a V3 error.
export V_MACOS_V3_NO_FALLBACK=1

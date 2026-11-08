#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
mkdir -p "$SPIKE_ARTIFACTS/logs"
python3 "$SPIKE_ROOT/scripts/prepare_native_source.py"
profile="${SPIKE_BUILD_PROFILE:-release}"
cargo_flags=''
case "$profile" in
    debug) ;;
    release) cargo_flags='--release' ;;
    *) echo 'SPIKE_BUILD_PROFILE must be debug or release' >&2; exit 1 ;;
esac
RUSTUP_TOOLCHAIN="${SPIKE_RUST_TOOLCHAIN:-1.99.1}" "${CARGO_BIN:-$HOME/.cargo/bin/cargo}" rustc \
    --manifest-path "$SPIKE_RUST/Cargo.toml" --locked -p temporalio-sdk-core-c-bridge \
    --config 'profile.dev.package.temporalio-client.debug=0' \
    --config 'profile.dev.package.temporalio-sdk-core.debug=0' \
    $cargo_flags --features temporalio-client/vendored-protox -- \
    -C debuginfo=0 -C split-debuginfo=off
python3 "$SPIKE_ROOT/scripts/build_payload_bridge.py" --profile "$profile" --library-only
python3 "$SPIKE_ROOT/scripts/verify_core_source.py" "$SPIKE_RUST"

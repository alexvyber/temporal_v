#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

# These revisions are the inputs used for the recorded experiment.
v_revision=213eb9b920b91bcbb2d24c5b98a48d3f49137a42
vc_revision=a5a8c9313a3e5ff07061dce993598ee2b326b4ea
protobuf_revision=64811280130b2b4bda45eba707512412ace968ad
core_revision=67174ea96e8a06bc33137036e73cfaf508aa9dd7
mkdir -p "$SPIKE_ARTIFACTS/tools" "$SPIKE_ARTIFACTS/logs" "$VMODULES"
python3 "$SPIKE_ROOT/scripts/prepare_formatter.py"

fetch_source() {
    local repository="$1" revision="$2" destination="$3"
    if [ ! -d "$destination" ]; then
        mkdir -p "$destination"
        curl --fail --silent --show-error --location \
            "https://codeload.github.com/$repository/tar.gz/$revision" \
            -o "$SPIKE_ARTIFACTS/tools/${repository##*/}-$revision.tar.gz"
        tar -xzf "$SPIKE_ARTIFACTS/tools/${repository##*/}-$revision.tar.gz" \
            -C "$destination" --strip-components=1
    fi
}

fetch_source vlang/v "$v_revision" "$SPIKE_ARTIFACTS/tools/v"
fetch_source vlang/vc "$vc_revision" "$SPIKE_ARTIFACTS/tools/v/vc"
python3 "$SPIKE_ROOT/scripts/prepare_v_runtime.py" "$SPIKE_ARTIFACTS/tools/v" > "$SPIKE_ARTIFACTS/logs/v-build.log" 2>&1

fetch_source we-be/protobuf.v "$protobuf_revision" "$VMODULES/protobuf"
if ! grep -q 'fn (g &Gen) oneof_condition' "$VMODULES/protobuf/gen/codegen.v"; then
    patch -d "$VMODULES/protobuf" -p1 < "$SPIKE_ROOT/patches/oneof-case-names.patch"
fi

if [ ! -f "$SPIKE_RUST/Cargo.toml" ]; then
    core_source="${SDK_RUST_SOURCE:-$SPIKE_ROOT/../../repos/sdk-rust}"
    if git -C "$core_source" cat-file -e "$core_revision" 2>/dev/null; then
        mkdir -p "$SPIKE_RUST"
        git -C "$core_source" archive "$core_revision" | tar -x -C "$SPIKE_RUST"
    else
        fetch_source temporalio/sdk-rust "$core_revision" "$SPIKE_RUST"
    fi
fi

if [ ! -x "$SPIKE_ARTIFACTS/tools/temporal" ]; then
    case "$(uname -sm)" in
        'Darwin arm64') platform=darwin_arm64 ;;
        'Linux x86_64') platform=linux_amd64 ;;
        'Linux aarch64') platform=linux_arm64 ;;
        *) echo 'Unsupported host for this spike bootstrap' >&2; exit 1 ;;
    esac
    curl --fail --silent --show-error --location \
        "https://github.com/temporalio/cli/releases/download/v1.10.0/temporal_cli_1.10.0_$platform.tar.gz" \
        -o "$SPIKE_ARTIFACTS/tools/temporal.tar.gz"
    tar -xzf "$SPIKE_ARTIFACTS/tools/temporal.tar.gz" -C "$SPIKE_ARTIFACTS/tools"
fi
python3 "$SPIKE_ROOT/scripts/install_test_server.py"
printf 'Toolchain ready in %s\n' "$SPIKE_ARTIFACTS"

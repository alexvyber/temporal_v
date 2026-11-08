#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
python3 "$SPIKE_ROOT/scripts/prepare_formatter.py" --check
python3 "$SPIKE_ROOT/scripts/prepare_v_runtime.py" "$SPIKE_ARTIFACTS/tools/v" --check
build_log=''
trap 'task_build_status=$?; if [ -n "$build_log" ] && [ -f "$build_log" ]; then tail -100 "$build_log" >&2; fi; exit "$task_build_status"' ERR
mkdir -p "$SPIKE_ARTIFACTS/logs" "$SPIKE_ROOT/temporal"
python3 "$SPIKE_ROOT/scripts/install_protocol_server.py"

if grep -q 'let _value_str' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/lib.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/binary-metadata.patch"
fi
if ! grep -q 'protocol(match otel_options.protocol' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/runtime.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/otlp-protocol.patch"
fi
if grep -q 'cancel_token.cancelled() => Err(anyhow::anyhow!' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/client.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/rpc-cancellation.patch"
fi
if grep -q 'TODO: https://github.com/temporalio/sdk-core/issues/867' "$SPIKE_RUST/crates/sdk-core/src/worker/client.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/scripts/core-patches/query-failure.patch"
fi
if grep -q 'fn borrow_buf(&mut self)' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/runtime.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/shared-runtime.patch"
fi
if ! grep -q 'disable_host_verification: c.disable_host_verification' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/envconfig.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/config-profile.patch"
fi
if ! grep -q 'grpc_override_callback_user_data_free' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/client.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/grpc-transport.patch"
fi
if ! grep -q 'trait ActivitySlot' "$SPIKE_RUST/crates/client/src/worker.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/eager-standalone.patch"
fi
if ! grep -q 'meter: \*const CustomMetricMeter,' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/metric.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/custom-metrics.patch"
fi
if ! grep -q 'legacy_versioning: bool' "$SPIKE_RUST/crates/sdk-core/src/worker/mod.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/legacy-eager.patch"
fi
if grep -q 'let new_met = new_req.metadata_mut();' "$SPIKE_RUST/crates/client/src/grpc.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/rpc-metadata.patch"
fi
if ! grep -q 'fn temporal_core_protocol_capabilities' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/lib.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/workflow-markers.patch"
fi
if ! grep -q 'pub mod trace;' "$SPIKE_RUST/crates/sdk-core-c-bridge/src/lib.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/tracing.patch"
fi
if ! grep -q 'last_workflow_marker_scan_event_id' "$SPIKE_RUST/crates/sdk-core/src/worker/workflow/machines/workflow_machines.rs"; then
    patch -d "$SPIKE_RUST" -p1 < "$SPIKE_ROOT/patches/workflow-marker-replay.patch"
fi
if ! cmp -s "$SPIKE_ROOT/native/include/temporal-sdk-core-c-bridge.h" "$SPIKE_RUST/crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h"; then
    echo 'Vendored Core header does not match the pinned Rust source' >&2
    exit 1
fi
cp "$SPIKE_ROOT/locks/core.Cargo.lock" "$SPIKE_RUST/Cargo.lock"
python3 "$SPIKE_ROOT/scripts/verify_core_source.py" "$SPIKE_RUST"
profile="${SPIKE_BUILD_PROFILE:-debug}"
cargo_flags=''
v_flags=''
case "$profile" in
    debug) ;;
    release) cargo_flags='--release'; v_flags='-prod' ;;
    *) echo 'SPIKE_BUILD_PROFILE must be debug or release' >&2; exit 1 ;;
esac

build_log="$SPIKE_ARTIFACTS/logs/core-build.log"
RUSTUP_TOOLCHAIN="${SPIKE_RUST_TOOLCHAIN:-1.99.1}" "${CARGO_BIN:-$HOME/.cargo/bin/cargo}" rustc \
    --manifest-path "$SPIKE_RUST/Cargo.toml" --locked -p temporalio-sdk-core-c-bridge \
    --config 'profile.dev.package.temporalio-client.debug=0' \
    --config 'profile.dev.package.temporalio-sdk-core.debug=0' \
    $cargo_flags \
    --features temporalio-client/vendored-protox -- -C debuginfo=0 -C split-debuginfo=off > "$SPIKE_ARTIFACTS/logs/core-build.log" 2>&1

if grep -q 'attr := if fld.deprecated' "$VMODULES/protobuf/gen/codegen.v"; then
    patch -d "$VMODULES/protobuf" -p1 < "$SPIKE_ROOT/scripts/codegen/protobuf-deprecated-fields.patch"
fi

python3 "$SPIKE_ROOT/scripts/verify_core_source.py" "$SPIKE_RUST"

python3 "$SPIKE_ROOT/scripts/harden_protobuf.py" --dependency "$VMODULES/protobuf"

build_log="$SPIKE_ARTIFACTS/logs/generator-build.log"
"$SPIKE_V" -d use_bundled_libgc -o "$SPIKE_ARTIFACTS/tools/vpbgen" \
    "$VMODULES/protobuf/cmd/vpbgen" > "$SPIKE_ARTIFACTS/logs/generator-build.log" 2>&1

build_log="$SPIKE_ARTIFACTS/logs/core-generation.log"
python3 "$SPIKE_ROOT/scripts/generate_protobuf.py" > "$SPIKE_ARTIFACTS/logs/core-generation.log" 2>&1

python3 "$SPIKE_ROOT/scripts/generate_service.py" --rust "$SPIKE_RUST"
python3 "$SPIKE_ROOT/scripts/generate_activation_decoder.py"

build_log="$SPIKE_ARTIFACTS/logs/payload-bridge-build.log"
python3 "$SPIKE_ROOT/scripts/build_payload_bridge.py" --profile "$profile" > "$build_log" 2>&1
python3 "$SPIKE_ROOT/scripts/format_v.py" "$SPIKE_ROOT/corebridge/payloads.v" "$SPIKE_ROOT/sdk/external_storage.v" "$SPIKE_ROOT/sdk/storage_worker.v" "$SPIKE_ROOT/sdk/storage_targets.v" > "$SPIKE_ARTIFACTS/logs/storage-format.log" 2>&1

case "$(uname -s)" in
    Darwin) link_flags="$CARGO_TARGET_DIR/$profile/libtemporalio_sdk_core_c_bridge.dylib $CARGO_TARGET_DIR/$profile/libtemporal_v_payloads.dylib -Wl,-rpath,$CARGO_TARGET_DIR/$profile" ;;
    Linux) link_flags="-L$CARGO_TARGET_DIR/$profile -ltemporalio_sdk_core_c_bridge -ltemporal_v_payloads -Wl,-rpath,\$ORIGIN/../lib -Wl,-rpath,$CARGO_TARGET_DIR/$profile" ;;
    *) echo 'Unsupported linker platform' >&2; exit 1 ;;
esac
python3 "$SPIKE_ROOT/scripts/verification.py" build-start
build_log="$SPIKE_ARTIFACTS/logs/worker-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-worker" worker) > "$SPIKE_ARTIFACTS/logs/worker-build.log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-worker"
build_log="$SPIKE_ARTIFACTS/logs/workflow-worker-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-workflow-worker" workflow_worker) > "$SPIKE_ARTIFACTS/logs/workflow-worker-build.log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-workflow-worker"
build_log="$SPIKE_ARTIFACTS/logs/client-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-client" client_demo) > "$SPIKE_ARTIFACTS/logs/client-build.log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-client"

build_log="$SPIKE_ARTIFACTS/logs/replay-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-replay" replay_demo) > "$SPIKE_ARTIFACTS/logs/replay-build.log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-replay"

build_log="$SPIKE_ARTIFACTS/logs/service-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-service" service_demo) > "$SPIKE_ARTIFACTS/logs/service-build.log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-service"

build_log="$SPIKE_ARTIFACTS/logs/nexus-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-nexus-worker" nexus_worker) > "$SPIKE_ARTIFACTS/logs/nexus-build.log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-nexus-worker"

build_log="$SPIKE_ARTIFACTS/logs/environment-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-environment" environment_demo) > "$SPIKE_ARTIFACTS/logs/environment-build.log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-environment"
build_log="$SPIKE_ARTIFACTS/logs/storage-demo-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-storage" storage_demo) > "$build_log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-storage"
build_log="$SPIKE_ARTIFACTS/logs/plugin-demo-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-plugins" plugin_demo) > "$build_log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-plugins"
build_log="$SPIKE_ARTIFACTS/logs/transport-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-transport" transport_demo) > "$build_log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-transport"
build_log="$SPIKE_ARTIFACTS/logs/metrics-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-metrics" metrics_demo) > "$build_log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-metrics"
build_log="$SPIKE_ARTIFACTS/logs/user-metrics-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-user-metrics" user_metrics_demo) > "$build_log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-user-metrics"
build_log="$SPIKE_ARTIFACTS/logs/profiles-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags \
    -ldflags "$link_flags" \
    -o "$SPIKE_ARTIFACTS/tools/v-profiles" profiles_demo) > "$build_log" 2>&1
printf 'Built %s\n' "$SPIKE_ARTIFACTS/tools/v-profiles"
build_log="$SPIKE_ARTIFACTS/logs/grpc-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-grpc" grpc_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/eager-activity-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-eager-activity" eager_activity_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/metric-export-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-metric-export" metric_export_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/deployment-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-deployment" deployment_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/legacy-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-legacy" legacy_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/schedule-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-schedule" schedule_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/batch-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-batch" batch_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/protobuf-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-protobuf" protobuf_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/mixed-values-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-mixed-values" mixed_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/protobuf-json-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-protobuf-json" protobuf_json_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/typed-values-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-typed-values" details_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/raw-history-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-raw-history" history_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/wait-deadlines-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-wait-deadlines" wait_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/metadata-transport-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-metadata-transport" metadata_transport_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/visibility-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-visibility" visibility_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/concurrency-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-concurrency" concurrency_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/management-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-management" management_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/workflow-pause-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-workflow-pause" pause_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/replay-batch-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-replay-batch" replay_batch_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/side-effects-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-side-effects" side_effect_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/tracing-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-tracing" tracing_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/panic-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-panic" panic_demo) > "$build_log" 2>&1
build_log="$SPIKE_ARTIFACTS/logs/codec-ownership-build.log"
(cd "$SPIKE_ROOT" && "$SPIKE_V" -d use_bundled_libgc $v_flags -ldflags "$link_flags" -o "$SPIKE_ARTIFACTS/tools/v-codec-ownership" codec_ownership_demo) > "$build_log" 2>&1
python3 "$SPIKE_ROOT/scripts/build_replay_baseline_v1.py"
python3 "$SPIKE_ROOT/scripts/api_contract.py"
python3 "$SPIKE_ROOT/scripts/verification.py" build

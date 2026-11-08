"""Instrument V-generated C and the native shim with Address/UndefinedBehaviorSanitizer.

The prebuilt Rust Core library is not instrumented by this check. Bundled Boehm
C inherits the sanitizer flags, but ASAN cannot track individual objects inside
its custom heap. Leak checking is deliberately disabled for collector ownership.
"""

RESULT_REPORT = 'sanitizer-results.json'
import json
import os
import platform
import subprocess
import sys
import tempfile
from validation_artifacts import InstrumentedBuilds
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI


def main():
    report = {'passed': False, 'cases': [], 'instrumented': ['V-generated C', 'native/core_shim.c', 'native/slot_bridge.c', 'native/grpc_bridge.c', 'native/metric_export.c', 'bundled Boehm C'],
              'not_instrumented': ['Rust Core', 'Rust payload visitor'],
              'limitations': ['ASAN does not track individual Boehm heap-object bounds; collector leak checking is disabled']}
    try:
        compiler = ARTIFACTS / 'tools/v/v'
        builds=InstrumentedBuilds(ARTIFACTS,compiler)
        binary = ARTIFACTS / 'tools/v-service-sanitized'
        environment = dict(os.environ, V_MACOS_V3_NO_FALLBACK="1", VMODULES=str(ARTIFACTS / 'vmodules'), V_C_ERROR_BUG_REPORT_DISABLED='1')
        extension = 'dylib' if platform.system() == 'Darwin' else 'so'
        library = ARTIFACTS / f'rust-target/debug/libtemporalio_sdk_core_c_bridge.{extension}'
        if os.environ.get('SPIKE_BUILD_PROFILE') == 'release':
            library = ARTIFACTS / f'rust-target/release/libtemporalio_sdk_core_c_bridge.{extension}'
        flags = '-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer'
        if platform.system() == 'Linux' and platform.machine() == 'x86_64':
            # Under amd64 emulation, Boehm's raw mmap can land beyond ASAN's
            # supported application ranges. A supported high-memory hint fixes
            # placement without suppressing instrumentation or diagnostics.
            flags += ' -DHEAP_START=0x300000000000'
            report['boehm_heap_hint'] = '0x300000000000'
        unit_environment=dict(environment,ASAN_OPTIONS='detect_leaks=0:halt_on_error=1',UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
        for test_name in ['panic_stack_test.v', 'random_stream_test.v']:
            with (ARTIFACTS/('logs/sanitizer-unit-'+test_name+'.log')).open('w') as output:
                tested=subprocess.run([str(compiler),'-d','use_bundled_libgc','-cc','clang','-cflags',flags,'-ldflags',f'{flags} {library} {library.parent / ("libtemporal_v_payloads."+extension)} -Wl,-rpath,{library.parent}',str(ROOT/'_tests'/test_name)],cwd=ROOT,env=unit_environment,stdout=output,stderr=subprocess.STDOUT,timeout=180)
            assert tested.returncode==0,'instrumented SDK unit failed: '+test_name
        report['cases'].append('Original panic stacks retained C-heap/thread-local ownership through nested recovery, protected failure conversion and exception export under instrumentation')
        with (ARTIFACTS / 'logs/sanitizer-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}', '-o', str(binary), 'service_demo'],
                                      cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'sanitizer compilation failed; see sanitizer-build.log'
        with TemporalFixture('sanitizer') as fixture:
            fixture.workflows(); fixture.activities()
            environment = dict(fixture.env, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
            for mode in ('native-validation', 'request-identities', 'eager-workflows', 'versioning-validation', 'dynamic-validation', 'nexus-links', 'service', 'operator', 'schedules', 'updates', 'concurrency', 'cancellation', 'failure-chains'):
                result = subprocess.run([str(binary), '--mode', mode], cwd=ROOT, env=environment,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / f'logs/sanitizer-{mode}.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-3000:]
                report['cases'] += json.loads(result.stdout)
            report['cases'].append('Address/UB sanitizers reported no errors in instrumented RPC/callback/buffer paths and concurrent close scenarios')
        with TemporalFixture('sanitizer-custom-tuners') as fixture:
            fixture.env['TEMPORAL_LOG_FORMAT'] = 'json'
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default',
                                      '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            environment = dict(fixture.env, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
            result = subprocess.run([str(binary), '--mode', 'custom-tuners'], cwd=ROOT, env=environment,
                                    capture_output=True, text=True, timeout=90)
            (ARTIFACTS / 'logs/sanitizer-custom-tuners.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr[-4000:]
            report['cases'] += json.loads(result.stdout)
        with tempfile.TemporaryDirectory(prefix='v-sanitizer-standalone-') as token_dir, TemporalFixture('sanitizer-standalone') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='binary-prefix', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded',
                               V_SDK_EXAMPLE_ASYNC_TOKEN_DIR=token_dir)
            worker = fixture.activities('before')
            environment = dict(fixture.env, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
            for mode in ('standalone-start', 'standalone-finish'):
                result = subprocess.run([str(binary), '--mode', mode], cwd=ROOT, env=environment,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / f'logs/sanitizer-{mode}.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-4000:]
                report['cases'] += json.loads(result.stdout)
                if mode == 'standalone-start':
                    worker.kill(); assert worker.wait(timeout=5) == -9
                    worker = fixture.activities('after')
            worker.terminate(); assert worker.wait(timeout=15) == 0
        with TemporalFixture('sanitizer-nexus-client') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='binary-prefix', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default',
                                      '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            worker = fixture.workflows('before')
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-before')
            environment = dict(fixture.env, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
            for mode in ('nexus-client-start', 'nexus-client-finish'):
                result = subprocess.run([str(binary), '--mode', mode], cwd=ROOT, env=environment,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / f'logs/sanitizer-{mode}.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-4000:]
                report['cases'] += json.loads(result.stdout)
                if mode == 'nexus-client-start':
                    for process in (worker, nexus):
                        process.kill(); assert process.wait(timeout=5) == -9
                    worker = fixture.workflows('after')
                    nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-after')
            for process in (worker, nexus):
                process.terminate(); assert process.wait(timeout=15) == 0
        with tempfile.TemporaryDirectory(prefix='v-sanitizer-nexus-activity-') as token_dir, TemporalFixture(
                'sanitizer-nexus-activity', server_binary=ARTIFACTS / 'tools/temporal-protocol',
                server_args=('--dynamic-config-value', 'activity.enableCallbacks=true', '--dynamic-config-value', 'history.enableUpdateCallbacks=true')) as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded',
                               V_SDK_EXAMPLE_ASYNC_TOKEN_DIR=token_dir, V_SDK_EXAMPLE_NEXUS_RETRY_DIRECTORY=token_dir)
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default',
                                      '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            worker = fixture.activities('before')
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-before')
            environment = dict(fixture.env, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
            for mode in ('nexus-activity-start', 'nexus-activity-finish'):
                result = subprocess.run([str(binary), '--mode', mode], cwd=ROOT, env=environment,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / f'logs/sanitizer-{mode}.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-4000:]
                report['cases'] += json.loads(result.stdout)
                if mode.endswith('start'):
                    for process in (worker, nexus):
                        process.kill(); assert process.wait(timeout=5) == -9
                    worker = fixture.activities('after')
                    nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-after')
            workflow = fixture.workflows('workflow-before')
            for mode in ('nexus-update-start', 'nexus-update-finish', 'nexus-interactions'):
                result = subprocess.run([str(binary), '--mode', mode], cwd=ROOT, env=environment,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / f'logs/sanitizer-{mode}.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-4000:]
                report['cases'] += json.loads(result.stdout)
                if mode == 'nexus-update-start':
                    for process in (workflow, nexus):
                        process.kill(); assert process.wait(timeout=5) == -9
                    workflow = fixture.workflows('workflow-after')
                    nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-update-after')
            for process in (worker, nexus, workflow):
                process.terminate(); assert process.wait(timeout=15) == 0
        with TemporalFixture('sanitizer-lifecycle') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='binary-prefix', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            worker = fixture.workflows()
            environment = dict(fixture.env, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
            result = subprocess.run([str(binary), '--mode', 'workflow-lifecycle'], cwd=ROOT, env=environment,
                                    capture_output=True, text=True, timeout=60)
            (ARTIFACTS / 'logs/sanitizer-workflow-lifecycle.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr[-4000:]
            report['cases'] += json.loads(result.stdout)
            worker.terminate(); assert worker.wait(timeout=15) == 0
        metrics_binary = ARTIFACTS / 'tools/v-user-metrics-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-user-metrics-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(metrics_binary), 'user_metrics_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'user metric sanitizer compilation failed'
        environment = dict(os.environ, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
        with (ARTIFACTS / 'logs/sanitizer-user-metrics.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/user_metrics_probe.py'), '--binary', str(metrics_binary),
                                     '--report', 'user-metrics-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=180)
        assert result.returncode == 0, 'user metric sanitizer probe failed'
        metrics_report = json.loads((ARTIFACTS / 'user-metrics-sanitized-results.json').read_text())
        assert metrics_report['passed']
        report['user_metrics'] = metrics_report
        report['cases'].append('User metric scope/runtime leases, concurrent recording/close and restart replay passed with instrumented V/C')
        profiles_binary = ARTIFACTS / 'tools/v-profiles-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-profiles-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(profiles_binary), 'profiles_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'profile sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-profiles.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/profiles_probe.py'), '--binary', str(profiles_binary),
                                     '--report', 'profiles-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=180)
        assert result.returncode == 0, 'profile sanitizer probe failed'
        profiles_report = json.loads((ARTIFACTS / 'profiles-sanitized-results.json').read_text())
        assert profiles_report['passed']
        report['profiles'] = profiles_report
        report['cases'].append('Profile success/error buffer ownership, real client/TLS setup and restart replay passed with instrumented V/C')
        grpc_unit = ARTIFACTS / 'tools/grpc-bridge-sanitized'
        compiled = builds.compile(['clang', '-std=c11', *flags.split(), '-Wall', '-Wextra', '-Werror', '-I', str(ROOT),
                                   str(ROOT / 'native/grpc_bridge.c'), str(ROOT / '_tests/native/grpc_bridge_test.c'),
                                   '-pthread', '-o', str(grpc_unit)], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=60)
        assert compiled.returncode == 0, compiled.stderr
        result = subprocess.run([str(grpc_unit)], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        report['cases'].append('Native custom gRPC queues balanced 2,007 owned requests under concurrent producers, bounded counts/bytes, class priority, cancellation, zero-byte replies and every owner-release order')
        grpc_binary = ARTIFACTS / 'tools/v-grpc-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-grpc-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(grpc_binary), 'grpc_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'custom gRPC sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-grpc.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/grpc_transport_probe.py'), '--binary', str(grpc_binary),
                                     '--report', 'grpc-transport-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=180)
        assert result.returncode == 0, 'custom gRPC sanitizer probe failed'
        report['grpc_transport'] = json.loads((ARTIFACTS / 'grpc-transport-sanitized-results.json').read_text())
        assert report['grpc_transport']['passed']
        report['cases'].append('Custom gRPC queue, V dispatch, cancellation, shared-client ownership and restart replay passed with instrumented V/C')
        eager_binary = ARTIFACTS / 'tools/v-eager-activity-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-eager-activity-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(eager_binary), 'eager_activity_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'eager standalone sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-eager-activity.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/eager_standalone_probe.py'), '--binary', str(eager_binary),
                                     '--report', 'eager-standalone-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=180)
        assert result.returncode == 0, 'eager standalone sanitizer probe failed'
        report['eager_standalone'] = json.loads((ARTIFACTS / 'eager-standalone-sanitized-results.json').read_text())
        assert report['eager_standalone']['passed']
        report['cases'].append('Inline standalone Activity dispatch, reservation fallback/cleanup and worker-loss heartbeat recovery passed with instrumented V/C')
        metric_unit = ARTIFACTS / 'tools/metric-export-sanitized'
        compiled = builds.compile(['clang', '-std=c11', *flags.split(), '-Wall', '-Wextra', '-Werror', '-I', str(ROOT),
                                   str(ROOT / 'native/metric_export.c'), str(ROOT / '_tests/native/metric_export_test.c'),
                                   '-pthread', '-o', str(metric_unit)], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=60)
        assert compiled.returncode == 0, compiled.stderr
        result = subprocess.run([str(metric_unit)], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        report['cases'].append('Custom metric queue covered 16,015 record attempts with typed labels, immutable overrides, u64/signed/duration ranges, count/byte limits, concurrent producers, independent runtimes and balanced ownership')
        metric_binary = ARTIFACTS / 'tools/v-metric-export-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-metric-export-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(metric_binary), 'metric_export_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'custom metric exporter sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-metric-export.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/metric_export_probe.py'), '--binary', str(metric_binary),
                                     '--report', 'metric-export-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=180)
        assert result.returncode == 0, 'custom metric exporter sanitizer probe failed'
        report['metric_export'] = json.loads((ARTIFACTS / 'metric-export-sanitized-results.json').read_text())
        assert report['metric_export']['passed']
        report['cases'].append('V metric exporter records, faults, late meter/self-close lifetime and workflow replay passed with instrumented V/C')
        deployment_binary = ARTIFACTS / 'tools/v-deployment-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-deployment-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(deployment_binary), 'deployment_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'deployment sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-deployment.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/deployment_probe.py'), '--binary', str(deployment_binary),
                                     '--report', 'deployment-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'deployment sanitizer probe failed'
        report['deployment'] = json.loads((ARTIFACTS / 'deployment-sanitized-results.json').read_text())
        assert report['deployment']['passed']
        report['cases'].append('Typed execution/child overrides, ramps/conflict tokens, continue-as-new inheritance and worker-loss/offline replay passed against both server versions with instrumented V/C')
        legacy_binary = ARTIFACTS / 'tools/v-legacy-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-legacy-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(legacy_binary), 'legacy_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'legacy versioning sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-legacy.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/legacy_versioning_probe.py'), '--binary', str(legacy_binary),
                                     '--report', 'legacy-versioning-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'legacy versioning sanitizer probe failed'
        report['legacy_versioning'] = json.loads((ARTIFACTS / 'legacy-versioning-sanitized-results.json').read_text())
        assert report['legacy_versioning']['passed']
        report['cases'].append('Legacy compatibility routing, safe eager fallback and worker-loss/offline replay passed against both server versions with instrumented V/C')
        schedule_binary = ARTIFACTS / 'tools/v-schedule-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-schedule-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(schedule_binary), 'schedule_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'schedule sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-schedule.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/schedule_probe.py'), '--binary', str(schedule_binary),
                                     '--report', 'schedule-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'schedule sanitizer probe failed'
        report['schedule'] = json.loads((ARTIFACTS / 'schedule-sanitized-results.json').read_text())
        assert report['schedule']['passed']
        report['cases'].append('Schedule lifecycle, encoded actions, codecs, concurrent backfills, conflict/memo semantics, restart and offline replay passed against both server versions with instrumented V/C')
        batch_binary = ARTIFACTS / 'tools/v-batch-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-batch-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(batch_binary), 'batch_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'batch sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-batch.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/batch_probe.py'), '--binary', str(batch_binary),
                                     '--report', 'batch-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'batch sanitizer probe failed'
        report['batch'] = json.loads((ARTIFACTS / 'batch-sanitized-results.json').read_text())
        assert report['batch']['passed']
        report['cases'].append('Batch lifecycle, query/explicit targets, payloads, cancellation/termination, stop/deadline behavior and restart/offline replay passed against both server versions with instrumented V/C')
        protobuf_binary = ARTIFACTS / 'tools/v-protobuf-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-protobuf-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(protobuf_binary), 'protobuf_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'protobuf sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-protobuf.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/protobuf_probe.py'), '--binary', str(protobuf_binary),
                                     '--report', 'protobuf-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'protobuf sanitizer probe failed'
        report['protobuf'] = json.loads((ARTIFACTS / 'protobuf-sanitized-results.json').read_text())
        assert report['protobuf']['passed']
        report['cases'].append('Typed protobuf payloads, decoder oracle vectors, unknown fields, codecs, failure details, worker-loss and offline replay passed against both server versions with instrumented V/C')
        protobuf_json_binary = ARTIFACTS / 'tools/v-protobuf-json-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-protobuf-json-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(protobuf_json_binary), 'protobuf_json_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'protobuf_json sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-protobuf-json.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/protobuf_json_probe.py'), '--binary', str(protobuf_json_binary),
                                     '--report', 'protobuf-json-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'protobuf_json sanitizer probe failed'
        report['protobuf_json'] = json.loads((ARTIFACTS / 'protobuf-json-sanitized-results.json').read_text())
        assert report['protobuf_json']['passed']
        report['cases'].append('Typed protobuf JSON, exact numeric tokens, pinned Go vectors, codecs, failure details, worker-loss and offline replay passed against both server versions with instrumented V/C')
        mixed_binary = ARTIFACTS / 'tools/v-mixed-values-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-mixed-values-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(mixed_binary), 'mixed_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'mixed_values sanitizer compilation failed'
        with (ARTIFACTS / 'logs/sanitizer-mixed-values.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/mixed_values_probe.py'), '--binary', str(mixed_binary),
                                     '--report', 'mixed-values-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'mixed_values sanitizer probe failed'
        report['mixed_values'] = json.loads((ARTIFACTS / 'mixed-values-sanitized-results.json').read_text())
        assert report['mixed_values']['passed']
        report['cases'].append('Mixed per-value inputs, raw views, typed results, deferred completion, worker-loss and offline replay passed against both server versions with instrumented V/C')
        with (ARTIFACTS / 'logs/sanitizer-typed-callbacks.log').open('w') as output:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/typed_callbacks_probe.py'), '--binary', str(mixed_binary),
                                     '--report', 'typed-callbacks-sanitized-results.json'], cwd=ROOT, env=environment,
                                    stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert result.returncode == 0, 'typed callback sanitizer probe failed'
        report['typed_callbacks'] = json.loads((ARTIFACTS / 'typed-callbacks-sanitized-results.json').read_text())
        assert report['typed_callbacks']['passed']
        report['cases'].append('Typed query/Update/remote/local Activity callbacks and interceptor chains survived restart and offline replay against both servers with instrumented V/C')
        typed_binary = ARTIFACTS / 'tools/v-typed-values-sanitized'
        with (ARTIFACTS / 'logs/sanitizer-typed-values-build.log').open('w') as output:
            compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                       '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                       '-o', str(typed_binary), 'details_demo'], cwd=ROOT, env=environment,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=300)
        assert compiled.returncode == 0, 'typed value sanitizer compilation failed'
        for script, report_name, key in [('typed_details_probe.py', 'typed-details-sanitized-results.json', 'typed_details'), ('typed_nexus_probe.py', 'typed-nexus-sanitized-results.json', 'typed_nexus'), ('codec_counts_probe.py', 'codec-counts-sanitized-results.json', 'codec_counts'), ('typed_metadata_probe.py', 'typed-metadata-sanitized-results.json', 'typed_metadata'), ('cancellation_details_probe.py', 'cancellation-details-sanitized-results.json', 'cancellation_details'), ('nexus_failure_probe.py', 'nexus-failure-sanitized-results.json', 'nexus_failure')]:
            with (ARTIFACTS / f'logs/sanitizer-{key}.log').open('w') as output:
                tested = subprocess.run([sys.executable, str(ROOT / 'scripts' / script), '--binary', str(typed_binary), '--report', report_name], cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT, timeout=600)
            assert tested.returncode == 0, key + ' sanitizer probe failed'
            report[key] = json.loads((ARTIFACTS / report_name).read_text())
            assert report[key]['passed']
        report['cases'].append('Rich typed converters, nested/scoped failure details, checkpoint/cancellation/deferred APIs, Nexus request/results and all 210 offline histories passed both servers with instrumented V/C')
        for name, directory, script in [('raw-history', 'history_demo', 'raw_history_probe.py'), ('wait-deadline', 'wait_demo', 'wait_deadline_probe.py'), ('metadata-transport', 'metadata_transport_demo', 'metadata_transport_probe.py'), ('visibility', 'visibility_demo', 'visibility_probe.py'), ('concurrency', 'concurrency_demo', 'concurrency_probe.py'), ('workflow-management', 'management_demo', 'workflow_management_probe.py'), ('workflow-pause', 'pause_demo', 'workflow_pause_probe.py'), ('replay-batch', 'replay_batch_demo', 'replay_batch_probe.py'), ('side-effect', 'side_effect_demo', 'side_effect_probe.py')]:
            tested_binary = ARTIFACTS / ('tools/v-' + name + '-sanitized')
            with (ARTIFACTS / ('logs/sanitizer-' + name + '-build.log')).open('w') as output:
                compiled = builds.compile([str(compiler), '-d', 'use_bundled_libgc', '-cc', 'clang', '-cflags', flags,
                                           '-ldflags', f'{flags} {library} {library.parent / ("libtemporal_v_payloads." + extension)} -Wl,-rpath,{library.parent}',
                                           '-o', str(tested_binary), directory], cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT, timeout=300)
            assert compiled.returncode == 0, name + ' sanitizer compilation failed'
            report_name = name + '-sanitized-results.json'
            with (ARTIFACTS / ('logs/sanitizer-' + name + '.log')).open('w') as output:
                tested = subprocess.run([sys.executable, str(ROOT / 'scripts' / script), '--binary', str(tested_binary), '--report', report_name], cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT, timeout=1200 if name == 'side-effect' else 600)
            assert tested.returncode == 0, name + ' sanitizer probe failed'
            report[name] = json.loads((ARTIFACTS / report_name).read_text())
            assert report[name]['passed']
            if name == 'side-effect':
                for extra_script, extra_name in [('side_effect_errors_probe.py', 'side-effect-errors'), ('side_effect_go_probe.py', 'side-effect-go-replay')]:
                    extra_report = extra_name + '-sanitized-results.json'
                    with (ARTIFACTS / ('logs/sanitizer-' + extra_name + '.log')).open('w') as output:
                        tested = subprocess.run([sys.executable, str(ROOT / 'scripts' / extra_script), '--binary', str(tested_binary), '--report', extra_report], cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT, timeout=600)
                    assert tested.returncode == 0, extra_name + ' sanitizer probe failed'
                    report[extra_name] = json.loads((ARTIFACTS / extra_report).read_text())
                    assert report[extra_name]['passed']
        trace_binary=ARTIFACTS/'tools/v-tracing-sanitized'
        with (ARTIFACTS/'logs/sanitizer-tracing-build.log').open('w') as output:
            compiled=builds.compile([str(compiler),'-d','use_bundled_libgc','-cc','clang','-cflags',flags,'-ldflags',f'{flags} {library} {library.parent / ("libtemporal_v_payloads."+extension)} -Wl,-rpath,{library.parent}','-o',str(trace_binary),'tracing_demo'],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=300)
        assert compiled.returncode==0,'tracing sanitizer compilation failed'
        for script,key,arguments in [('trace_context_probe.py','trace-context',[]),('tracing_transport_probe.py','tracing-transport',[]),('tracing_transport_probe.py','tracing-typed',['--role','typed-export']),('tracing_tls_probe.py','tracing-tls',[]),('tracing_provider_probe.py','tracing-provider',[]),('tracing_client_probe.py','tracing-client',[]),('tracing_workflow_probe.py','tracing-workflow',[]),('tracing_nexus_probe.py','tracing-nexus',[]),('baggage_probe.py','baggage',[]),('baggage_e2e_probe.py','baggage-e2e',[]),('tracing_go_probe.py','tracing-go',[]),('sampling_probe.py','sampling',[]),('sampling_e2e_probe.py','sampling-e2e',[]),('sampling_nexus_probe.py','sampling-nexus',[]),('query_observation_probe.py','query-observation',[])]:
            report_name=key+'-sanitized-results.json'
            with (ARTIFACTS/('logs/sanitizer-'+key+'.log')).open('w') as output:
                tested=subprocess.run([sys.executable,str(ROOT/'scripts'/script),'--binary',str(trace_binary),'--report',report_name,*arguments],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=1200)
            assert tested.returncode==0,key+' sanitizer probe failed'
            report[key]=json.loads((ARTIFACTS/report_name).read_text());assert report[key]['passed']
        panic_binary=ARTIFACTS/'tools/v-panic-sanitized'
        with (ARTIFACTS/'logs/sanitizer-panic-build.log').open('w') as output:
            compiled=builds.compile([str(compiler),'-d','use_bundled_libgc','-cc','clang','-cflags',flags,'-ldflags',f'{flags} {library} {library.parent / ("libtemporal_v_payloads."+extension)} -Wl,-rpath,{library.parent}','-o',str(panic_binary),'panic_demo'],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=300)
        assert compiled.returncode==0,'panic sanitizer compilation failed'
        for script,key in [('panic_probe.py','panic'),('panic_nexus_probe.py','panic-nexus'),('panic_extensions_probe.py','panic-extensions'),('handler_lifecycle_probe.py','handler-lifecycle'),('handler_go_probe.py','handler-go'),('numeric_version_probe.py','numeric-version'),('dynamic_updates_probe.py','dynamic-updates'),('forced_task_probe.py','forced-task'),('random_stream_probe.py','random-stream'),('raw_interactions_probe.py','raw-interactions'),('raw_activities_probe.py','raw-activities'),('raw_heartbeat_probe.py','raw-heartbeat'),('raw_workflow_probe.py','raw-workflow'),('activity_client_probe.py','activity-client'),('activity_deadline_probe.py','activity-deadline'),('event_groups_probe.py','event-groups'),('context_values_probe.py','context-values'),('conditions_probe.py','conditions'),('raw_headers_probe.py','raw-headers'),('parent_handle_probe.py','parent-handle'),('workflow_headers_probe.py','workflow-headers'),('patch_policy_probe.py','patch-policy'),('worker_hooks_probe.py','worker-hooks'),('worker_completion_probe.py','worker-completion'),('worker_activation_probe.py','worker-activation'),('activity_wait_probe.py','activity-wait'),('readonly_view_probe.py','readonly-view'),('initializers_probe.py','initializers')]:
            report_name=key+'-sanitized-results.json'
            with (ARTIFACTS/('logs/sanitizer-'+key+'.log')).open('w') as output:
                tested=subprocess.run([sys.executable,str(ROOT/'scripts'/script),'--binary',str(panic_binary),'--report',report_name],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=1200)
            assert tested.returncode==0,key+' sanitizer probe failed'
            report[key]=json.loads((ARTIFACTS/report_name).read_text());assert report[key]['passed']
        ownership_unit=ARTIFACTS/'tools/v-codec-ownership-sanitized'
        with (ARTIFACTS/'logs/sanitizer-codec-ownership-build.log').open('w') as output:
            compiled=builds.compile([str(compiler),'-d','use_bundled_libgc','-cc','clang','-cflags',flags,'-ldflags',f'{flags} {library} {library.parent / ("libtemporal_v_payloads."+extension)} -Wl,-rpath,{library.parent}','-o',str(ownership_unit),'codec_ownership_demo'],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=300)
        assert compiled.returncode==0,'codec ownership sanitizer compilation failed'
        with (ARTIFACTS/'logs/sanitizer-codec-ownership.log').open('w') as output:
            tested=subprocess.run([sys.executable,str(ROOT/'scripts/codec_ownership_probe.py'),'--binary',str(panic_binary),'--unit-binary',str(ownership_unit),'--report','codec-ownership-sanitized-results.json'],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=1200)
        assert tested.returncode==0,'codec ownership sanitizer probe failed'
        report['codec-ownership']=json.loads((ARTIFACTS/'codec-ownership-sanitized-results.json').read_text());assert report['codec-ownership']['passed']
        nexus_context_binary=ARTIFACTS/'tools/v-nexus-context-sanitized'
        with (ARTIFACTS/'logs/sanitizer-nexus-context-build.log').open('w') as output:
            compiled=builds.compile([str(compiler),'-d','use_bundled_libgc','-cc','clang','-cflags',flags,'-ldflags',f'{flags} {library} {library.parent / ("libtemporal_v_payloads."+extension)} -Wl,-rpath,{library.parent}','-o',str(nexus_context_binary),'nexus_worker'],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=300)
        assert compiled.returncode==0,'Nexus context client sanitizer compilation failed'
        with (ARTIFACTS/'logs/sanitizer-nexus-context.log').open('w') as output:
            tested=subprocess.run([sys.executable,str(ROOT/'scripts/nexus_context_probe.py'),'--binary',str(nexus_context_binary),'--report','nexus-context-sanitized-results.json'],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=1200)
        assert tested.returncode==0,'Nexus context client sanitizer probe failed'
        report['nexus_context']=json.loads((ARTIFACTS/'nexus-context-sanitized-results.json').read_text());assert report['nexus_context']['passed']
        report['cases'].append('Native V recover isolated workflow/factory/command, query/validator, local/remote Activity and Nexus handler panics with retry, cleanup, both workflow panic policies, worker replacement and offline replay under V/C instrumentation')
        report['cases'].append('W3C Go reference corpus, typed/native OTLP with TLS/mTLS, bounded concurrent exporter lifecycle, shared client close and 108 restart/offline replay histories passed with instrumented V/C')
        report['cases'].append('Raw-history pagination/result/restart/offline paths and overall wait-deadline fault/live matrices passed with instrumented V/C')
        with (ARTIFACTS/'logs/sanitizer-extension-callbacks.log').open('w') as output:
            tested=subprocess.run([sys.executable,str(ROOT/'scripts/extension_callbacks_probe.py'),'--grpc-binary',str(grpc_binary),'--metric-binary',str(metric_binary),'--trace-binary',str(trace_binary),'--report','extension-callbacks-sanitized-results.json'],cwd=ROOT,env=environment,stdout=output,stderr=subprocess.STDOUT,timeout=600)
        assert tested.returncode==0,'extension callback sanitizer probe failed'
        report['extension-callbacks']=json.loads((ARTIFACTS/'extension-callbacks-sanitized-results.json').read_text());assert report['extension-callbacks']['passed']
        report['cases'].append('Custom gRPC call/connect and metric/trace export/shutdown panics preserved redaction, progress, user defers, closed contexts, native queue ownership and dispatcher drain under V/C instrumentation')
        report['instrumented_builds']=builds.finish()
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

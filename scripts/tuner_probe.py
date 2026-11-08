"""Native reservation races plus real V custom suppliers on all worker kinds."""

RESULT_REPORT = 'tuner-results.json'
import json
import os
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI


def main():
    report = {'passed': False, 'cases': []}
    try:
        native = ARTIFACTS / 'tools/slot-bridge-test'
        built = subprocess.run(['clang', '-std=c11', '-D_POSIX_C_SOURCE=200809L', '-g',
                                '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                                '-Wall', '-Wextra', '-Werror', '-I', str(ROOT), '-I', str(ROOT / 'native'),
                                str(ROOT / 'native/slot_bridge.c'), str(ROOT / '_tests/native/slot_bridge_test.c'),
                                '-pthread', '-o', str(native)], capture_output=True, text=True, timeout=60)
        assert built.returncode == 0, built.stderr
        tested = subprocess.run([str(native)], capture_output=True, text=True, timeout=30,
                                env=dict(os.environ, ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1'))
        assert tested.returncode == 0, tested.stderr
        report['cases'].append(tested.stdout.strip())
        with TemporalFixture('custom-tuners') as fixture:
            fixture.env['TEMPORAL_LOG_FORMAT'] = 'json'
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default',
                                      '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            driver = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'custom-tuners'], cwd=ROOT, env=fixture.env,
                                    capture_output=True, text=True, timeout=90)
            (ARTIFACTS / 'logs/custom-tuner-driver.log').write_text(driver.stdout + driver.stderr)
            assert driver.returncode == 0, driver.stderr[-5000:]
            report['cases'] += json.loads(driver.stdout)
            for suffix in ('blocked', 'counter', 'local', 'nexus', 'restart'):
                handle = {'workflow_id': fixture.queue + '-custom-' + suffix}
                fixture.assert_clean_history(handle)
                file = ARTIFACTS / f'binary-histories/custom-tuner-{suffix}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'],
                                         '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            report['cases'].append('Custom-supplier workflow/Activity/local retry/Nexus histories replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

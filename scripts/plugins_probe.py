"""Plugin snapshots, combined task kinds, worker metadata and crash/replay."""

RESULT_REPORT = 'plugins-results.json'
import json
import subprocess
import threading
from network_proxy import ConnectProxy
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI, counts

BINARY = ARTIFACTS / 'tools/v-plugins'


def configure_fixture(fixture):
    directory = ARTIFACTS / 'plugin-objects' / fixture.queue
    directory.mkdir(parents=True)
    fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_STORAGE_DIR=str(directory),
                       V_SDK_EXAMPLE_PLUGIN_METADATA_FILE=str(directory / 'worker-metadata.json'),
                       V_SDK_EXAMPLE_PLUGIN_ACTIVITY_FILE=str(directory / 'activities.log'))
    return directory


def main():
    report = {'passed': False, 'cases': []}
    try:
        for mask in (False, True):
            with TemporalFixture('plugins-' + ('masked' if mask else 'environment'), server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
                directory = configure_fixture(fixture)
                fixture.env['V_SDK_EXAMPLE_PLUGIN_OMIT_ENV'] = str(mask).lower()
                created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                          '--name', fixture.queue + '-endpoint', '--target-namespace', 'default', '--target-task-queue', fixture.queue],
                                         capture_output=True, text=True, timeout=20)
                assert created.returncode == 0, created.stderr
                result = subprocess.run([str(BINARY)], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=90)
                (ARTIFACTS / f'logs/plugins-{mask}-driver.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-5000:]
                metadata = json.loads((directory / 'worker-metadata.json').read_text())
                assert {item['name']: item['version'] for item in metadata['plugins']} == {'counter': '1.2.3', 'bundle': '0.1.0'}
                assert [item['type_'] for item in metadata['drivers']] == ['fixture.filesystem']
                assert metadata['task_queue'] == fixture.queue
                if mask: assert metadata.get('environment') is None
                else: assert metadata['environment']['runtimes'][0]['version'] == 'v-plugin-test'
                report['cases'].append(f'Plugin configuration snapshots and shared-client propagation installed workflows, local/remote Activities and Nexus on one Core worker; phase-specific errors, registration isolation and worker recreation passed; environment masking={mask}')
        with TemporalFixture('plugins-crash', server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            directory = configure_fixture(fixture)

            def call(role, *args):
                result = subprocess.run([str(BINARY), role, *map(str, args)], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=50)
                assert result.returncode == 0, (role, result.stdout, result.stderr)
                return result.stdout

            worker = fixture.launch(BINARY, 'before', args=['worker'])
            handle = json.loads(call('start'))
            fixture.wait_for(lambda: counts(fixture.history(handle))['timerstarted'] == 1, timeout=40)
            worker.kill(); assert worker.wait(timeout=10) == -9
            replacement = fixture.launch(BINARY, 'after', args=['worker'])
            call('checkpoint'); call('finish')
            assert (directory / 'activities.log').read_text().splitlines() == ['local', 'remote']
            fixture.assert_clean_history(handle)
            file = ARTIFACTS / 'binary-histories/plugins-crash.bin'
            call('history', file); call('replay', file)
            replacement.terminate(); assert replacement.wait(timeout=20) == 0
            report['cases'].append('Plugin-defined workflow/Activity execution survived SIGKILL, reconstructed from a fresh registry and replayed offline without repeating committed Activities')
        with TemporalFixture('plugins-construction', server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            directory = configure_fixture(fixture)
            barrier = directory / 'observed-query'
            marker = 'construction-query-' + fixture.queue
            observed = threading.Event()
            trailing = bytearray()
            observer_lock = threading.Lock()

            def observe(data):
                with observer_lock:
                    trailing.extend(data)
                    if marker.encode() in trailing and not observed.is_set():
                        observed.set()
                        barrier.write_text('query forwarded before worker construction')
                    del trailing[:-len(marker)]

            with ConnectProxy(fixture.address, observe_client_bytes=observe) as proxy:
                fixture.env.update(TEMPORAL_PROXY_TARGET=proxy.address, TEMPORAL_PROXY_USERNAME='v-test', TEMPORAL_PROXY_PASSWORD='disposable-proxy-password',
                                   V_SDK_EXAMPLE_CONSTRUCTION_MARKER=marker, V_SDK_EXAMPLE_CONSTRUCTION_BARRIER=str(barrier))
                result = subprocess.run([str(BINARY), 'construction-race'], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=30)
                (ARTIFACTS / 'logs/plugins-construction-driver.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0 and 'CONSTRUCTION_OK' in result.stdout, result.stderr
                assert observed.is_set()
            fixture.assert_clean_history({'workflow_id': fixture.queue + '-construction'})
            report['cases'].append('The proxy observed a blocking query on the wire before shared worker construction; the new worker answered it without a constructor/RPC deadlock and all loops drained')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

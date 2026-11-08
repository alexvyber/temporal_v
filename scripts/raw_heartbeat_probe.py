"""Opaque heartbeat progress is available before optional payload conversion."""
RESULT_REPORT = 'raw-heartbeat-results.json'
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS, ROOT, CLI, TemporalFixture, counts


def main(binary, report_name, quick=False):
    report = {'passed': False, 'cases': [], 'histories': 0}
    try:
        profiles = [(version, server, codec, stored)
                    for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]
                    for codec in ['', 'prefix', 'context'] for stored in [False, True]]
        if quick:
            profiles = profiles[:1]
        for version, server, codec, stored in profiles:
            with TemporalFixture('raw-heartbeat-' + version + '-' + (codec or 'default') + '-' + str(stored) + '-' + binary.name,
                                 server_binary=ARTIFACTS / 'tools' / server) as fixture:
                directory = fixture.histories / fixture.queue
                directory.mkdir()
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec, V_HEARTBEAT_CHECKPOINTS=str(directory))
                if stored:
                    fixture.env.update(V_PANIC_STORAGE_DRIVER='true', V_SDK_EXAMPLE_STORAGE_DIR=str(directory / 'storage'))
                worker = fixture.launch(binary, 'one', ('worker',))
                handle = fixture.start('opaque', 'v.raw-heartbeats', {})

                def pending():
                    result = subprocess.run([str(CLI), 'workflow', 'describe', '--address', fixture.address,
                                             '--workflow-id', handle['workflow_id'], '--raw',
                                             '--no-json-shorthand-payloads', '--output', 'json'],
                                            capture_output=True, text=True, timeout=15)
                    assert result.returncode == 0, result.stderr
                    return json.loads(result.stdout).get('pendingActivities', [])

                for attempt, label in [(1, 'two'), (2, 'three')]:
                    def checkpointed():
                        items = pending()
                        return all((directory / (name + '-' + str(attempt))).exists() for name in ['v.heartbeat.named', 'v.heartbeat.dynamic']) and len(items) == 2 and all(int(item['attempt']) == attempt and
                            len(item.get('heartbeatDetails', {}).get('payloads', [])) == 5 for item in items)
                    fixture.wait_for(checkpointed)
                    items = pending()
                    (directory / ('attempt-' + str(attempt) + '.json')).write_text(json.dumps(items, indent=2) + '\n')
                    for item in items:
                        payload = item['heartbeatDetails']['payloads'][0]
                        message_type = base64.b64decode(payload.get('metadata', {}).get('messageType', ''))
                        assert (message_type == b'temporal.api.sdk.v1.ExternalStorageReference') == stored, payload
                    worker.kill()
                    assert worker.wait(timeout=10) == -9
                    if attempt == 2:
                        fixture.env['V_HEARTBEAT_FINISH'] = 'true'
                    worker = fixture.launch(binary, label, ('worker',))

                def state():
                    return json.loads(fixture.call('query', handle, name='state', rpc_ms=20000)['data'])
                fixture.wait_for(lambda: len(state()) == 2)
                for name, encoded in state().items():
                    value = json.loads(encoded)
                    assert int(value['attempt']) == 3 and value['count'] == '5', value
                    assert value['digest'] == hashlib.sha256(b'\xff\0\x80' + b'x' * 320000).hexdigest(), value
                    assert value['encoding'] == 'application/x-temporal-v-raw-activity', value
                    assert value['protobuf'] == 'temporal.api.common.v1.WorkflowExecution', value
                fixture.call('signal', handle, name='finish')
                assert fixture.call('result', handle)['status'] == 'completed'
                assert counts(fixture.history(handle))['workflowtaskfailed'] == 0
                file = directory / 'history.bin'
                fixture.call('history', handle, file=file)
                worker.terminate()
                assert worker.wait(timeout=20) == 0
                replay = subprocess.run([str(binary), 'replay', str(file), handle['workflow_id'], fixture.queue],
                                        cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                assert replay.returncode == 0 and replay.stdout.strip() == 'OK', replay.stdout + replay.stderr
                report['histories'] += 1
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: named and dynamic Activities retained five owned raw checkpoint values through two SIGKILL replacements; explicit typed/string conversion rejected only at the call site, raw access survived; completion/offline replay and clean drain passed')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=ARTIFACTS / 'tools/v-panic')
    parser.add_argument('--report', default=RESULT_REPORT)
    parser.add_argument('--quick', action='store_true')
    args = parser.parse_args()
    main(args.binary, args.report, args.quick)

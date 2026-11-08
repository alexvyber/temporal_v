"""Nexus callback panics report handler errors and retry without killing the worker."""
RESULT_REPORT = 'panic-nexus-results.json'
import argparse
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, counts


def main(binary, report_name):
    report = {'passed': False, 'cases': [], 'histories': 0}
    try:
        for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]:
            for codec in ['', 'context']:
                with TemporalFixture('panic-nexus-' + version + '-' + (codec or 'default') + '-' + binary.name, server_binary=ARTIFACTS / 'tools' / server) as fixture:
                    markers = fixture.histories / fixture.queue; markers.mkdir()
                    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec, V_PANIC_NEXUS_DIR=str(markers))
                    endpoint = fixture.queue + '-endpoint'
                    created = subprocess.run([str(ARTIFACTS / 'tools' / server), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address, '--name', endpoint, '--target-namespace', 'default', '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
                    assert created.returncode == 0, created.stderr
                    worker = fixture.launch(binary, 'before', ('worker',))
                    synchronous = fixture.start('sync', 'v.panic', {'mode': 'nexus-sync', 'endpoint': endpoint})
                    fixture.wait_for(lambda: len(list(markers.iterdir())) == 1)
                    assert worker.poll() is None
                    worker.kill(); worker.wait(timeout=10)
                    worker = fixture.launch(binary, 'after', ('worker',))
                    result = fixture.call('result', synchronous)
                    assert result['status'] == 'completed' and result['data'] == '"nexus-recovered"', result
                    asynchronous = fixture.start('cancel', 'v.panic', {'mode': 'nexus-cancel', 'endpoint': endpoint})
                    fixture.wait_for(lambda: counts(fixture.history(asynchronous))['nexusoperationstarted'] == 1)
                    history = fixture.history(asynchronous)
                    started = next(event['nexusOperationStartedEventAttributes'] for event in history['events'] if 'nexusOperationStartedEventAttributes' in event)
                    backing = {'workflow_id': 'nexus-' + started['requestId']}
                    fixture.call('signal', asynchronous, name='cancel-nexus')
                    fixture.wait_for(lambda: len(list(markers.iterdir())) == 2)
                    assert worker.poll() is None
                    result = fixture.call('result', asynchronous)
                    assert result['status'] == 'completed' and result['data'] == '"nexus-canceled"', result
                    assert fixture.call('result', backing)['status'] == 'canceled'
                    for index, handle in enumerate([synchronous, asynchronous, backing]):
                        fixture.assert_clean_history(handle)
                        file = markers / (str(index) + '.bin'); fixture.call('history', handle, file=file)
                        replay = subprocess.run([str(binary), 'replay', str(file), handle['workflow_id'], fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                        assert replay.returncode == 0, replay.stdout + replay.stderr
                        report['histories'] += 1
                    worker.terminate(); assert worker.wait(timeout=20) == 0
                    report['cases'].append(f'{version}/{codec or "default"}: start/cancel handler panics retried; start retry survived SIGKILL/replacement; workflow-backed cancellation and three offline histories passed with clean drain')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--binary', type=Path, default=ARTIFACTS / 'tools/v-panic'); parser.add_argument('--report', default=RESULT_REPORT)
    args = parser.parse_args(); main(args.binary, args.report)

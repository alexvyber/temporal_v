"""Preparation, output and failure conversion panics produce one Activity completion."""
RESULT_REPORT = 'panic-extensions-results.json'
from panic_stack_assertions import assert_stack
import argparse
import concurrent.futures
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, counts


def main(binary, report_name):
    report = {'passed': False, 'cases': [], 'histories': 0}
    try:
        for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]:
            for codec in ['', 'context']:
                with TemporalFixture('panic-ext-' + version + '-' + (codec or 'default') + '-' + binary.name, server_binary=ARTIFACTS / 'tools' / server) as fixture:
                    storage = fixture.histories / fixture.queue; storage.mkdir()
                    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec, V_PANIC_EXTENSIONS='true', V_PANIC_STORAGE_DRIVER='true', V_SDK_EXAMPLE_STORAGE_DIR=str(storage))
                    worker = fixture.launch(binary, 'before', ('worker',))
                    healthy = fixture.start('healthy', 'v.panic', {'mode': 'wait'})
                    handles = [(mode, fixture.start(mode, 'v.panic', {'mode': mode})) for mode in ['activity-input', 'local-input', 'activity-output', 'local-output', 'activity-error', 'local-error', 'activity-store', 'local-store', 'activity-retrieve']]
                    def failed(item):
                        mode, handle = item
                        result = fixture.call('result', handle)
                        expected = ('storage store panic' if mode.endswith('-store') else 'storage retrieve panic') if mode.endswith(('-store', '-retrieve')) else 'failure converter panic' if mode.endswith('-error') else ('input codec panic' if mode.endswith('-input') else 'output codec panic')
                        assert result['status'] == 'failed' and result['message'] == expected, (mode, result)
                        history = fixture.history(handle)
                        assert counts(history)['workflowtaskfailed'] == 0, history
                        if mode.startswith('activity-'):
                            attributes = next(event['activityTaskFailedEventAttributes'] for event in history['events'] if 'activityTaskFailedEventAttributes' in event)
                            assert attributes['failure']['applicationFailureInfo']['type'] == 'PanicError', attributes
                            expected_frame = 'PanicStorageDriver__store' if mode.endswith('-store') else 'PanicStorageDriver__retrieve' if mode.endswith('-retrieve') else 'PanicFailureConverter__to_failure' if mode.endswith('-error') else 'PanicCodec__decode' if mode.endswith('-input') else 'PanicCodec__encode'
                            assert_stack(attributes['failure'],expected_frame)
                            assert attributes['retryState'] == 'RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED', attributes
                    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as executor:
                        list(executor.map(failed, handles))
                    assert worker.poll() is None
                    assert fixture.call('query', healthy, name='state')['data'] == '0'
                    # A new Activity proves prior failed preparations released their worker slots.
                    recovered = fixture.start('new-activity', 'v.panic', {'mode': 'activity-retry'})
                    assert fixture.call('result', recovered)['data'] == '"activity-recovered"'
                    worker.kill(); worker.wait(timeout=10)
                    worker = fixture.launch(binary, 'after', ('worker',))
                    fixture.call('signal', healthy, name='finish')
                    assert fixture.call('result', healthy)['data'] == '0'
                    files = storage / 'histories'; files.mkdir()
                    for index, handle in enumerate([healthy, recovered, *[handle for _, handle in handles]]):
                        file = files / (str(index) + '.bin'); fixture.call('history', handle, file=file)
                        replay = subprocess.run([str(binary), 'replay', str(file), handle['workflow_id'], fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                        assert replay.returncode == 0, replay.stdout + replay.stderr
                        report['histories'] += 1
                    worker.terminate(); assert worker.wait(timeout=20) == 0
                    report['cases'].append(f'{version}/{codec or "default"}: nine concurrent input/output/failure conversion and storage callback panics produced retryable PanicError completions; new Activity and unrelated workflow progressed; SIGKILL/replacement, eleven offline histories; one-slot storage permits drained after panics and clean drain passed')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--binary', type=Path, default=ARTIFACTS / 'tools/v-panic'); parser.add_argument('--report', default=RESULT_REPORT)
    args = parser.parse_args(); main(args.binary, args.report)

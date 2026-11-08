"""Export real histories via the V client and replay them with the server stopped."""

RESULT_REPORT = 'replay-results.json'
import json
import subprocess

from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': [], 'histories': []}
    directory = ARTIFACTS / 'binary-histories'
    directory.mkdir(exist_ok=True)
    try:
        with TemporalFixture('replay') as fixture:
            fixture.workflows()
            fixture.activities()
            fixtures = [
                ('restart', 'v.restart', {'name': 'offline', 'delay_seconds': 1}),
                ('local', 'v.activity-options', {'local': True, 'fail_until': 1}),
                ('chain', 'v.chain', {'remaining': 1}),
                ('random', 'v.random', {}),
                ('timer', 'v.timer-cancel', {}),
            ]
            for label, workflow_type, data in fixtures:
                handle = fixture.start(label, workflow_type, data)
                fixture.call('result', handle)
                fixture.assert_clean_history(handle)
                file = directory / f'{label}.bin'
                fixture.call('history', handle, file=file)
                report['histories'].append(dict(handle, file=str(file), queue=fixture.queue))
            counter = fixture.start('counter', 'v.counter', {})
            fixture.call('signal', counter, name='add', input='{"amount":2}')
            update = fixture.call('update', counter, name='add', update_id='offline-update', input='{"amount":3}')
            fixture.call('update-result', counter, update_id=update['update_id'])
            fixture.call('signal', counter, name='finish')
            fixture.call('result', counter)
            file = directory / 'counter.bin'
            fixture.call('history', counter, file=file)
            report['histories'].append(dict(counter, file=str(file), queue=fixture.queue))
        # The dev server and all workers are stopped before any replay executable runs.
        for item in report['histories']:
            result = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', item['file'],
                                     '--workflow-id', item['workflow_id'], '--task-queue', item['queue']],
                                    cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40)
            assert result.returncode == 0, (item['file'], result.stderr)
            assert json.loads(result.stdout)['activations'] > 1, result.stdout
        report['cases'].append('Six binary histories replayed offline: remote/local Activities, continuation, patch, RNG, timer cancellation, signals and Updates')
        item = report['histories'][0]
        wrong = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', item['file'],
                                '--workflow-id', item['workflow_id'], '--task-queue', item['queue'], '--wrong-code'],
                               cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40)
        assert wrong.returncode != 0 and 'nondeterminism' in wrong.stderr.lower(), (wrong.stdout, wrong.stderr)
        report['cases'].append('Intentionally incompatible workflow code failed offline replay with a nondeterminism diagnostic')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

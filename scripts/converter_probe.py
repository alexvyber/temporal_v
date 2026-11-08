"""Converter/codec propagation through clients, workers, Activities and replay."""

RESULT_REPORT = 'converter-results.json'
import base64
import json
import subprocess

from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def payloads(value):
    if isinstance(value, dict):
        if 'metadata' in value and 'data' in value:
            yield value
        for nested in value.values():
            yield from payloads(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from payloads(nested)


def main():
    report = {'passed': False, 'cases': []}
    try:
        for converter in ('prefix', 'binary'):
            with TemporalFixture('converter-' + converter) as fixture:
                fixture.env['V_SDK_EXAMPLE_CODEC'] = converter
                worker = fixture.workflows('before')
                fixture.activities()
                counter = fixture.start('counter', 'v.counter', {})
                fixture.call('signal', counter, name='add', input='{"amount":2}')
                update = fixture.call('update', counter, name='add', update_id='converted-update', input='{"amount":3}')
                fixture.call('update-result', counter, update_id=update['update_id'])
                assert json.loads(fixture.call('query', counter, name='state')['data'])['total'] == 5
                workflow = fixture.start('restart', 'v.restart', {'name': 'codec 🌍', 'delay_seconds': 2})
                fixture.wait_for(lambda: counts(fixture.history(workflow))['timerstarted'] == 1)
                worker.kill()
                assert worker.wait(timeout=5) == -9
                worker = fixture.workflows('after')
                completed = fixture.call('result', workflow)
                assert completed['status'] == 'completed' and 'codec 🌍' in json.loads(completed['data']), completed
                assert json.loads(fixture.call('query', counter, name='state')['data'])['total'] == 5
                fixture.call('signal', counter, name='finish')
                fixture.call('result', counter)
                local = fixture.start('local', 'v.activity-options', {'local': True, 'fail_until': 1})
                result = fixture.call('result', local)
                assert result['status'] == 'completed' and json.loads(result['data'])['attempt'] == 2, result
                history = fixture.history(workflow)
                encoded = list(payloads(history))
                assert encoded, 'no payloads observed in independent history'
                if converter == 'prefix':
                    assert any(base64.b64decode(item['data']).startswith(b'\x00B\xff\x00A\xff') for item in encoded)
                else:
                    assert any(base64.b64decode(item['metadata']['encoding']) == b'binary/plain' for item in encoded)
                for handle in (workflow, counter, local):
                    fixture.assert_clean_history(handle)
                    file = ARTIFACTS / 'binary-histories' / f'{converter}-{handle["run_id"]}.bin'
                    file.parent.mkdir(exist_ok=True)
                    fixture.call('history', handle, file=file)
                    replayed = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file),
                                               '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue],
                                              cwd=ROOT, env=fixture.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=40)
                    assert replayed.returncode == 0, replayed.stderr
                report['cases'].append(converter + ': client, signals, queries, Updates, remote/local Activities, restart replay and offline replay')
                worker.terminate()
                assert worker.wait(timeout=15) == 0
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

"""Dynamic fallback dispatch, named precedence and replay without redispatch."""

RESULT_REPORT = 'dynamic-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        validation = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'dynamic-validation'],
                                    cwd=ROOT, capture_output=True, text=True, timeout=15)
        assert validation.returncode == 0, validation.stderr
        report['cases'] += json.loads(validation.stdout)
        with TemporalFixture('dynamic') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_DYNAMIC_HANDLERS='true', V_SDK_EXAMPLE_CODEC='context',
                               V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            worker = fixture.workflows('before'); activity = fixture.activities('activity-before')
            counter = fixture.start('named', 'v.counter', {'initial': 4})
            assert json.loads(fixture.call('query', counter, name='state')['data'])['total'] == 4
            fixture.call('signal', counter, name='finish')
            assert fixture.call('result', counter)['status'] == 'completed'
            specs = [('fallback', False, False), ('named', True, False), ('retry', False, True)]
            handles = [fixture.start('unknown-' + label, fixture.queue + '-unknown-' + label, {'known': known, 'retry': retry}) for label, known, retry in specs]
            for (label, known, retry), handle in zip(specs, handles):
                def ready():
                    return json.loads(fixture.call('query', handle, name='state')['data'])['ready']
                fixture.wait_for(ready)
                state = json.loads(fixture.call('query', handle, name='state')['data'])
                assert state['workflow_type'] == fixture.queue + '-unknown-' + label
                local = json.loads(state['local'])
                assert local['activity_type'] == state['workflow_type'] + '.local' and local['is_local'] == 'true', local
                assert local['attempt'] == ('2' if retry else '1'), local
                assert local['value'] == 'dynamic 世界' and local['workflow_id'] == handle['workflow_id']
                if known:
                    assert json.loads(state['remote']) == 'Hello from V, named!'
                else:
                    remote = json.loads(state['remote'])
                    assert remote['activity_type'] == state['workflow_type'] + '.remote' and remote['is_local'] == 'false', remote
                    assert remote['attempt'] == ('2' if retry else '1'), remote
                    assert remote['value'] == 'dynamic 世界'
                metadata = fixture.call('metadata', handle)
                assert metadata['definition']['type'] == state['workflow_type'], metadata
            failed = fixture.start('failure', fixture.queue + '-unknown-failure', {'fail': True})
            failure = fixture.call('result', failed)
            assert failure['status'] == 'failed' and failure['message'] and failure['message'] != 'Encoded failure', failure
            for process in (worker, activity):
                process.kill(); assert process.wait(timeout=5) == -9
            worker = fixture.workflows('after'); activity = fixture.activities('activity-after')
            for handle in handles:
                assert json.loads(fixture.call('query', handle, name='state')['data'])['ready']
                fixture.call('signal', handle, name='finish')
                assert fixture.call('result', handle)['status'] == 'completed'
            for index, handle in enumerate([counter, *handles, failed]):
                fixture.assert_clean_history(handle)
                file = ARTIFACTS / f'binary-histories/dynamic-{index}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'],
                                         '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            for process in (worker, activity):
                process.terminate(); assert process.wait(timeout=15) == 0
            for name in ('dynamic-after.log', 'dynamic-activity-after.log'):
                assert 'ACTIVITY_START' not in (ARTIFACTS / 'logs' / name).read_text()
            report['cases'] += [
                'Unknown workflow and remote/local Activity types dispatched dynamically with original type information, argument boundaries and transient retry recovery',
                'Named workflow/Activity registrations took precedence; query metadata preserved the actual dynamic workflow type',
                'Dynamic failures remained workflow failures under scoped conversion; completed remote/local results survived SIGKILL without redispatch',
                'All dynamic and named histories replayed offline and workers drained',
            ]
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

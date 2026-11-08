"""Interceptor chains against Temporal, including durable restart and offline replay."""

RESULT_REPORT = 'interceptors-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, LOGS, CLI, counts


def ordered(text, prefix):
    lines = [line for line in text.splitlines() if line.startswith('INTERCEPT ' + prefix + ' ')]
    assert lines, (prefix, text[-2000:])
    stack = []
    for line in lines:
        _, _, name, action, *operation = line.split()
        if action == 'enter':
            assert name == ('outer' if not stack else 'inner'), line
            stack.append(name)
        else:
            assert stack and stack.pop() == name, (line, stack)
    assert not stack, stack


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('interceptors') as fixture:
            fixture.env['V_SDK_EXAMPLE_INTERCEPTORS'] = 'true'
            worker = fixture.workflows('before')
            fixture.activities()
            run = fixture.start('counter', 'v.counter', {'initial': 10, 'label': 'intercepted'})
            assert json.loads(fixture.call('query', run, name='state')['data'])['total'] == 10
            duplicate = fixture.call('start', run, type='v.counter', input='{}', success=False)
            assert duplicate['code'] == '6', duplicate
            rejected = fixture.call('query', run, name='interceptor_reject', success=False)
            assert 'query rejected by interceptor' in rejected['message'], rejected
            rejected = fixture.call('update', run, name='add', input='{"amount":777}')
            assert 'update rejected by interceptor' in rejected['message'], rejected
            update = fixture.call('update', run, name='add', input='{"amount":5}')
            assert json.loads(update['data'])['total'] == 15, update
            fixture.call('signal', run, name='add', input='{"amount":3}')
            fixture.wait_for(lambda: json.loads(fixture.call('query', run, name='state')['data'])['total'] == 18)
            remote = fixture.start('remote', 'v.restart', {'name': 'intercepted', 'delay_seconds': 3})
            local = fixture.start('local', 'v.activity-options', {'local': True, 'fail_until': 1})
            fixture.wait_for(lambda: counts(fixture.history(remote))['timerstarted'] == 1)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            assert json.loads(fixture.call('query', run, name='state')['data'])['total'] == 18
            metadata = fixture.call('metadata', run)
            assert metadata['currentDetails'] == 'Counter: intercepted', metadata
            definition = metadata['definition']
            assert definition['type'] == 'v.counter', definition
            assert [item['name'] for item in definition['queryDefinitions']] == ['explode', 'invalid-result', 'offset', 'state'], definition
            assert [item['name'] for item in definition['signalDefinitions']] == ['add', 'finish'], definition
            assert [item['name'] for item in definition['updateDefinitions']] == ['add', 'delayed_add', 'fail'], definition
            assert definition['queryDefinitions'][-1]['description'] == 'Current counter state 世界', definition
            independent = subprocess.run([str(CLI), 'workflow', 'query', '--address', fixture.address,
                                          '--workflow-id', run['workflow_id'], '--type', '__temporal_workflow_metadata',
                                          '--output', 'json'], capture_output=True, text=True, timeout=15)
            assert independent.returncode == 0 and 'queryDefinitions' in independent.stdout and 'v.counter' in independent.stdout, independent.stderr
            fixture.call('signal', run, name='finish')
            assert json.loads(fixture.call('result', run)['data'])['total'] == 18
            assert fixture.call('result', remote)['status'] == 'completed'
            assert fixture.call('result', local)['status'] == 'completed'
            report['cases'].append('Workflow/Activity/query/validator interceptor chains preserved state across SIGKILL; query and validation errors stayed isolated')
            file = ARTIFACTS / 'binary-histories/interceptors.bin'
            fixture.call('history', run, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', run['workflow_id'], '--task-queue', fixture.queue],
                                    cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            ordered(replay.stderr, 'workflow')
            ordered(replay.stderr, 'command')
            assert 'enter update_received' in replay.stderr
            for handle in (run, remote, local): fixture.assert_clean_history(handle)
            ordered((LOGS / 'interceptors-before.log').read_text(), 'workflow')
            after = (LOGS / 'interceptors-after.log').read_text()
            ordered(after, 'workflow')
            ordered(after, 'command')
            ordered(after, 'query')
            before = (LOGS / 'interceptors-before.log').read_text()
            ordered(before, 'validator')
            activity = (LOGS / 'interceptors-activity.log').read_text()
            ordered(activity, 'activity')
            assert 'local=true' in before + after
            report['cases'].append('Outer/inner entry and reverse exit order verified for live execution and offline replay; local and remote Activities traversed chains')
            # Captured client stderr is checked independently, including result and Update waits.
            for command, options, method in [('describe', {}, 'DescribeWorkflowExecution'), ('result', {}, 'GetWorkflowExecutionHistory'), ('query', {'name': 'state'}, 'QueryWorkflow')]:
                args = [str(ARTIFACTS / 'tools/v-client'), '--address', 'http://' + fixture.address, '--workflow-id', run['workflow_id'], '--run-id', run['run_id']]
                for key, value in options.items(): args += ['--' + key, value]
                response = subprocess.run(args + [command], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=30)
                assert response.returncode == 0, response.stderr
                ordered(response.stderr, 'client')
                assert 'enter ' + method in response.stderr
            report['cases'].append('Typed client calls and result long polls traversed ordered chains with per-call ASCII and arbitrary binary metadata')
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

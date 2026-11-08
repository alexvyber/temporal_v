"""Target-aware conversion, cross-namespace failures and durable async handles."""

RESULT_REPORT = 'context-results.json'
import base64
import json
from pathlib import Path
import subprocess
import tempfile
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI, counts


def payloads(value):
    if isinstance(value, dict):
        if 'metadata' in value and 'data' in value: yield value
        for nested in value.values(): yield from payloads(nested)
    elif isinstance(value, list):
        for nested in value: yield from payloads(nested)


def context_of(payload):
    return json.loads(base64.b64decode(payload['metadata']['temporal-v/codec-context']))


def main():
    report = {'passed': False, 'cases': []}
    try:
        with tempfile.TemporaryDirectory(prefix='v-context-tokens-') as temp, TemporalFixture('context', server_args=(
                '--dynamic-config-value', 'system.enableCrossNamespaceCommands=true')) as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded',
                               V_SDK_EXAMPLE_ASYNC_TOKEN_DIR=temp, TEMPORAL_CORE_LOG_FILTER='warn')
            child_namespace = 'default-context-child'
            created = subprocess.run([str(CLI), 'operator', 'namespace', 'create', '--address', fixture.address,
                                      '--namespace', child_namespace, '--retention', '1d'], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            worker = fixture.workflows('before'); activity = fixture.activities('before-activity')
            child_env = dict(fixture.env, TEMPORAL_NAMESPACE=child_namespace)
            child_worker = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'child-workflow', env=child_env)
            child_activity = fixture.launch(ARTIFACTS / 'tools/v-worker', 'child-activity', env=child_env)
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus')
            endpoint = fixture.queue + '-endpoint'
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', endpoint, '--target-namespace', 'default', '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            counter = fixture.start('counter', 'v.counter', {'initial': 1})
            fixture.call('signal', counter, name='add', input='{"amount":2}')
            update = fixture.call('update', counter, name='add', input='{"amount":3}')
            assert json.loads(fixture.call('update-result', counter, update_id=update['update_id'])['data'])['total'] == 6
            assert json.loads(fixture.call('query', counter, name='state')['data'])['total'] == 6
            external = fixture.start('external', 'v.external', {'mode': 'signal', 'workflow_id': counter['workflow_id']})
            assert not json.loads(fixture.call('result', external)['data'])['failed']
            assert json.loads(fixture.call('query', counter, name='state')['data'])['total'] == 11
            fixture.call('signal', counter, name='finish')
            assert fixture.call('result', counter)['status'] == 'completed'
            child_counter = fixture.call('start', workflow_id=fixture.queue + '-cross-signal-target',
                                         type='v.counter', input='{"initial":4}', namespace=child_namespace)
            child_counter['namespace'] = child_namespace
            cross_signal = fixture.start('cross-signal', 'v.external', {'mode': 'signal',
                'workflow_id': child_counter['workflow_id'], 'namespace': child_namespace})
            assert not json.loads(fixture.call('result', cross_signal)['data'])['failed']
            assert json.loads(fixture.call('query', child_counter, name='state')['data'])['total'] == 9
            fixture.call('signal', child_counter, name='finish')
            assert fixture.call('result', child_counter)['status'] == 'completed'
            operations = []
            for operation in ('echo', 'async'):
                handle = fixture.start('nexus-' + operation, 'v.nexus', {'endpoint': endpoint, 'operation': operation, 'name': 'context'})
                result = fixture.call('result', handle)
                assert result['status'] == 'completed' and 'context' in result['data'], result
                operations.append(handle)
            driver = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'serialization-contexts'], cwd=ROOT, env=fixture.env,
                                    capture_output=True, text=True, timeout=90)
            (ARTIFACTS / 'logs/context-driver.log').write_text(driver.stdout + driver.stderr)
            assert driver.returncode == 0, driver.stderr[-5000:]
            report['cases'] += json.loads(driver.stdout)
            asynchronous = []
            for by_token in (True, False):
                handle = fixture.start('deferred-' + str(by_token), 'v.async-activity', {})
                token = Path(temp) / (handle['run_id'] + '.json')
                fixture.wait_for(lambda: token.exists() and token.stat().st_size > 0)
                assert token.stat().st_mode & 0o077 == 0
                metadata = json.loads(token.read_text())['serialization_context']
                assert metadata['kind'] == 'activity' and metadata['workflow_id'] == handle['workflow_id'], metadata
                if by_token:
                    worker.kill(); activity.kill()
                    assert worker.wait(timeout=5) == activity.wait(timeout=5) == -9
                address = {'token_file': token} if by_token else {'activity_id': 'activity-1'}
                fixture.call('activity-heartbeat', handle, input='{"cursor":7}', **address)
                fixture.call('activity-complete', handle, input='{"external":42}', **address)
                if by_token:
                    worker = fixture.workflows('after'); activity = fixture.activities('after-activity')
                result = fixture.call('result', handle)
                assert result['status'] == 'completed' and json.loads(result['data']) == {'external': 42}, result
                asynchronous.append(handle)
            report['cases'] += ['Signals, structured query failures, Updates and cross-namespace external workflow signals used the target workflow namespace/ID',
                                'Synchronous and workflow-backed asynchronous Nexus operations preserved endpoint/service/operation context',
                                'Persisted Activity tokens retained context across worker loss; completion by token and execution ID decoded correctly']
            handles = [counter, external, cross_signal, child_counter, *operations, *asynchronous]
            handles += [{'workflow_id': fixture.queue + suffix} for suffix in ('-ctx-remote-世界', '-ctx-local-世界', '-ctx-parent', '-ctx-child-success', '-ctx-continue', '-ctx-query-error')]
            handles.append({'workflow_id': fixture.queue + '-ctx-parent-child', 'namespace': child_namespace})
            # The first run contains the continuation command; the latest run alone
            # cannot prove its arguments replay with the original actor context.
            latest = fixture.history({'workflow_id': fixture.queue + '-ctx-continue'})
            started = latest['events'][0]['workflowExecutionStartedEventAttributes']
            first_run = started['firstExecutionRunId']
            handles.append({'workflow_id': fixture.queue + '-ctx-continue', 'run_id': first_run})
            observed = []
            for index, handle in enumerate(handles):
                history = fixture.history(handle)
                fixture.assert_clean_history(handle)
                for payload in payloads(history):
                    if 'temporal-v/codec-context' in payload['metadata']: observed.append(context_of(payload))
                file = ARTIFACTS / f'binary-histories/context-{index}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'],
                                         '--task-queue', fixture.queue, '--namespace', handle.get('namespace', 'default')], cwd=ROOT, env=dict(fixture.env, TEMPORAL_NAMESPACE=handle.get('namespace', 'default')), capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            assert {'workflow', 'activity', 'nexus'} <= {context['kind'] for context in observed}, observed
            assert any(context['is_local'] for context in observed if context['kind'] == 'activity'), observed
            (ARTIFACTS / 'context-wire-observations.json').write_text(json.dumps(observed, indent=2) + '\n')
            for process in (worker, activity, child_worker, child_activity, nexus):
                process.terminate(); assert process.wait(timeout=15) == 0
            report['cases'].append('Independent history carried actor metadata at converter and codec layers; contextual histories replayed offline and workers drained')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

"""Concurrent request identities, explicit deduplication and process isolation."""

RESULT_REPORT = 'identity-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('identities') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context')
            worker = fixture.workflows()
            result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'request-identities'], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=90)
            (ARTIFACTS / 'logs/identities-driver.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr[-5000:]
            report['cases'] += json.loads(result.stdout)
            worker.terminate(); assert worker.wait(timeout=15) == 0
            # Independent V processes must not derive the same request identity
            # from the elapsed time of their nearly identical startup paths.
            handle = fixture.start('processes', 'v.counter', {'initial': 0})
            for _ in range(100): fixture.call('signal', handle, name='add', input='{"amount":1}')
            observed = fixture.assert_clean_history(handle)
            assert observed['workflowexecutionsignaled'] == 100, observed
            history = fixture.history(handle)
            ids = [event['workflowExecutionSignaledEventAttributes']['requestId'] for event in history['events'] if 'workflowExecutionSignaledEventAttributes' in event]
            assert len(set(ids)) == 100
            worker = fixture.workflows('replacement')
            assert json.loads(fixture.call('query', handle, name='state')['data'])['total'] == 100
            fixture.call('signal', handle, name='finish')
            assert fixture.call('result', handle)['status'] == 'completed'
            for label in ('identities', 'processes'):
                target = {'workflow_id': fixture.queue + '-' + label}
                history = fixture.history(target)
                failures = [event['workflowTaskFailedEventAttributes'] for event in history['events'] if 'workflowTaskFailedEventAttributes' in event]
                # The signal burst can exceed Temporal's buffered-event limit.
                # History service forces a rollover; this is not an SDK failure.
                if label == 'identities':
                    assert all(failure.get('cause') == 'WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND' and failure.get('identity') == 'history-service' for failure in failures), failures
                    report['server_buffer_rollovers'] = len(failures)
                else:
                    assert not failures, failures
                file = ARTIFACTS / f'binary-histories/identity-{label}.bin'
                fixture.call('history', target, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', target['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            worker.terminate(); assert worker.wait(timeout=15) == 0
            report['cases'].append('One hundred independent V clients committed distinct signals before a replacement worker replayed them; both histories also replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

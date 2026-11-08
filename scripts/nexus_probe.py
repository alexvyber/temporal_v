"""Real Nexus endpoint -> V handler -> workflow resolutions and replay."""

RESULT_REPORT = 'nexus-results.json'
import json
import base64
import subprocess
import tempfile

from integration import TemporalFixture, ARTIFACTS, CLI, LOGS, ROOT, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with tempfile.TemporaryDirectory(prefix='v-nexus-retry-') as retry_directory, TemporalFixture('nexus') as fixture:
            fixture.env['V_SDK_EXAMPLE_NEXUS_RETRY_DIRECTORY'] = retry_directory
            endpoint = fixture.queue + '-endpoint'
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', endpoint, '--target-namespace', 'default', '--target-task-queue', fixture.queue],
                                     text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
            assert created.returncode == 0, created.stderr
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'handler-before')
            worker = fixture.workflows('before')
            first = fixture.start('success', 'v.nexus', {'endpoint': endpoint, 'name': 'world 🌍'})
            fixture.wait_for(lambda: counts(fixture.history(first))['timerstarted'] == 1)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            before = (LOGS / 'nexus-handler-before.log').read_text().count('NEXUS_START')
            worker = fixture.workflows('after')
            result = fixture.call('result', first)
            assert result['status'] == 'completed' and json.loads(result['data']) == 'Hello from Nexus, world 🌍!', result
            events = fixture.assert_clean_history(first)
            assert events['nexusoperationscheduled'] == 1 and events['nexusoperationcompleted'] == 1, events
            scheduled = next(event for event in fixture.history(first)['events'] if 'nexusOperationScheduledEventAttributes' in event)
            assert json.loads(base64.b64decode(scheduled['userMetadata']['summary']['data'])) == 'Nexus summary 世界'
            assert (LOGS / 'nexus-handler-before.log').read_text().count('NEXUS_START') == before
            report['cases'].append('Nexus synchronous handler completed; workflow restart replay did not invoke operation again')
            for operation, message in (('fail', 'requested Nexus failure'), ('missing', 'unknown Nexus operation')):
                failed = fixture.start(operation, 'v.nexus', {'endpoint': endpoint, 'operation': operation})
                result = fixture.call('result', failed)
                assert result['status'] == 'failed' and message in result['message'], result
                fixture.assert_clean_history(failed)
            report['cases'].append('Nexus operation failures and unknown handlers produced isolated workflow failures')
            rejected = fixture.start('handler-rejected', 'v.nexus', {'endpoint': endpoint, 'operation': 'handler-error', 'name': 'permanent'})
            result = fixture.call('result', rejected)
            assert result['status'] == 'failed' and 'permanent request rejection' in result['message'], result
            fixture.assert_clean_history(rejected)
            retried = fixture.start('handler-retry', 'v.nexus', {'endpoint': endpoint, 'operation': 'handler-error'})
            result = fixture.call('result', retried)
            assert result['status'] == 'completed' and result['data'] == '"handler retry succeeded"', result
            assert counts(fixture.history(retried))['nexusoperationscheduled'] == 1
            fixture.assert_clean_history(retried)
            report['cases'].append('Typed Nexus handler errors enforced nonretryable rejection and server-driven retry with stable request ID')
            timed_out = fixture.start('timeout', 'v.nexus', {'endpoint': endpoint, 'delay_ms': 3000, 'timeout_ms': 200})
            result = fixture.call('result', timed_out)
            assert result['status'] == 'completed' and result['data'] == '"Nexus timed out"', result
            fixture.assert_clean_history(timed_out)
            report['cases'].append('Nexus schedule-to-close timeout resolved to a typed timeout event')
            count_before = (LOGS / 'nexus-handler-before.log').read_text().count('NEXUS_START')
            pending = fixture.start('shutdown', 'v.nexus', {'endpoint': endpoint, 'name': 'shutdown', 'delay_ms': 2000})
            fixture.wait_for(lambda: (LOGS / 'nexus-handler-before.log').read_text().count('NEXUS_START') > count_before)
            nexus.terminate()
            assert nexus.wait(timeout=15) == 0
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'handler-after')
            result = fixture.call('result', pending)
            assert result['status'] == 'completed' and 'shutdown' in result['data'], result
            fixture.assert_clean_history(pending)
            report['cases'].append('SIGTERM cancelled/drained in-flight Nexus request; replacement handler served retry')
            asynchronous = fixture.start('async', 'v.nexus', {'endpoint': endpoint, 'operation': 'async', 'name': 'durable', 'delay_ms': 3000})
            fixture.wait_for(lambda: counts(fixture.history(asynchronous))['nexusoperationstarted'] == 1)
            started = next(event for event in fixture.history(asynchronous)['events'] if event['eventType'] == 'EVENT_TYPE_NEXUS_OPERATION_STARTED')
            target = {'workflow_id': 'nexus-' + started['nexusOperationStartedEventAttributes']['requestId']}
            links = fixture.call('history-links', target)
            assert any(link['target_workflow_id'] == asynchronous['workflow_id'] and link['target_run_id'] == asynchronous['run_id'] for link in links), links
            (ARTIFACTS / 'nexus-server-links.json').write_text(json.dumps(links, indent=2))
            report['cases'].append('Workflow-backed Nexus handler decoded the incoming workflow-event link and Temporal persisted it with the durable completion callback')
            incoming = (LOGS / 'nexus-handler-after.log').read_text()
            assert 'NEXUS_INCOMING_LINKS count=1' in incoming, incoming[-1500:]
            worker.kill(); nexus.kill()
            assert worker.wait(timeout=5) == -9 and nexus.wait(timeout=5) == -9
            worker = fixture.workflows('async-after')
            result = fixture.call('result', asynchronous)
            assert result['status'] == 'completed' and json.loads(result['data']) == 'Hello from async Nexus, durable!', result
            fixture.assert_clean_history(asynchronous)
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'async-handler-after')
            cancelled = fixture.start('async-cancel', 'v.nexus', {'endpoint': endpoint, 'operation': 'async', 'delay_ms': 10000})
            fixture.wait_for(lambda: counts(fixture.history(cancelled))['nexusoperationstarted'] == 1)
            fixture.call('signal', cancelled, name='cancel')
            result = fixture.call('result', cancelled)
            assert result['status'] == 'completed' and result['data'] == '"Nexus cancelled"', result
            fixture.assert_clean_history(cancelled)
            failed = fixture.start('async-fail', 'v.nexus', {'endpoint': endpoint, 'operation': 'async-fail'})
            result = fixture.call('result', failed)
            assert result['status'] == 'failed' and 'async Nexus workflow failed' in result['message'], result
            fixture.assert_clean_history(failed)
            report['cases'].append('Workflow-backed async Nexus completion survived loss of both workers; durable callbacks propagated cancellation and failure')
            file = ARTIFACTS / 'binary-histories' / 'nexus.bin'
            file.parent.mkdir(exist_ok=True)
            fixture.call('history', first, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', first['workflow_id'],
                                     '--task-queue', fixture.queue], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            report['cases'].append('Nexus operation history replayed offline')
            worker.terminate(); nexus.terminate()
            assert worker.wait(timeout=15) == 0 and nexus.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

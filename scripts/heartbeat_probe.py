"""Heartbeat payload lists and liveness preserve progress across worker loss."""

RESULT_REPORT = 'heartbeat-results.json'
import base64
import json
import subprocess
import time
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('heartbeat') as fixture:
            fixture.env['V_SDK_EXAMPLE_CODEC'] = 'prefix'
            workflow = fixture.workflows()
            activity = fixture.activities('before')
            handle = fixture.start('workflow', 'v.heartbeat-state', {})
            metadata = fixture.call('metadata', handle)
            assert metadata['definition']['type'] == 'v.heartbeat-state', metadata
            assert metadata['definition']['signalDefinitions'] == [{'name': '', 'description': ''}], metadata
            def pending():
                result = subprocess.run([str(CLI), 'workflow', 'describe', '--address', fixture.address,
                                         '--workflow-id', handle['workflow_id'], '--raw', '--no-json-shorthand-payloads', '--output', 'json'], capture_output=True, text=True, timeout=10)
                assert result.returncode == 0, result.stderr
                return json.loads(result.stdout).get('pendingActivities', [])
            def progress():
                items = pending()
                return items and len(items[0].get('heartbeatDetails', {}).get('payloads', [])) == 2
            fixture.wait_for(progress)
            time.sleep(.35)  # Several liveness heartbeats must retain the application checkpoint.
            items = pending()
            payloads = items[0]['heartbeatDetails']['payloads']
            values = [base64.b64decode(payload['data']) for payload in payloads]
            expected = [b'{"cursor":7}', '"opaque 世界"'.encode()]
            assert values == [bytes([0, ord('B'), 255, 0, ord('A'), 255]) + value for value in expected], values
            (ARTIFACTS / 'heartbeat-pending.json').write_text(json.dumps(items, indent=2) + '\n')
            activity.kill(); assert activity.wait(timeout=5) == -9
            activity = fixture.activities('after')
            result = fixture.call('result', handle)
            assert result['status'] == 'completed' and json.loads(result['data']) == [value.decode() for value in expected], result
            fixture.assert_clean_history(handle)
            history = fixture.history(handle)
            started = [event['activityTaskStartedEventAttributes'] for event in history['events'] if 'activityTaskStartedEventAttributes' in event]
            assert started[-1]['attempt'] >= 2, started
            file = ARTIFACTS / 'binary-histories/heartbeat.bin'
            fixture.call('history', handle, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            for process in (workflow, activity):
                process.terminate(); assert process.wait(timeout=15) == 0
            report['cases'] += ['Two heartbeat details retained their payload boundaries and exact codec frames in independent server description',
                                'Cooperative sleep preserved the checkpoint across repeated liveness heartbeats; retry after SIGKILL received both original values',
                                'Completed heartbeat retry history replayed offline and both worker loops drained']
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

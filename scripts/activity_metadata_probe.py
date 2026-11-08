"""Activity IDs, scheduling information, priority and command summaries through replay."""

RESULT_REPORT = 'activity-metadata-results.json'
import base64
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def summary(event):
    return json.loads(base64.b64decode(event['userMetadata']['summary']['data']))


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('activity-metadata') as fixture:
            worker = fixture.workflows('before')
            activities = fixture.activities()
            remote = fixture.start('remote', 'v.activity-metadata', False)
            local = fixture.start('local', 'v.activity-metadata', True)
            def ready():
                histories = [fixture.history(handle) for handle in (remote, local)]
                return all(any(summary(event) == 'result ready 世界' for event in history['events']
                               if event.get('userMetadata', {}).get('summary')) for history in histories)
            fixture.wait_for(ready)
            worker.kill(); assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            for handle, is_local in ((remote, False), (local, True)):
                try:
                    result = fixture.call('result', handle)
                except BaseException:
                    fixture.history(handle)
                    raise
                assert result['status'] == 'completed', result
                info = json.loads(result['data'])
                assert info['activity_id'] == ('custom-local' if is_local else 'custom-remote'), info
                assert info['workflow_type'] == 'v.activity-metadata' and info['task_queue'] == fixture.queue, info
                assert info['attempt'] == (2 if is_local else 1), info
                assert info['start_to_close'] == 20_000_000_000 and info['schedule_to_close'] == 60_000_000_000, info
                assert info['scheduled_at'] and info['started_at'] and info['current_attempt_scheduled_at'], info
                assert info['retry_policy']['maximum_attempts'] == 2, info
                history = fixture.history(handle)
                fixture.assert_clean_history(handle)
                if is_local:
                    markers = [event for event in history['events'] if 'markerRecordedEventAttributes' in event]
                    assert any(summary(event) == 'local retry summary 世界' for event in markers if event.get('userMetadata')), markers
                else:
                    assert info['heartbeat_timeout'] == 3_000_000_000, info
                    assert info['priority']['priority_key'] == 3 and info['priority']['fairness_key'] == 'tenant-a', info
                    scheduled = next(event for event in history['events'] if 'activityTaskScheduledEventAttributes' in event)
                    attributes = scheduled['activityTaskScheduledEventAttributes']
                    assert attributes['activityId'] == 'custom-remote' and attributes['priority']['priorityKey'] == 3, attributes
                    assert summary(scheduled) == 'remote summary 世界', scheduled
                file = ARTIFACTS / f'binary-histories/activity-metadata-{is_local}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            cancelled = fixture.start('cancel-retry', 'v.activity-options', {
                'local': True, 'fail_until': 1, 'delay_ms': 500,
                'retry_ms': 100, 'threshold_ms': 1, 'cancel_after_ms': 1000})
            outcome = fixture.call('result', cancelled)
            assert outcome['status'] == 'completed' and outcome['data'] == '"activity cancelled"', outcome
            fixture.assert_clean_history(cancelled)
            for process in (worker, activities):
                process.terminate(); assert process.wait(timeout=15) == 0
            report['cases'] += ['Custom remote/local Activity IDs, retry policy, timestamps and timeout information reached handlers',
                                'Remote priority and Unicode summaries on Activities, local markers and timers matched independent Temporal history',
                                'Local durable retry preserved summary metadata; both workflows survived SIGKILL and replayed offline',
                                'Cancellation through the original Activity handle interrupted a later local retry attempt']
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

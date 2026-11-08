"""Local/remote Activity retries, cancellation, durable backoff and replay."""

RESULT_REPORT = 'activities-results.json'
import json

from integration import TemporalFixture, ARTIFACTS, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('activities') as fixture:
            worker = fixture.workflows('before')
            remote_worker = fixture.activities()
            local = fixture.start('local', 'v.activity-options', {'local': True, 'fail_until': 2})
            remote = fixture.start('remote', 'v.activity-options', {'fail_until': 2})
            fixture.wait_for(lambda: counts(fixture.history(local))['markerrecorded'] >= 1 and counts(fixture.history(local))['timerstarted'] >= 1)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            for handle, is_local in ((local, True), (remote, False)):
                result = fixture.call('result', handle)
                assert result['status'] == 'completed', result
                info = json.loads(result['data'])
                assert info['attempt'] == 3 and info['is_local'] == is_local, info
                events = fixture.assert_clean_history(handle)
                if is_local:
                    assert events['activitytaskscheduled'] == 0, events
                else:
                    assert events['activitytaskscheduled'] == 1 and events['activitytaskcompleted'] == 1, events
            report['cases'].append('Local and remote Activities retried twice, exposed attempt=3, and completed; local result survived replay')

            backoff = fixture.start('backoff', 'v.activity-options', {'local': True, 'fail_until': 1, 'retry_ms': 1500, 'threshold_ms': 10})
            fixture.wait_for(lambda: counts(fixture.history(backoff))['timerstarted'] >= 1)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after-backoff')
            result = fixture.call('result', backoff)
            assert result['status'] == 'completed' and json.loads(result['data'])['attempt'] == 2, result
            assert fixture.assert_clean_history(backoff)['timerstarted'] >= 2
            report['cases'].append('Long local retry backoff used a durable timer and resumed at attempt=2 after SIGKILL')

            for is_local in (False, True):
                permanent = fixture.start(f'permanent-{is_local}', 'v.activity-options', {'local': is_local, 'fail_until': 10, 'non_retryable': True})
                result = fixture.call('result', permanent)
                assert result['status'] == 'failed' and 'failure 1' in result['message'], result
                fixture.assert_clean_history(permanent)
                cancel = fixture.start(f'cancel-{is_local}', 'v.activity-options', {'local': is_local, 'delay_ms': 5000, 'cancel_after_ms': 100})
                result = fixture.call('result', cancel)
                assert result['status'] == 'completed' and result['data'] == '"activity cancelled"', result
                fixture.assert_clean_history(cancel)
            cancel_backoff = fixture.start('cancel-backoff', 'v.activity-options',
                                           {'local': True, 'fail_until': 10, 'retry_ms': 5000, 'threshold_ms': 10, 'cancel_after_ms': 500})
            assert fixture.call('result', cancel_backoff)['data'] == '"activity cancelled"'
            assert fixture.assert_clean_history(cancel_backoff)['timercanceled'] == 1
            report['cases'].append('Non-retryable application errors stopped retries; running local/remote and backed-off local Activities cancelled')
            cancellation_logs = (ARTIFACTS / 'logs/activities-activity.log').read_text()
            assert any('Activity cancellation observed' in line and 'reason=cancelled' in line for line in cancellation_logs.splitlines())
            report['cases'].append('Activity handlers observed the typed Core cancellation reason rather than a boolean')
            for local in (False, True):
                default = fixture.start('default-' + str(local), 'v.default-retry', {'local': local, 'fail_until': 2})
                result = fixture.call('result', default)
                assert result['status'] == 'completed' and json.loads(result['data'])['attempt'] == 3, result
                fixture.assert_clean_history(default)
            report['cases'].append('Default remote/local Activity retry policies retried transient failures through attempt three without an explicit maximum')
            for process in (worker, remote_worker):
                process.terminate()
                assert process.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

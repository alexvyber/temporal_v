"""Complete deferred Activities from a separate V client, including worker loss."""

RESULT_REPORT = 'async-activity-results.json'
import json
from pathlib import Path
import tempfile

from integration import TemporalFixture, ARTIFACTS, LOGS


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('async-activity') as fixture, tempfile.TemporaryDirectory(prefix='temporal-v-tokens-') as temp:
            fixture.env['V_SDK_EXAMPLE_ASYNC_TOKEN_DIR'] = temp
            worker = fixture.workflows('before')
            activities = fixture.activities('before-activity')
            for by_token in (True, False):
                for outcome in ('complete', 'fail', 'cancel'):
                    handle = fixture.start(f'{by_token}-{outcome}', 'v.async-activity', {})
                    file = Path(temp) / (handle['run_id'] + '.json')
                    fixture.wait_for(lambda: file.exists() and file.stat().st_size > 0)
                    assert file.stat().st_mode & 0o077 == 0, 'task token file is not private'
                    options = {'token_file': file} if by_token else {'activity_id': 'activity-1'}
                    if by_token and outcome == 'complete':
                        worker.kill(); activities.kill()
                        assert worker.wait(timeout=5) == -9 and activities.wait(timeout=5) == -9
                    heartbeat = fixture.call('activity-heartbeat', handle, input='{"progress":1}', **options)
                    assert not json.loads(heartbeat['data'])['cancel_requested']
                    if outcome == 'complete':
                        fixture.call('activity-complete', handle, input='{"external":42}', **options)
                        if by_token:
                            worker = fixture.workflows('after')
                            activities = fixture.activities('after-activity')
                        result = fixture.call('result', handle)
                        assert result['status'] == 'completed' and json.loads(result['data']) == {'external': 42}, result
                    elif outcome == 'fail':
                        fixture.call('activity-fail', handle, input='{"reason":"external"}', **options)
                        result = fixture.call('result', handle)
                        assert result['status'] == 'failed' and 'external Activity failure' in result['message'], result
                    else:
                        fixture.call('signal', handle, name='cancel')
                        fixture.wait_for(lambda: json.loads(fixture.call('activity-heartbeat', handle, **options)['data'])['cancel_requested'])
                        fixture.call('activity-cancel', handle, **options)
                        result = fixture.call('result', handle)
                        assert result['status'] == 'completed' and result['data'] == '"async cancelled"', result
                    fixture.assert_clean_history(handle)
                    file.unlink()
            report['cases'].append('Deferred remote Activities completed, failed and acknowledged cancellation by token and by execution ID')
            report['cases'].append('External completion survived SIGKILL of both workers and resumed through replay')
            report['cases'].append('External heartbeat exposed cancellation requests; task tokens stayed in private temporary files and were removed')
            worker.terminate(); activities.terminate()
            assert worker.wait(timeout=15) == 0 and activities.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

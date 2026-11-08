"""Recorded nanoseconds match server history across restart and offline replay."""

RESULT_REPORT = 'clock-results.json'
import calendar
from datetime import datetime
import json
import re
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def timestamp_nanos(text):
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z', text)
    assert match, text
    seconds = calendar.timegm(datetime.strptime(match[1], '%Y-%m-%dT%H:%M:%S').timetuple())
    return seconds * 1000000000 + int((match[2] or '').ljust(9, '0'))


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('recorded-clock') as fixture:
            worker = fixture.workflows('before')
            handle = fixture.start('clock', 'v.clock', {})
            fixture.wait_for(lambda: counts(fixture.history(handle))['timerstarted'] == 2, timeout=30)
            before = json.loads(fixture.call('query', handle, name='clock')['data'])
            assert len(before['seconds']) == 2, before
            worker.kill(); worker.wait(timeout=10)
            replacement = fixture.workflows('after')
            after = json.loads(fixture.call('query', handle, name='clock')['data'])
            assert after == before, (before, after)
            fixture.call('signal', handle, name='finish')
            outcome = fixture.call('result', handle)
            assert outcome['status'] == 'completed', outcome
            result = json.loads(outcome['data'])
            actual = [seconds * 1000000000 + nanos for seconds, nanos in zip(result['seconds'], result['nanoseconds'])]
            assert len(actual) == 3 and actual == sorted(actual), actual
            assert any(value % 1000000 for value in actual), actual
            assert result['millis'] == [value // 1000000 for value in actual]
            history = fixture.history(handle)
            recorded = {timestamp_nanos(event['eventTime']) for event in history['events'] if event['eventType'] == 'EVENT_TYPE_WORKFLOW_TASK_STARTED'}
            assert set(actual) <= recorded, (actual, recorded)
            fixture.assert_clean_history(handle)
            file = ARTIFACTS / 'binary-histories/recorded-clock.bin'
            fixture.call('history', handle, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            replacement.terminate(); assert replacement.wait(timeout=20) == 0
            report.update(passed=True, nanoseconds=actual)
            report['cases'].append('now() preserved exact server-recorded nanoseconds and the millisecond metadata view; query state survived SIGKILL and the resulting commands replayed offline')
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

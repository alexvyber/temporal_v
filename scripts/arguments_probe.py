"""Lossless multiple arguments, arbitrary binary bytes, codecs and crash/replay."""

RESULT_REPORT = 'arguments-results.json'
import base64
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT


def driver(fixture, mode):
    result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env,
                            capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('arguments') as fixture:
            fixture.env['V_SDK_EXAMPLE_CODEC'] = 'binary-prefix'
            worker = fixture.workflows('before')
            activity = fixture.activities('activity-before')
            handles = driver(fixture, 'arguments-start')
            for handle in handles:
                events = fixture.history(handle)['events']
                payloads = events[0]['workflowExecutionStartedEventAttributes']['input']['payloads']
                assert len(payloads) == 3, payloads
                assert base64.b64decode(payloads[1]['data']) == bytes([0, ord('B'), 255, 0, ord('A'), 255, 0, 255, 128, 1, 2])
            worker.kill(); activity.kill()
            assert worker.wait(timeout=5) == -9 and activity.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            activity = fixture.activities('activity-after')
            report['cases'] += driver(fixture, 'arguments-finish')
            for index, handle in enumerate(handles):
                fixture.assert_clean_history(handle)
                file = ARTIFACTS / f'binary-histories/arguments-{index}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue],
                                        cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            report['cases'].append('Independent server history showed three distinct payloads and exact framed binary bytes; both histories replayed offline')
            for process in (worker, activity):
                process.terminate()
                assert process.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

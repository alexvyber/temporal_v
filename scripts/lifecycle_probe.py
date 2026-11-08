"""Workflow close details, chain guards and replayed signal identity."""

RESULT_REPORT = 'lifecycle-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('lifecycle') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            worker = fixture.workflows('before')
            result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'workflow-lifecycle'], cwd=ROOT, env=fixture.env,
                                    capture_output=True, text=True, timeout=60)
            (ARTIFACTS / 'logs/lifecycle-driver.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr[-4000:]
            report['cases'] += json.loads(result.stdout)
            signal = fixture.start('signal', 'v.signal-metadata', {})
            fixture.call('signal', signal, name='record')
            fixture.wait_for(lambda: json.loads(fixture.call('query', signal, name='state')['data'])['originating_event_id'] > 0)
            before = json.loads(fixture.call('query', signal, name='state')['data'])
            event = next(event for event in fixture.history(signal)['events'] if 'workflowExecutionSignaledEventAttributes' in event)
            assert before['originating_event_id'] == int(event['eventId'])
            assert before['sender_identity'] == event['workflowExecutionSignaledEventAttributes']['identity'] == 'temporal-v'
            worker.kill(); assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            assert json.loads(fixture.call('query', signal, name='state')['data']) == before
            fixture.call('signal', signal, name='finish')
            assert json.loads(fixture.call('result', signal)['data']) == before
            handles = [signal] + [{'workflow_id': fixture.queue + suffix} for suffix in ('-cancel-details', '-terminate-details', '-timeout-result', '-failed-result')]
            for index, handle in enumerate(handles):
                fixture.assert_clean_history(handle)
                file = ARTIFACTS / f'binary-histories/lifecycle-{index}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'],
                                         '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            worker.terminate(); assert worker.wait(timeout=15) == 0
            report['cases'].append('Signal sender identity and originating history event ID matched independent history, survived SIGKILL, and all close histories replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

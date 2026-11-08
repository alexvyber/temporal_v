"""Failure cause/detail fidelity, captured rethrow and Update error isolation."""

RESULT_REPORT = 'failure-chain-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        for custom in ('', 'tagged', 'encoded'):
            with TemporalFixture('failure-chain-' + (custom or 'default')) as fixture:
                fixture.env['V_SDK_EXAMPLE_CODEC'] = 'prefix'
                fixture.env['V_SDK_EXAMPLE_FAILURE_CONVERTER'] = custom
                workflow = fixture.workflows()
                activity = fixture.activities()
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'failure-chains'], cwd=ROOT, env=fixture.env,
                                        capture_output=True, text=True, timeout=70)
                assert result.returncode == 0, result.stderr
                report['cases'] += json.loads(result.stdout)
                for mode in ('remote', 'local'):
                    handle = {'workflow_id': fixture.queue + '-failure-' + mode}
                    fixture.assert_clean_history(handle)
                    if custom == 'encoded':
                        history = fixture.history(handle)
                        failure = next(event['workflowExecutionFailedEventAttributes']['failure'] for event in history['events'] if 'workflowExecutionFailedEventAttributes' in event)
                        encoded_count = 0
                        while failure:
                            if failure.get('applicationFailureInfo', {}).get('type') in ('OuterFailure', 'InnerFailure', 'VError'):
                                assert failure['message'] == 'Encoded failure' and not failure.get('stackTrace'), failure
                                assert failure.get('encodedAttributes'), failure
                                encoded_count += 1
                            failure = failure.get('cause')
                        assert encoded_count == 3, encoded_count
                    file = ARTIFACTS / f'binary-histories/failure-chain-{custom or "default"}-{mode}.bin'
                    fixture.call('history', handle, file=file)
                    replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue],
                                            cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                    assert replay.returncode == 0, replay.stderr
                for process in (workflow, activity):
                    process.terminate()
                    assert process.wait(timeout=15) == 0
                report['cases'].append((custom or 'default') + ': remote/local captured failures replayed offline; converter and codec policy preserved')
        with TemporalFixture('failure-converter-retry') as fixture:
            fixture.env['V_SDK_EXAMPLE_FAILURE_CONVERTER'] = 'failing'
            fixture.workflows(); fixture.activities()
            for local in (False, True):
                handle = fixture.start(str(local), 'v.default-retry', {'local': local, 'fail_until': 1})
                outcome = fixture.call('result', handle)
                assert outcome['status'] == 'completed' and json.loads(outcome['data'])['attempt'] == 2, outcome
                fixture.assert_clean_history(handle)
            report['cases'].append('A failing custom error encoder produced retryable fallback failures; remote/local Activities recovered on attempt two')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

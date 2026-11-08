"""Child options, cancellation races, parent close policy and workflow retries."""

RESULT_REPORT = 'children-results.json'
import base64
import json

from integration import TemporalFixture, ARTIFACTS


def started(history):
    return history['events'][0]['workflowExecutionStartedEventAttributes']


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('children') as fixture:
            worker = fixture.workflows()
            for mode in ('retry', 'duplicate', 'cancel_before', 'cancel'):
                parent = fixture.start(mode, 'v.child-options', {'mode': mode})
                result = fixture.call('result', parent)
                expected = '3' if mode == 'retry' else ('"duplicate rejected"' if mode == 'duplicate' else '"child cancelled"')
                assert result['status'] == 'completed' and result['data'] == expected, (mode, result)
                fixture.assert_clean_history(parent)
            report['cases'].append('Child retries, duplicate-ID rejection, cancel-before-start and acknowledged running cancellation')

            for policy, terminal in (('terminate', 'terminated'), ('request_cancel', 'canceled'), ('abandon', None)):
                parent = fixture.start('close-' + policy, 'v.child-options', {'mode': 'close', 'close_policy': policy})
                result = fixture.call('result', parent)
                assert result['status'] == 'completed', result
                child = json.loads(result['data'])
                if terminal:
                    assert fixture.call('result', child)['status'] == terminal
                else:
                    assert fixture.call('describe', child)['status'] == 'workflow_execution_status_running'
                    fixture.call('cancel', child)
                    assert fixture.call('result', child)['status'] == 'canceled'
                attributes = started(fixture.history(child))
                assert attributes['workflowTaskTimeout'] == '5s' and attributes['workflowRunTimeout'] == '30s', attributes
                assert base64.b64decode(attributes['header']['fields']['origin']['data']) == b'"parent"'
                assert base64.b64decode(attributes['memo']['fields']['purpose']['data']) == b'"child-test"'
                metadata = fixture.history(child)['events'][0]['userMetadata']
                assert json.loads(base64.b64decode(metadata['summary']['data'])) == 'child summary 世界'
                assert json.loads(base64.b64decode(metadata['details']['data'])) == 'child details 世界'
                fixture.assert_clean_history(parent)
            report['cases'].append('Parent-close terminate/request-cancel/abandon policies and child timeouts, headers and memo matched server history')

            retry = fixture.call('start', workflow_id=fixture.queue + '-client-retry', type='v.retry', input='{}', maximum_attempts=3, retry_ms=20)
            initial = fixture.call('result', retry)
            fixture.history(retry)
            assert initial['status'] in ('failed', 'continued_as_new') and initial['next_run_id'], initial
            chain = fixture.call('chain-result', retry)
            assert chain['status'] == 'completed' and chain['data'] == '3', chain
            report['cases'].append('V client followed retry run IDs to workflow attempt=3')

            continuation = fixture.call('start', workflow_id=fixture.queue + '-continuation', type='v.chain', input='{"remaining":1}',
                                        task_timeout_seconds=7, run_timeout_seconds=25)
            first = fixture.call('result', continuation)
            assert first['status'] == 'continued_as_new', first
            next_run = dict(continuation, run_id=first['next_run_id'])
            assert fixture.call('result', next_run)['status'] == 'completed'
            attributes = started(fixture.history(next_run))
            assert attributes['workflowTaskTimeout'] == '7s' and attributes['workflowRunTimeout'] == '25s', attributes
            report['cases'].append('Continue-as-new inherited configured run/task timeouts')
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

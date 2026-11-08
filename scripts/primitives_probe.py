"""Durable external operations, timer cancellation and deterministic RNG replay."""

RESULT_REPORT = 'primitives-results.json'
import json
import uuid

from integration import TemporalFixture, ARTIFACTS, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('primitives') as fixture:
            worker = fixture.workflows('before')
            timer = fixture.start('timers', 'v.timer-cancel', {})
            assert fixture.call('result', timer)['data'] == '"cancelled"'
            events = fixture.assert_clean_history(timer)
            assert events['timerstarted'] == 2 and events['timercanceled'] == 1 and events['timerfired'] == 1, events
            report['cases'].append('Sub-second durable timer fired; explicit timer cancellation was idempotent')

            receiver = fixture.start('receiver', 'v.counter', {})
            sender = fixture.start('sender', 'v.external', {'mode': 'signal', 'workflow_id': receiver['workflow_id']})
            fixture.wait_for(lambda: counts(fixture.history(sender))['timerstarted'] == 1)
            assert json.loads(fixture.call('query', receiver, name='state')['data'])['total'] == 5
            random_run = fixture.start('random', 'v.random', {})
            fixture.wait_for(lambda: counts(fixture.history(random_run))['timerstarted'] == 1)
            before = json.loads(fixture.call('query', random_run, name='state')['data'])
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            random_result = json.loads(fixture.call('result', random_run)['data'])
            assert random_result[:2] == before and len(random_result) == 3, (before, random_result)
            assert uuid.UUID(random_result[0]).version == 4 and uuid.UUID(random_result[2]).version == 4
            assert random_result[0] != random_result[2]
            assert not json.loads(fixture.call('result', sender)['data'])['failed']
            state = json.loads(fixture.call('query', receiver, name='state')['data'])
            assert state['total'] == 5 and state['additions'] == [5], state
            assert fixture.assert_clean_history(sender)['externalworkflowexecutionsignaled'] == 1
            fixture.assert_clean_history(random_run)
            report['cases'].append('History-seeded UUID/random stream replayed identically after SIGKILL')
            report['cases'].append('External signal was acknowledged and not repeated during replay')

            cancel = fixture.start('cancel', 'v.external', {'mode': 'cancel', 'workflow_id': receiver['workflow_id']})
            assert not json.loads(fixture.call('result', cancel)['data'])['failed']
            assert fixture.call('result', receiver)['status'] == 'canceled'
            assert fixture.assert_clean_history(cancel)['externalworkflowexecutioncancelrequested'] == 1
            for mode in ('signal', 'cancel'):
                missing = fixture.start(f'missing-{mode}', 'v.external', {'mode': mode, 'workflow_id': 'nonexistent-' + fixture.queue})
                result = json.loads(fixture.call('result', missing)['data'])
                assert result['failed'] and result['message'], result
                fixture.assert_clean_history(missing)
            report['cases'].append('External cancellation and missing-target failures resolved without workflow-task errors')
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

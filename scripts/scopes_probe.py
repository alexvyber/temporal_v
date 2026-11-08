"""Nested/shielded cancellation and durable cleanup after workflow cancellation."""

RESULT_REPORT = 'scopes-results.json'
import json
import subprocess

from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('scopes') as fixture:
            worker = fixture.workflows('before')
            fixture.activities()
            manual = fixture.start('manual', 'v.scopes', {'mode': 'manual'})
            result = fixture.call('result', manual)
            assert result['status'] == 'completed', result
            assert json.loads(result['data']) == {'normal_cancelled': True, 'shield_completed': True, 'rejected_new_work': True}, result
            fixture.assert_clean_history(manual)
            report['cases'].append('Parent scope cancellation propagated to nested work, preserved shielded sibling and rejected new work in cancelled scope')
            cleanup = fixture.start('cleanup', 'v.scopes', {'mode': 'cleanup'})
            fixture.wait_for(lambda: counts(fixture.history(cleanup))['activitytaskscheduled'] == 1)
            fixture.call('cancel', cleanup)
            fixture.wait_for(lambda: counts(fixture.history(cleanup))['activitytaskscheduled'] == 2)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            result = fixture.call('result', cleanup)
            assert result['status'] == 'canceled', result
            events = fixture.assert_clean_history(cleanup)
            assert events['activitytaskcompleted'] == 1 and events['activitytaskcanceled'] == 1, events
            report['cases'].append('Workflow cancellation delivered an event; shielded cleanup completed after SIGKILL before workflow acknowledged cancellation')
            file = ARTIFACTS / 'binary-histories/scopes.bin'
            fixture.call('history', cleanup, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', cleanup['workflow_id'],
                                     '--task-queue', fixture.queue], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            report['cases'].append('Cancellation/cleanup history replayed offline')
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

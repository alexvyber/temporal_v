"""Workflow-task failure blocks execution, permits other work, and recovers with fixed code."""

RESULT_REPORT = 'task-failure-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('task-failure') as fixture:
            worker = fixture.workflows('before')
            blocked = fixture.start('blocked', 'v.task-failure', {})
            fixture.wait_for(lambda: counts(fixture.history(blocked))['workflowtaskfailed'] >= 1)
            events = counts(fixture.history(blocked))
            assert events['workflowexecutionfailed'] == 0 and worker.poll() is None, events
            healthy = fixture.start('healthy', 'v.counter', {'initial': 17})
            assert json.loads(fixture.call('query', healthy, name='state')['data'])['total'] == 17
            fixture.call('signal', healthy, name='finish')
            assert fixture.call('result', healthy)['status'] == 'completed'
            worker.kill()
            assert worker.wait(timeout=5) == -9
            fixture.env['V_SDK_EXAMPLE_TASK_ERROR_FIXED'] = 'true'
            worker = fixture.workflows('after')
            result = fixture.call('result', blocked)
            assert result['status'] == 'completed' and result['data'] == '"repaired"', result
            events = counts(fixture.history(blocked))
            assert events['workflowexecutionfailed'] == 0 and events['workflowexecutioncompleted'] == 1, events
            file = ARTIFACTS / 'binary-histories/task-failure.bin'
            fixture.call('history', blocked, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', blocked['workflow_id'], '--task-queue', fixture.queue],
                                    cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            report['cases'].append('WorkflowTaskError failed a task without failing execution; unrelated workflow completed; repaired deployment resumed and history replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

"""Real eager tasks, shared-client ownership, fallback, replacement and replay."""

RESULT_REPORT = 'eager-workflow-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, WORKFLOW_WORKER


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('eager-workflows') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'eager-workflows'], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=90)
            (ARTIFACTS / 'logs/eager-workflows-driver.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr[-5000:]
            report['cases'] += json.loads(result.stdout)
            for label, value, queue in (('main', 11, '-eager'), ('fallback', 22, '-fallback'), ('other', 33, '-fallback'), ('after-close', 44, '-fallback')):
                handle = {'workflow_id': fixture.queue + '-eager-' + label}
                observed = fixture.assert_clean_history(handle)
                assert observed['workflowexecutionstarted'] == observed['workflowexecutioncompleted'] == 1
                worker = fixture.launch(WORKFLOW_WORKER, 'replacement-' + label, env=dict(fixture.env, TEMPORAL_TASK_QUEUE=fixture.queue + queue))
                assert json.loads(fixture.call('query', handle, name='state')['data'])['total'] == value
                file = ARTIFACTS / f'binary-histories/eager-{label}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue + queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
                worker.terminate(); assert worker.wait(timeout=15) == 0
            report['cases'].append('Replacement workers reconstructed all eager/fallback histories and offline replay passed without workflow-task failures')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

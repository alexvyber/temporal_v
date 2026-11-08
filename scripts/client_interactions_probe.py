"""Converter-aware atomic signal-with-start, query policies and restart replay."""

RESULT_REPORT = 'client-interactions-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT
from restart_probe import counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('client-interactions') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            worker = fixture.workflows('before')
            for mode in ('client-interactions-start', 'client-interactions-finish'):
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / f'logs/{mode}.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-5000:]
                report['cases'] += json.loads(result.stdout)
                if mode.endswith('start'):
                    worker.kill(); assert worker.wait(timeout=5) == -9
                    worker = fixture.workflows('after')
            for label, signal_count in (('main', 3), ('race', 21), ('failed', 1)):
                handle = {'workflow_id': fixture.queue + '-interaction-' + label}
                observed = fixture.assert_clean_history(handle)
                assert observed['workflowexecutionstarted'] == 1, observed
                assert observed['workflowexecutionsignaled'] == signal_count, observed
                file = ARTIFACTS / f'binary-histories/client-interactions-{label}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'],
                                         '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            worker.terminate(); assert worker.wait(timeout=15) == 0
            report['cases'].append('Independent histories confirmed exactly one start and every unique signal; all three histories replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

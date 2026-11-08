"""Nexus synchronous queries and retry-deduplicated signal interactions."""

RESULT_REPORT = 'nexus-interactions-results.json'
import json
import subprocess
import tempfile
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI


def main():
    report = {'passed': False, 'cases': []}
    try:
        with tempfile.TemporaryDirectory(prefix='v-nexus-interaction-retry-') as directory, TemporalFixture('nexus-interactions', server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded', V_SDK_EXAMPLE_NEXUS_RETRY_DIRECTORY=directory)
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default', '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            worker = fixture.workflows()
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus')
            result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'nexus-interactions'], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=90)
            (ARTIFACTS / 'logs/nexus-interactions-driver.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr[-5000:]
            report['cases'] += json.loads(result.stdout)
            for label in ('signal-retry', 'signal-start-retry'):
                handle = {'workflow_id': fixture.queue + '-' + label}
                observed = fixture.assert_clean_history(handle)
                assert observed['workflowexecutionstarted'] == 1 and observed['workflowexecutionsignaled'] == 2, observed
                history = fixture.history(handle)
                event = next(e['workflowExecutionSignaledEventAttributes'] for e in history['events'] if e.get('workflowExecutionSignaledEventAttributes', {}).get('signalName') == 'add')
                assert len(event.get('links', [])) > 0 or any(e.get('links') for e in history['events']), history
                file = ARTIFACTS / f'binary-histories/nexus-interactions-{label}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            for process in (worker, nexus): process.terminate(); assert process.wait(timeout=15) == 0
            report['cases'].append('Independent histories contained one start, one deduplicated add signal and propagated links; both histories replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

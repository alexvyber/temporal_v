"""Propagated Nexus workflow results, separate input scope and run-chain replay."""

RESULT_REPORT = 'result-context-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI
from context_probe import payloads, context_of


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('result-context', server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default',
                                      '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            worker = fixture.workflows()
            result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'result-contexts'], cwd=ROOT, env=fixture.env,
                                    capture_output=True, text=True, timeout=60)
            (ARTIFACTS / 'logs/result-context-driver.log').write_text(result.stdout + result.stderr)
            if result.returncode != 0:
                fixture.history({'workflow_id': fixture.queue + '-result-scope'})
            assert result.returncode == 0, result.stderr[-4000:]
            report['cases'] += json.loads(result.stdout)
            handles = [{'workflow_id': fixture.queue + suffix} for suffix in ('-result-scope', '-result-failure', '-result-cancel', '-result-continue')]
            latest = fixture.history(handles[-1])
            handles.append({**handles[-1], 'run_id': latest['events'][0]['workflowExecutionStartedEventAttributes']['firstExecutionRunId']})
            observed = []
            for index, handle in enumerate(handles):
                history = fixture.history(handle)
                fixture.assert_clean_history(handle)
                for event in history['events']:
                    for name in ('workflowExecutionCompletedEventAttributes', 'workflowExecutionFailedEventAttributes', 'workflowExecutionCanceledEventAttributes'):
                        if name not in event: continue
                        for payload in payloads(event[name]):
                            if 'temporal-v/codec-context' not in payload['metadata']: continue
                            scope = context_of(payload)
                            assert scope['kind'] == 'nexus' and scope['endpoint'] == fixture.queue + '-endpoint', scope
                            assert scope['service'] == 'v-service' and scope['operation'] == 'async', scope
                            observed.append(scope)
                file = ARTIFACTS / f'binary-histories/result-context-{index}.bin'
                fixture.call('history', handle, file=file)
                replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'],
                                         '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                assert replay.returncode == 0, replay.stderr
            assert len(observed) >= 5, observed
            worker.terminate(); assert worker.wait(timeout=15) == 0
            report['cases'].append('Independent terminal history payloads carried the Nexus target and all histories, including both continuation runs, replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

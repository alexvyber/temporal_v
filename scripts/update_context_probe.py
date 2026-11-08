"""Per-Update output scopes, metadata, failures, detached results and replay."""

RESULT_REPORT = 'update-context-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT
from context_probe import payloads, context_of


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('update-context', server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            worker = fixture.workflows('before')
            for mode in ('update-context-start', 'update-context-finish'):
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=90)
                (ARTIFACTS / ('logs/' + mode + '.log')).write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-5000:]
                report['cases'] += json.loads(result.stdout)
                if mode.endswith('start'):
                    worker.kill(); assert worker.wait(timeout=5) == -9
                    worker = fixture.workflows('after')
            handle = {'workflow_id': fixture.queue + '-update-context'}
            history = fixture.history(handle)
            fixture.assert_clean_history(handle)
            observed = []
            for event in history['events']:
                attributes = event.get('workflowExecutionUpdateCompletedEventAttributes')
                if not attributes: continue
                for payload in payloads(attributes):
                    if 'temporal-v/codec-context' in payload['metadata']:
                        scope = context_of(payload)
                        assert scope['kind'] == 'nexus', scope
                        observed.append(scope['operation'])
            assert set(observed) == {'held', 'immediate', 'fail'}, observed
            file = ARTIFACTS / 'binary-histories/update-context.bin'
            fixture.call('history', handle, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            worker.terminate(); assert worker.wait(timeout=15) == 0
            report['cases'].append('Independent completed-Update payloads used separate Nexus scopes and the entire workflow replayed offline')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

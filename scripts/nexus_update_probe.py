"""Nexus backed by workflow Updates, durable callbacks, failure and restart."""

RESULT_REPORT = 'nexus-update-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('nexus-update', server_binary=ARTIFACTS / 'tools/temporal-protocol',
                             server_args=('--dynamic-config-value', 'history.enableUpdateCallbacks=true')) as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default', '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            worker = fixture.workflows('before')
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-before')
            for mode in ('nexus-update-start', 'nexus-update-finish'):
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=90)
                (ARTIFACTS / ('logs/' + mode + '.log')).write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-5000:]
                report['cases'] += json.loads(result.stdout)
                if mode.endswith('start'):
                    for process in (worker, nexus): process.kill(); assert process.wait(timeout=5) == -9
                    worker = fixture.workflows('after')
                    nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-after')
            handle = {'workflow_id': fixture.queue + '-nexus-update-target'}
            fixture.assert_clean_history(handle)
            file = ARTIFACTS / 'binary-histories/nexus-update-target.bin'
            fixture.call('history', handle, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            for process in (worker, nexus): process.terminate(); assert process.wait(timeout=15) == 0
            report['cases'].append('The backing workflow history replayed offline and both workers drained')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

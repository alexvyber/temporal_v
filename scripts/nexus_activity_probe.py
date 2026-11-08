"""Activity-backed async Nexus, protected callbacks, worker loss and replay."""

RESULT_REPORT = 'nexus-activity-results.json'
import json
import subprocess
import tempfile
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with tempfile.TemporaryDirectory(prefix='v-nexus-activity-') as directory, TemporalFixture('nexus-activity', server_args=('--dynamic-config-value', 'activity.enableCallbacks=true'), server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded', V_SDK_EXAMPLE_ASYNC_TOKEN_DIR=directory)
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default', '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            activity = fixture.activities('activity-before')
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-before')
            for mode in ('nexus-activity-start', 'nexus-activity-finish'):
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=90)
                (ARTIFACTS / ('logs/' + mode + '.log')).write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-5000:]
                report['cases'] += json.loads(result.stdout)
                if mode.endswith('start'):
                    for process in (activity, nexus): process.kill(); assert process.wait(timeout=5) == -9
                    activity = fixture.activities('activity-after')
                    nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-after')
            worker = fixture.workflows('workflow-before')
            caller = fixture.start('caller', 'v.nexus', {'endpoint': fixture.queue + '-endpoint', 'operation': 'activity', 'name': 'workflow caller', 'delay_ms': 20})
            fixture.wait_for(lambda: counts(fixture.history(caller))['timerstarted'] == 1)
            worker.kill(); assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('workflow-after')
            outcome = fixture.call('result', caller)
            assert outcome['status'] == 'completed' and json.loads(outcome['data']) == 'workflow caller', outcome
            fixture.assert_clean_history(caller)
            file = ARTIFACTS / 'binary-histories/nexus-activity-caller.bin'
            fixture.call('history', caller, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', caller['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            for process in (worker, activity, nexus): process.terminate(); assert process.wait(timeout=15) == 0
            report['cases'].append('A workflow caller resumed through restart and offline replay after its Activity-backed Nexus result; all workers drained')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

"""Per-type deployment behavior, worker defaults and replay after process loss."""

RESULT_REPORT = 'workflow-versioning-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT
from worker_options_probe import completed_versions


def main():
    report = {'passed': False, 'cases': []}
    try:
        validation = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'versioning-validation'],
                                    cwd=ROOT, capture_output=True, text=True, timeout=15)
        assert validation.returncode == 0, validation.stderr
        report['cases'] += json.loads(validation.stdout)
        for default in ('pinned', 'auto_upgrade'):
            with TemporalFixture('type-versioning-' + default) as fixture:
                fixture.env.update(TEMPORAL_DEPLOYMENT_NAME=fixture.queue, TEMPORAL_VERSIONING_BEHAVIOR=default,
                                   TEMPORAL_BUILD_ID='build-a', V_SDK_EXAMPLE_VERSIONED_TYPES='true')
                worker_a = fixture.workflows('a')
                def make_current(build):
                    response = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'deployment-current'],
                        cwd=ROOT, env=dict(fixture.env, TEMPORAL_BUILD_ID=build), capture_output=True, text=True, timeout=15)
                    return response.returncode == 0
                fixture.wait_for(lambda: make_current('build-a'))
                handles = {kind: fixture.start(kind, workflow_type, {}) for kind, workflow_type in
                           [('pinned', 'v.versioned.pinned'), ('auto', 'v.versioned.auto'), ('default', 'v.counter')]}
                for handle in handles.values():
                    assert json.loads(fixture.call('query', handle, name='state')['data'])['total'] == 0
                    assert completed_versions(fixture.history(handle))[-1] == 'build-a'
                env_b = dict(fixture.env, TEMPORAL_BUILD_ID='build-b')
                worker_b = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'b', env=env_b)
                fixture.wait_for(lambda: make_current('build-b'))
                for kind, handle in handles.items():
                    fixture.call('signal', handle, name='add', input='{"amount":3}')
                    assert json.loads(fixture.call('query', handle, name='state')['data'])['total'] == 3
                    expected = 'build-a' if kind == 'pinned' or (kind == 'default' and default == 'pinned') else 'build-b'
                    if expected == 'build-b':
                        # Deployment propagation is asynchronous; a sticky task may
                        # still land on A immediately after SetCurrent succeeds.
                        def migrated():
                            fixture.call('signal', handle, name='add', input='{"amount":0}')
                            fixture.call('query', handle, name='state')
                            return completed_versions(fixture.history(handle))[-1] == expected
                        fixture.wait_for(migrated, timeout=15)
                    assert completed_versions(fixture.history(handle))[-1] == expected, (kind, expected)
                    if kind != 'default':
                        info = json.loads(fixture.call('query', handle, name='versioning')['data'])
                        assert info['current_task_deployment_version']['build_id'] == expected, info
                        assert info['current_task_deployment_version']['deployment_name'] == fixture.queue, info
                for worker in (worker_a, worker_b):
                    worker.kill(); assert worker.wait(timeout=5) == -9
                worker_a = fixture.workflows('a-after')
                worker_b = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'b-after', env=env_b)
                for kind, handle in handles.items():
                    assert json.loads(fixture.call('query', handle, name='state')['data'])['total'] == 3
                    fixture.call('signal', handle, name='finish')
                    assert fixture.call('result', handle)['status'] == 'completed'
                    fixture.assert_clean_history(handle)
                    file = ARTIFACTS / f'binary-histories/type-versioning-{default}-{kind}.bin'
                    fixture.call('history', handle, file=file)
                    replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file),
                        '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT,
                        env=fixture.env, capture_output=True, text=True, timeout=40)
                    assert replay.returncode == 0, replay.stderr
                for worker in (worker_a, worker_b):
                    worker.terminate(); assert worker.wait(timeout=15) == 0
                report['cases'].append(default + ' worker default: explicit pinned/auto types routed independently, unspecified inherited default; all replayed after SIGKILL and offline')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

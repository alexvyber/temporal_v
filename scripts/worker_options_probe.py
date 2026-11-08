"""Fixed/resource tuning, uncached replay and deployment-version routing."""

RESULT_REPORT = 'worker-options-results.json'
import json
import re
import subprocess

from integration import TemporalFixture, ARTIFACTS, LOGS, ROOT


def completed_versions(history):
    versions = []
    for event in history['events']:
        attributes = event.get('workflowTaskCompletedEventAttributes')
        if attributes:
            version = attributes.get('deploymentVersion', {}).get('buildId')
            if not version:
                version = attributes.get('workerDeploymentVersion', '').split('.')[-1]
            versions.append(version)
    return versions


def configuration(report):
    for mode in ('uncached', 'resource'):
        with TemporalFixture('worker-' + mode) as fixture:
            fixture.env.update(TEMPORAL_ACTIVITY_CONCURRENCY='1' if mode == 'uncached' else '2',
                               TEMPORAL_LOCAL_ACTIVITY_CONCURRENCY='1', TEMPORAL_NEXUS_CONCURRENCY='1',
                               TEMPORAL_WORKFLOW_CONCURRENCY='1' if mode == 'uncached' else '2',
                               TEMPORAL_MAX_CACHED_WORKFLOWS='0' if mode == 'uncached' else '2',
                               TEMPORAL_RESOURCE_TUNER='false' if mode == 'uncached' else 'true')
            worker = fixture.workflows()
            activities = fixture.activities()
            counter = fixture.start('counter', 'v.counter', {})
            fixture.call('signal', counter, name='add', input='{"amount":5}')
            assert json.loads(fixture.call('query', counter, name='state')['data'])['total'] == 5
            fixture.call('signal', counter, name='finish')
            assert fixture.call('result', counter)['status'] == 'completed'
            runs = [fixture.start(str(index), 'v.parallel', {'name': str(index), 'delay_ms': 50}) for index in range(4)]
            for handle in runs:
                assert fixture.call('result', handle)['status'] == 'completed'
                fixture.assert_clean_history(handle)
            fixture.assert_clean_history(counter)
            for process in (worker, activities):
                process.terminate()
                assert process.wait(timeout=15) == 0
            log = (LOGS / f'worker-{mode}-activity.log').read_text()
            observed = [int(count) for count in re.findall(r'ACTIVITY_START[^\n]+active=(\d+)', log)]
            assert observed and max(observed) <= int(fixture.env['TEMPORAL_ACTIVITY_CONCURRENCY']), observed
            if mode == 'uncached':
                assert 'replay=true' in (LOGS / 'worker-uncached-workflow.log').read_text()
            report['cases'].append(mode + ': workflow state, replay, concurrent work, slot bounds and graceful shutdown')


def deployment(report):
    for behavior in ('pinned', 'auto_upgrade'):
        with TemporalFixture('deployment-' + behavior) as fixture:
            fixture.env.update(TEMPORAL_DEPLOYMENT_NAME=fixture.queue, TEMPORAL_VERSIONING_BEHAVIOR=behavior, TEMPORAL_BUILD_ID='build-a')
            first_worker = fixture.workflows('a')

            def make_current(build):
                environment = dict(fixture.env, TEMPORAL_BUILD_ID=build)
                response = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'deployment-current'],
                                          cwd=ROOT, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
                return response.returncode == 0

            fixture.wait_for(lambda: make_current('build-a'))
            original = fixture.start('original', 'v.counter', {})
            assert json.loads(fixture.call('query', original, name='state')['data'])['total'] == 0
            assert completed_versions(fixture.history(original))[-1] == 'build-a'
            second_environment = dict(fixture.env, TEMPORAL_BUILD_ID='build-b')
            second_worker = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'b', env=second_environment)
            fixture.wait_for(lambda: make_current('build-b'))
            newer = fixture.start('newer', 'v.counter', {})
            assert json.loads(fixture.call('query', newer, name='state')['data'])['total'] == 0
            fixture.call('signal', original, name='add', input='{"amount":8}')
            assert json.loads(fixture.call('query', original, name='state')['data'])['total'] == 8
            fixture.call('signal', original, name='finish')
            fixture.call('signal', newer, name='finish')
            fixture.call('result', original)
            fixture.call('result', newer)
            old_versions = completed_versions(fixture.history(original))
            new_versions = completed_versions(fixture.history(newer))
            assert old_versions[0] == 'build-a' and new_versions[-1] == 'build-b', (old_versions, new_versions)
            assert old_versions[-1] == ('build-a' if behavior == 'pinned' else 'build-b'), old_versions
            fixture.assert_clean_history(original)
            fixture.assert_clean_history(newer)
            for process in (first_worker, second_worker):
                process.terminate()
                assert process.wait(timeout=15) == 0
            report['cases'].append(behavior + ': switching current deployment routed new runs to B and preserved/migrated existing run as configured')


def main():
    report = {'passed': False, 'cases': []}
    try:
        configuration(report)
        deployment(report)
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

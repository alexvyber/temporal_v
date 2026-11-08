"""Typed deployment routing and canonical execution overrides across restart/replay."""
RESULT_REPORT = 'deployment-results.json'
import argparse
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS, ROOT, TemporalFixture
from worker_options_probe import completed_versions

BINARY = ARTIFACTS / 'tools/v-deployment'


def invoke(fixture, mode, *arguments, success=True):
    result = subprocess.run([str(BINARY), mode, *map(str, arguments)], cwd=ROOT, env=fixture.env,
                            capture_output=True, text=True, timeout=25)
    (ARTIFACTS / f'logs/{fixture.name}-{mode}.log').write_text(result.stdout + result.stderr)
    if not success: return result
    assert result.returncode == 0, result.stderr
    if result.stdout.strip() == 'VALIDATION_OK': return 'VALIDATION_OK'
    return json.loads(result.stdout)


def route(fixture, handle, build):
    def arrived():
        fixture.call('signal', handle, name='add', input='{"amount":0}')
        fixture.call('query', handle, name='state')
        return completed_versions(fixture.history(handle))[-1] == build
    fixture.wait_for(arrived, timeout=20)


def exercise(label, server_binary):
    replays = []
    with TemporalFixture('deployment-' + label, server_binary=server_binary) as fixture:
        fixture.env.update(TEMPORAL_DEPLOYMENT_NAME=fixture.queue, TEMPORAL_BUILD_ID='build-a', V_SDK_EXAMPLE_VERSIONED_TYPES='true')
        assert invoke(fixture, 'validation') == 'VALIDATION_OK'
        worker_a = fixture.workflows('a')
        worker_b = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'b', env=dict(fixture.env, TEMPORAL_BUILD_ID='build-b'))
        fixture.wait_for(lambda: invoke(fixture, 'current', 'build-a', success=False).returncode == 0)
        original = invoke(fixture, 'describe')
        token = invoke(fixture, 'ramp', 'build-b', 100)
        assert token and token != original['conflict_token']
        stale = invoke(fixture, 'ramp', 'build-b', 50, json.dumps(original['conflict_token']), success=False)
        assert stale.returncode != 0 and 'code: 9' in stale.stderr, stale.stderr
        routing = invoke(fixture, 'describe')['worker_deployment_info']['routing_config']
        assert routing['ramping_version_percentage'] == 100 and routing['ramping_deployment_version']['build_id'] == 'build-b', routing
        handle = invoke(fixture, 'start', fixture.queue + '-override', 'v.versioned.auto', 'pinned', 'build-a')
        route(fixture, handle, 'build-a')
        options = invoke(fixture, 'override', json.dumps(handle), 'auto', '')
        assert options['priority']['priority_key'] == 2 and options['versioning_override']['override']['value'] is True, options
        route(fixture, handle, 'build-b')
        options = invoke(fixture, 'clear', json.dumps(handle))
        assert 'versioning_override' not in options and options['priority']['priority_key'] == 2, options
        invoke(fixture, 'ramp', 'build-b', 0)
        route(fixture, handle, 'build-a')
        invoke(fixture, 'ramp', 'build-b', 100)
        route(fixture, handle, 'build-b')
        prior = max(int(event['eventId']) for event in fixture.history(handle)['events'])
        invoke(fixture, 'override', json.dumps(handle), 'once', 'build-a')
        fixture.call('signal', handle, name='add', input='{"amount":0}')
        def moved_once():
            history = fixture.history(handle)
            events = [event for event in history['events'] if int(event['eventId']) > prior]
            return 'build-a' in completed_versions({'events': events})
        fixture.wait_for(moved_once, timeout=20)
        route(fixture, handle, 'build-b')
        invoke(fixture, 'override', json.dumps(handle), 'pinned', 'build-a')
        route(fixture, handle, 'build-a')
        missing = invoke(fixture, 'override', json.dumps(handle), 'pinned', 'missing-build', success=False)
        assert missing.returncode != 0 and 'code: 9' in missing.stderr, missing.stderr
        route(fixture, handle, 'build-a')
        controls = [fixture.launch(BINARY, 'control-' + build, ('worker',),
                                  env=dict(fixture.env, TEMPORAL_TASK_QUEUE=fixture.queue + '-control', TEMPORAL_BUILD_ID=build))
                    for build in ('build-a', 'build-b')]
        for build in ('build-a', 'build-b'):
            fixture.wait_for(lambda: any(item['name'] == fixture.queue + '-control' for item in
                                        invoke(fixture, 'describe-version', build)['worker_deployment_version_info']['task_queue_infos']))
        def export_history(execution, name, task_queue, custom):
            fixture.assert_clean_history(execution)
            path = ARTIFACTS / f'binary-histories/deployment-{label}-{name}.bin'
            fixture.call('history', execution, file=path)
            replays.append((path, execution['workflow_id'], task_queue, custom))
        for mode in ('child', 'continue'):
            parent = invoke(fixture, 'start-control', fixture.queue + '-' + mode, 'v.version-control', 'pinned', 'build-a',
                            json.dumps({'mode': mode, 'deployment_name': fixture.queue}))
            result = fixture.call('result', parent)
            assert result['status'] == ('completed' if mode == 'child' else 'continued_as_new'), result
            history = fixture.history(parent)
            assert set(completed_versions(history)) == {'build-a'}, history
            if mode == 'child':
                child = {'workflow_id': parent['workflow_id'] + '-child'}
                assert set(completed_versions(fixture.history(child))) == {'build-b'}
                export_history(child, 'child', fixture.queue + '-control', True)
            else:
                final = {'workflow_id': parent['workflow_id'], 'run_id': result['next_run_id']}
                assert fixture.call('result', final)['status'] == 'completed'
                assert set(completed_versions(fixture.history(final))) == {'build-a'}
                detail = invoke(fixture, 'describe-workflow', json.dumps(final))
                inherited = detail['workflow_execution_info']['versioning_info']['versioning_override']
                assert inherited['override']['value']['version']['build_id'] == 'build-a', inherited
                export_history(final, 'continued', fixture.queue + '-control', True)
            export_history(parent, 'parent-' + mode, fixture.queue + '-control', True)
        for control in controls: control.terminate(); assert control.wait(timeout=20) == 0
        for worker in (worker_a, worker_b): worker.kill(); assert worker.wait(timeout=10) == -9
        worker_a = fixture.workflows('a-after')
        worker_b = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'b-after', env=dict(fixture.env, TEMPORAL_BUILD_ID='build-b'))
        route(fixture, handle, 'build-a')
        fixture.call('signal', handle, name='finish')
        assert fixture.call('result', handle)['status'] == 'completed'
        export_history(handle, 'override', fixture.queue, False)
        for worker in (worker_a, worker_b): worker.terminate(); assert worker.wait(timeout=20) == 0
        replay_env = fixture.env.copy()
    for history, workflow_id, queue, custom in replays:
        args = [str(BINARY), 'replay', str(history), workflow_id, queue] if custom else [
            str(ARTIFACTS / 'tools/v-replay'), '--history', str(history), '--workflow-id', workflow_id, '--task-queue', queue]
        replay = subprocess.run(args, cwd=ROOT, env=replay_env, capture_output=True, text=True, timeout=40)
        (ARTIFACTS / f'logs/deployment-{label}-replay-{history.stem}.log').write_text(replay.stdout + replay.stderr)
        assert replay.returncode == 0, replay.stderr


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        for label, server in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
            exercise(label, server)
            report['cases'].append(label + ': invalid typed overrides/ramps failed before RPC; stale conflict tokens and unknown target versions were rejected; 0/100 ramps, pinned/auto/one-time overrides and clear respected priority, survived SIGKILL/replacement and replayed offline')
            report['cases'].append(label + ': a pinned A parent explicitly dispatched its child to B; continue-as-new inherited its A override despite a 100% B ramp; both parent/child and continued histories replayed offline')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'deployment-sanitized-results.json'), default=RESULT_REPORT)
    args = parser.parse_args()
    main(args.binary, args.report)

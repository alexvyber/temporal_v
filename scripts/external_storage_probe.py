"""Large payload references, storage outages, concurrency and durable replay."""

RESULT_REPORT = 'external-storage-results.json'
import base64
from collections import defaultdict
import json
import os
from pathlib import Path
import subprocess
import sys
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI, counts

BINARY = ARTIFACTS / 'tools/v-storage'
REFERENCE_TYPE = base64.b64encode(b'temporal.api.sdk.v1.ExternalStorageReference').decode()


def main():
    report = {'passed': False, 'cases': []}
    try:
        native = subprocess.run([sys.executable, str(ROOT / 'scripts/build_payload_bridge.py'), '--test'], cwd=ROOT, capture_output=True, text=True, timeout=300)
        (ARTIFACTS / 'logs/payload-bridge-tests.log').write_text(native.stdout + native.stderr)
        assert native.returncode == 0, native.stderr
        report['cases'].append('Native traversal preserved map associations across 128 decodes, nested failures and search attributes; corrupt layouts and FFI ownership checks passed')
        for local in (False, True):
            label = 'local' if local else 'remote'
            with TemporalFixture('external-storage-' + label) as fixture:
                directory = ARTIFACTS / 'external-storage-objects' / fixture.queue
                directory.mkdir(parents=True)
                os.chmod(directory, 0o700)
                fixture.env.update(V_SDK_EXAMPLE_STORAGE_DIR=str(directory), V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded', V_SDK_EXAMPLE_STORAGE_LOCAL=str(local).lower())

                def call(role, *args, success=True, env=None):
                    result = subprocess.run([str(BINARY), role, *map(str, args)], env=env or fixture.env, cwd=ROOT, capture_output=True, text=True, timeout=60)
                    if success:
                        assert result.returncode == 0, (role, result.stdout, result.stderr)
                    else:
                        assert result.returncode != 0, (role, result.stdout)
                    assert 'private-claim-credentials' not in result.stdout + result.stderr
                    return result

                # Failed uploads never send a partial client request.
                (directory / 'fail-store').touch()
                call('start', success=False)
                absent = subprocess.run([str(CLI), 'workflow', 'show', '--address', fixture.address, '--workflow-id', fixture.queue, '--output', 'json'], capture_output=True, text=True, timeout=15)
                assert absent.returncode != 0
                (directory / 'fail-store').unlink()
                handle = json.loads(call('start').stdout)
                worker = fixture.launch(BINARY, 'before', args=['worker'])
                fixture.wait_for(lambda: counts(fixture.history(handle))['timerstarted'] == 1, timeout=60)
                history = fixture.history(handle)
                encoded = json.dumps(history)
                assert REFERENCE_TYPE in encoded
                assert len(encoded) < 100000, len(encoded)
                # Verify the independent reference, not merely a decoded SDK view.
                input_payload = history['events'][0]['workflowExecutionStartedEventAttributes']['input']['payloads'][0]
                reference = json.loads(base64.b64decode(input_payload['data']))
                assert reference['driverName'] == 'fixture.files'
                assert reference['claimData']['key']
                assert int(input_payload['externalPayloads'][0]['sizeBytes']) > 3 * 1024 * 1024
                call('query'); call('query-failure'); call('update'); call('update-failure')
                failed_before = counts(fixture.history(handle))['workflowtaskfailed']
                (directory / 'fail-store').touch()
                call('query', success=False)
                assert counts(fixture.history(handle))['workflowtaskfailed'] == failed_before
                (directory / 'fail-store').unlink()
                call('query')
                worker.kill(); worker.wait(timeout=10)
                before_replacement = len((directory / 'stores.log').read_text().splitlines())
                replacement = fixture.launch(BINARY, 'after', args=['worker'])
                call('query')
                assert len((directory / 'stores.log').read_text().splitlines()) == before_replacement + 1, 'replacement replay uploaded recorded commands/results'
                # A single client owns one shared two-permit limiter.
                call('concurrency', env=dict(fixture.env, V_SDK_EXAMPLE_STORAGE_DELAY_MS='200'))
                call('query')
                call('signal')
                fixture.assert_clean_history(handle)
                file = ARTIFACTS / f'binary-histories/external-storage-{label}.bin'
                call('history', file)
                assert file.stat().st_size < 100000
                stores = (directory / 'stores.log').read_bytes()
                call('replay', file)
                assert stores == (directory / 'stores.log').read_bytes(), 'offline replay uploaded payloads'
                active = defaultdict(int)
                peak = defaultdict(int)
                for line in (directory / 'operations.log').read_text().splitlines():
                    operation = json.loads(line)
                    pid = operation['pid']
                    active[pid] += 1 if operation['action'] == 'store-start' else -1
                    assert 0 <= active[pid] <= 2, (operation, dict(active))
                    peak[pid] = max(peak[pid], active[pid])
                assert all(value == 0 for value in active.values())
                assert 2 in peak.values(), dict(peak)
                replacement.terminate(); assert replacement.wait(timeout=20) == 0
                report['cases'].append(f'3 MiB {label} Activity/workflow payloads, queries, protected query/Update failures, concurrent signals and worker restart round-tripped through references; offline replay made zero uploads')
        exercise_targets(report)
        exercise_outages(report)
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


def exercise_targets(report):
    for kind in ('child', 'continue', 'signal'):
        with TemporalFixture('external-storage-target-' + kind) as fixture:
            directory = ARTIFACTS / 'external-storage-objects' / fixture.queue
            directory.mkdir(parents=True)
            fixture.env.update(V_SDK_EXAMPLE_STORAGE_DIR=str(directory), V_SDK_EXAMPLE_STORAGE_TARGETS=kind, V_SDK_EXAMPLE_CODEC='context')
            worker = fixture.launch(BINARY, 'worker', args=['worker'])
            result = subprocess.run([str(BINARY), 'targets'], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=60)
            assert result.returncode == 0, (kind, result.stdout, result.stderr)
            handle = json.loads(result.stdout)
            fixture.assert_clean_history(handle)
            operations = [json.loads(line) for line in (directory / 'operations.log').read_text().splitlines()]
            stores = [item for item in operations if item['action'] == 'store-start']
            if kind == 'child':
                child = [item for item in stores if any('StartChildWorkflowExecution.input' in field for field in item['paths'])]
                assert child and all(item['workflow_id'] == fixture.queue + '-child' and item['workflow_type'] == 'storage.child' and item['run_id'] == '' for item in child), stores
                results = [item for item in stores if any('CompleteWorkflowExecution.result' in field for field in item['paths'])]
                assert len(results) == 2 and all(item['workflow_id'] == fixture.queue and item['run_id'] == handle['run_id'] for item in results), stores
            elif kind == 'continue':
                continued = [item for item in stores if any('ContinueAsNewWorkflowExecution.arguments' in field for field in item['paths'])]
                assert continued and all(item['workflow_id'] == fixture.queue and item['workflow_type'] == 'storage.continue' and item['run_id'] == '' for item in continued), stores
            else:
                signals = [item for item in stores if any('SignalExternalWorkflowExecution.args' in field for field in item['paths'])]
                assert signals and all(item['workflow_id'] == fixture.queue + '-receiver' and item['run_id'] == '' for item in signals), stores
            file = ARTIFACTS / f'binary-histories/external-storage-target-{kind}.bin'
            export = subprocess.run([str(BINARY), 'history', str(file), handle['run_id']], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=30)
            assert export.returncode == 0, export.stderr
            before = (directory / 'stores.log').read_bytes()
            replay = subprocess.run([str(BINARY), 'replay', str(file)], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            assert before == (directory / 'stores.log').read_bytes()
            worker.terminate(); assert worker.wait(timeout=20) == 0
    report['cases'].append('Child starts/results, continue-as-new and external signals used the receiving execution as storage owner; each original run replayed without uploads')


def exercise_outages(report):
    with TemporalFixture('external-storage-outages') as fixture:
        directory = ARTIFACTS / 'external-storage-objects' / fixture.queue
        directory.mkdir(parents=True)
        fixture.env.update(V_SDK_EXAMPLE_STORAGE_DIR=str(directory), V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')

        def call(role, *args):
            result = subprocess.run([str(BINARY), role, *map(str, args)], env=fixture.env, cwd=ROOT, capture_output=True, text=True, timeout=70)
            assert result.returncode == 0, (role, result.stdout, result.stderr)
            return result

        def failed_tasks(handle):
            return [event['workflowTaskFailedEventAttributes'] for event in fixture.history(handle)['events'] if 'workflowTaskFailedEventAttributes' in event]

        handle = json.loads(call('start').stdout)
        missing = fixture.launch(BINARY, 'missing-driver', args=['worker'], env=dict(fixture.env, V_SDK_EXAMPLE_STORAGE_DIR=''))
        fixture.wait_for(lambda: len(failed_tasks(handle)) >= 1, timeout=40)
        assert failed_tasks(handle)[0]['cause'] == 'WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE'
        missing.terminate(); assert missing.wait(timeout=20) == 0
        (directory / 'fail-retrieve').touch()
        (directory / 'fail-activity-store').touch()
        worker = fixture.launch(BINARY, 'recovering', args=['worker'])
        # Temporal compresses consecutive failed task attempts in history.
        # Observe the new worker's specific driver failure instead of requiring
        # a second persisted WorkflowTaskFailed event.
        fixture.wait_for(lambda: 'EXTERNAL_STORAGE_RETRIEVE_FAILED message=TMPRL1105 external storage retrieve: driver failed' in (ARTIFACTS / 'logs/external-storage-outages-recovering.log').read_text(), timeout=50)
        (directory / 'fail-retrieve').unlink()

        def activity_retry():
            result = subprocess.run([str(CLI), 'workflow', 'describe', '--address', fixture.address, '--workflow-id', handle['workflow_id'], '--output', 'json'], capture_output=True, text=True, timeout=15)
            assert result.returncode == 0, result.stderr
            description = json.loads(result.stdout)
            for pending in description.get('pendingActivities', []):
                failure = pending.get('lastFailure', {})
                if 'TMPRL1105' in failure.get('message', ''):
                    assert not failure.get('applicationFailureInfo', {}).get('nonRetryable', False), failure
                    (fixture.histories / 'activity-storage-retry.json').write_text(json.dumps(description, indent=2))
                    return True
            return False

        fixture.wait_for(activity_retry, timeout=60)
        (directory / 'fail-activity-store').unlink()
        fixture.wait_for(lambda: counts(fixture.history(handle))['timerstarted'] == 1, timeout=60)
        call('query')
        before_finish = len(failed_tasks(handle))
        (directory / 'fail-workflow-store').touch()
        finisher = fixture.launch(BINARY, 'finish-after-storage-repair', args=['signal'])
        fixture.wait_for(lambda: len(failed_tasks(handle)) > before_finish, timeout=30)
        assert counts(fixture.history(handle))['workflowexecutioncompleted'] == 0
        (directory / 'fail-workflow-store').unlink()
        assert finisher.wait(timeout=40) == 0
        assert counts(fixture.history(handle))['workflowexecutioncompleted'] == 1
        assert all(task['cause'] == 'WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE' for task in failed_tasks(handle))
        file = ARTIFACTS / 'binary-histories/external-storage-outages.bin'
        call('history', file)
        stores = (directory / 'stores.log').read_bytes()
        call('replay', file)
        assert stores == (directory / 'stores.log').read_bytes()
        worker.terminate(); assert worker.wait(timeout=20) == 0
        logs = ''.join(path.read_text() for path in (ARTIFACTS / 'logs').glob('external-storage-outages-*.log'))
        assert 'private-claim-credentials' not in logs
        report['cases'].append('Missing drivers and retrieval/upload outages used the external-storage WFT cause, remote Activity upload failures stayed retryable, repaired workers completed once and the failure history replayed without uploads')


if __name__ == '__main__':
    main()

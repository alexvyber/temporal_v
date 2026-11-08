"""Batch lifecycle and converted payloads against isolated Temporal servers."""
RESULT_REPORT = 'batch-results.json'
import argparse
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS, ROOT, TemporalFixture

BINARY = ARTIFACTS / 'tools/v-batch'


def invoke(fixture, mode, *arguments, success=True):
    process = subprocess.run([str(BINARY), mode, *map(str, arguments)], cwd=ROOT, env=fixture.env,
                             capture_output=True, text=True, timeout=70)
    (ARTIFACTS / f'logs/{fixture.name}-{mode}.log').write_text(process.stdout + process.stderr)
    if not success: return process
    assert process.returncode == 0, process.stderr
    output = process.stdout.strip()
    return output if output in ('OK', 'VALIDATION_OK') else json.loads(output)


def exercise(label, server, codec):
    replays = []
    with TemporalFixture(f'batch-{label}-{codec}', server_binary=server,
                         server_args=('--dynamic-config-value', 'frontend.enableBatcher=true',
                                      '--dynamic-config-value', 'worker.batcherRPS=1')) as fixture:
        if codec: fixture.env['V_SDK_EXAMPLE_CODEC'] = 'prefix'
        assert invoke(fixture, 'validation') == 'VALIDATION_OK'
        worker = fixture.launch(BINARY, 'worker', ('worker',))
        handles = [fixture.start(str(index), 'v.batch', {'expect_headers': label == '1-33'}) for index in range(6)]
        for handle in handles:
            assert json.loads(fixture.call('query', handle, name='state')['data']) == 0
        batch = invoke(fixture, 'signal', json.dumps(handles))
        result = invoke(fixture, 'wait', batch['job_id'])
        assert result['state'] == 'batch_operation_state_completed' and result['complete_operation_count'] == 6, result
        assert result['failure_operation_count'] == 0 and result['reason'] == 'disposable V SDK batch test'
        for handle in handles:
            assert json.loads(fixture.call('query', handle, name='state')['data']) == 3
        for handle in handles:
            signaled = [event['workflowExecutionSignaledEventAttributes'] for event in fixture.history(handle)['events']
                        if 'workflowExecutionSignaledEventAttributes' in event]
            assert len(signaled) == 1
            assert bool(signaled[0].get('header', {}).get('fields')) == (label == '1-33'), signaled
        duplicate = invoke(fixture, 'signal', json.dumps(handles), batch['job_id'], success=False)
        assert duplicate.returncode != 0 and 'code: 6' in duplicate.stderr, duplicate.stderr
        worker.kill()
        assert worker.wait(timeout=10) == -9
        worker = fixture.launch(BINARY, 'replacement', ('worker',))
        for handle in handles:
            assert json.loads(fixture.call('query', handle, name='state')['data']) == 3
        cancel = invoke(fixture, 'cancel', json.dumps(handles[:3]))
        assert invoke(fixture, 'wait', cancel['job_id'])['complete_operation_count'] == 3
        terminate = invoke(fixture, 'terminate', json.dumps(handles[3:]))
        assert invoke(fixture, 'wait', terminate['job_id'])['complete_operation_count'] == 3

        def export(execution, name):
            fixture.assert_clean_history(execution)
            path = ARTIFACTS / f'binary-histories/{fixture.name}-{name}.bin'
            fixture.call('history', execution, file=path)
            replays.append((path, execution['workflow_id'], fixture.queue, fixture.env.copy()))

        for index, handle in enumerate(handles):
            outcome = invoke(fixture, 'result', json.dumps(handle))
            assert outcome['status'] == ('canceled' if index < 3 else 'terminated'), outcome
            if index >= 3:
                assert outcome['details'] == (['"batch-detail"', '7'] if label == '1-33' else []), outcome
                terminated = [event['workflowExecutionTerminatedEventAttributes'] for event in fixture.history(handle)['events']
                              if 'workflowExecutionTerminatedEventAttributes' in event]
                assert bool(terminated[0].get('details', {}).get('payloads')) == (label == '1-33'), terminated
            export(handle, str(index))
        query_handles = [fixture.start('query-' + str(index), 'v.batch', {'expect_headers': label == '1-33'}) for index in range(3)]
        query = 'WorkflowId IN (' + ','.join("'" + handle['workflow_id'] + "'" for handle in query_handles) + ')'
        fixture.wait_for(lambda: invoke(fixture, 'query-count', query) == 3)
        queried = invoke(fixture, 'signal-query', query)
        result = invoke(fixture, 'wait', queried['job_id'])
        assert result['complete_operation_count'] == 3 and result['query'] == query, result
        for index, handle in enumerate(query_handles):
            assert json.loads(fixture.call('query', handle, name='state')['data']) == 3
            fixture.call('signal', handle, name='finish')
            outcome = invoke(fixture, 'result', json.dumps(handle))
            assert outcome['status'] == 'completed' and outcome['data'] == '3', outcome
            export(handle, 'query-' + str(index))
        missing_wait = invoke(fixture, 'wait', 'missing-wait-' + fixture.queue, success=False)
        assert missing_wait.returncode != 0 and 'code: 5' in missing_wait.stderr, missing_wait.stderr
        slow_handles = [fixture.start('slow-' + str(index), 'v.batch', {'expect_headers': label == '1-33'}) for index in range(5)]
        slow = invoke(fixture, 'slow', json.dumps(slow_handles))
        for deadline_attempt in range(10):
            deadline = invoke(fixture, 'short-wait', slow['job_id'], success=False)
            assert deadline.returncode != 0 and 'code: 4' in deadline.stderr, (deadline_attempt, deadline.stderr)
        assert invoke(fixture, 'describe', slow['job_id'])['state'] == 'batch_operation_state_running'
        invoke(fixture, 'stop', slow['job_id'])
        assert invoke(fixture, 'wait', slow['job_id'])['state'] == 'batch_operation_state_failed'
        for handle in slow_handles:
            fixture.call('cancel', handle)
            assert invoke(fixture, 'result', json.dumps(handle))['status'] == 'canceled'
        wanted = {item['job_id'] for item in (batch, cancel, terminate, queried, slow)}
        found = set()
        token = []

        def listed():
            nonlocal token
            page = invoke(fixture, 'list', json.dumps(token))
            found.update(item['job_id'] for item in page.get('operation_info', []))
            token = page.get('next_page_token', [])
            return wanted <= found

        fixture.wait_for(listed)
        missing = invoke(fixture, 'describe', fixture.queue + '-absent', success=False)
        assert missing.returncode != 0 and 'code: 5' in missing.stderr, missing.stderr
        worker.terminate()
        assert worker.wait(timeout=20) == 0
    for path, workflow_id, queue, environment in replays:
        replay = subprocess.run([str(BINARY), 'replay', str(path), workflow_id, queue], cwd=ROOT,
                                env=environment, capture_output=True, text=True, timeout=40)
        (ARTIFACTS / f'logs/{path.stem}-offline.log').write_text(replay.stdout + replay.stderr)
        assert replay.returncode == 0, replay.stderr


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        for label, server in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
            for codec in (False, True):
                exercise(label, server, codec)
                report['cases'].append(f'{label} codec={codec}: invalid inputs failed before RPC; explicit/query batches signaled exact targets; independent history verified headers on 1.33 and their server-side omission on 1.32; duplicate IDs, cancellation/termination details, pagination, missing jobs, bounded wait without stopping the job under a server-enforced one-operation/second test cap and explicit stop passed; SIGKILL/replacement and nine histories replayed offline')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'batch-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)

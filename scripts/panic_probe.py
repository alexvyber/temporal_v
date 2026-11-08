"""Application panics fail the affected task or call and preserve worker liveness."""
RESULT_REPORT = 'panic-results.json'
from panic_stack_assertions import assert_panic_histories
import argparse
import concurrent.futures
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, counts


def main(binary, report_name):
    report = {'passed': False, 'cases': [], 'histories': 0}
    try:
        for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]:
            for codec in ['default', 'context']:
                with TemporalFixture('panic-' + version + '-' + codec + '-' + binary.name, server_binary=ARTIFACTS / 'tools' / server) as fixture:
                    fixture.env['V_SDK_EXAMPLE_CODEC'] = '' if codec == 'default' else codec
                    case_files = fixture.histories / fixture.queue; case_files.mkdir()
                    defer_log = case_files / 'defers.log'
                    defer_log.write_text('')
                    fixture.env['V_PANIC_DEFER_LOG'] = str(defer_log)
                    worker = fixture.launch(binary, 'before', ('worker',))
                    healthy = fixture.start('healthy', 'v.panic', {'mode': 'wait'})
                    fixture.wait_for(lambda: fixture.call('query', healthy, name='state')['data'] == '0')
                    query = fixture.call('query', healthy, name='panic', success=False)
                    assert 'query panic' in json.dumps(query), query
                    assert fixture.call('query', healthy, name='state')['data'] == '0'
                    response_check=subprocess.run([str(binary),'panic-responses',json.dumps(healthy)],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=20)
                    assert response_check.returncode==0 and response_check.stdout.strip()=='PANIC_RESPONSES_OK',response_check.stdout+response_check.stderr
                    rejected = fixture.call('update', healthy, name='validator-panic', update_id='rejected')
                    assert rejected['status'] == 'failed' and 'validator panic' in rejected['message'], rejected
                    assert fixture.call('query', healthy, name='state')['data'] == '0'
                    activity_handles = []
                    for mode in ['activity-retry', 'local-retry', 'activity-fail', 'local-fail']:
                        activity_handles += [fixture.start(mode + '-' + str(index), 'v.panic', {'mode': mode}) for index in range(4)]
                    def check_activity(item):
                        index, handle = item
                        result = fixture.call('result', handle)
                        if index < 8:
                            assert result['status'] == 'completed' and result['data'] == '"activity-recovered"', result
                        else:
                            assert result['status'] == 'failed' and result['message'] == 'Activity panic', result
                        return result
                    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                        list(executor.map(check_activity, enumerate(activity_handles)))
                    assert worker.poll() is None
                    assert len(defer_log.read_text().splitlines()) == 32
                    assert_panic_histories(fixture, [(handle,'panic_activity') for handle in activity_handles[8:]])
                    blocked = fixture.start('start', 'v.panic', {'mode': 'start'})
                    command = fixture.start('command', 'v.panic', {'mode': 'command'})
                    signaled = fixture.start('signal', 'v.panic', {'mode': 'wait'})
                    fixture.call('signal', signaled, name='panic')
                    updating = fixture.start('updating', 'v.panic', {'mode': 'wait'})
                    pending = fixture.call('update', updating, name='handler-panic', update_id='handler', wait_ms=200, success=False)
                    assert pending
                    for handle in [blocked, signaled, updating, command]:
                        fixture.wait_for(lambda: counts(fixture.history(handle))['workflowtaskfailed'] >= 1)
                        assert counts(fixture.history(handle))['workflowexecutionfailed'] == 0
                    assert_panic_histories(fixture, [(blocked,'PanicWorkflow__on_event'),(signaled,'PanicWorkflow__on_event'),(updating,'PanicWorkflow__on_event'),(command,'PanicCommandInterceptor__intercept')])
                    fixture.call('signal', healthy, name='finish')
                    assert fixture.call('result', healthy)['status'] == 'completed'
                    assert worker.poll() is None
                    worker.kill(); worker.wait(timeout=10)
                    fixture.env['V_PANIC_FIXED'] = 'true'
                    worker = fixture.launch(binary, 'after', ('worker',))
                    assert fixture.call('result', blocked)['data'] == '"repaired"'
                    assert fixture.call('result', command)['data'] == '"command-recovered"'
                    fixture.wait_for(lambda: fixture.call('query', signaled, name='state')['data'] == '1')
                    fixture.wait_for(lambda: fixture.call('query', updating, name='state')['data'] == '1')
                    for handle in [signaled, updating]:
                        fixture.call('signal', handle, name='finish')
                        assert fixture.call('result', handle)['data'] == '1'
                    handles = [healthy, blocked, signaled, updating, command, *activity_handles]
                    for index, handle in enumerate(handles):
                        history = case_files / (str(index) + '.bin')
                        fixture.call('history', handle, file=history)
                        replay = subprocess.run([str(binary), 'replay', str(history), handle['workflow_id'], fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                        assert replay.returncode == 0 and replay.stdout.strip() == 'OK', replay.stdout + replay.stderr
                        report['histories'] += 1
                    worker.terminate(); assert worker.wait(timeout=20) == 0
                    fixture.env.update(V_PANIC_FIXED='false', V_PANIC_FACTORY='true')
                    factory_worker = fixture.launch(binary, 'factory-before', ('worker',))
                    factory = fixture.start('factory', 'v.panic', {'mode': 'factory'})
                    fixture.wait_for(lambda: counts(fixture.history(factory))['workflowtaskfailed'] >= 1)
                    assert factory_worker.poll() is None
                    assert_panic_histories(fixture, [(factory,'panic_factory')])
                    factory_worker.kill(); factory_worker.wait(timeout=10)
                    fixture.env['V_PANIC_FIXED'] = 'true'
                    factory_worker = fixture.launch(binary, 'factory-after', ('worker',))
                    assert fixture.call('result', factory)['data'] == '"repaired"'
                    history = case_files / 'factory.bin'; fixture.call('history', factory, file=history)
                    replay = subprocess.run([str(binary), 'replay', str(history), factory['workflow_id'], fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                    assert replay.returncode == 0, replay.stdout + replay.stderr
                    report['histories'] += 1
                    factory_worker.terminate(); assert factory_worker.wait(timeout=20) == 0
                    fixture.env.update(V_PANIC_FIXED='false', V_PANIC_FACTORY='false', V_PANIC_FAIL_WORKFLOW='true')
                    fail_worker = fixture.launch(binary, 'fail-policy', ('worker',))
                    for label in ['start', 'signal']:
                        failed = fixture.start('fail-' + label, 'v.panic', {'mode': 'start' if label == 'start' else 'wait'})
                        if label == 'signal': fixture.call('signal', failed, name='panic')
                        result = fixture.call('result', failed)
                        assert result['status'] == 'failed' and result['message'] == label + ' panic', result
                        history_value = fixture.history(failed)
                        assert counts(history_value)['workflowtaskfailed'] == 0
                        assert_panic_histories(fixture, [(failed,'PanicWorkflow__on_event')])
                        attributes = next(event['workflowExecutionFailedEventAttributes'] for event in history_value['events'] if 'workflowExecutionFailedEventAttributes' in event)
                        assert attributes['failure']['applicationFailureInfo']['type'] == 'PanicError', attributes
                        history = case_files / ('fail-' + label + '.bin'); fixture.call('history', failed, file=history)
                        replay = subprocess.run([str(binary), 'replay', str(history), failed['workflow_id'], fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                        assert replay.returncode == 0, replay.stdout + replay.stderr
                        report['histories'] += 1
                    fail_worker.terminate(); assert fail_worker.wait(timeout=20) == 0
                    report['cases'].append(f'{version}/{codec}: query/validator isolation, 16 concurrent local/remote Activity panics with retries and deferred cleanup, start/signal/Update task failures, unaffected workflow, SIGKILL/repaired replay factory panic repair, fail-workflow policy and clean drain')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=ARTIFACTS / 'tools/v-panic')
    parser.add_argument('--report', default=RESULT_REPORT)
    args = parser.parse_args()
    main(args.binary, args.report)

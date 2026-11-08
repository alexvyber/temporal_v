"""Use only the V client for operations; CLI is an independent history observer."""

RESULT_REPORT = 'client-results.json'
from contextlib import ExitStack
import json
import os
import socket
import subprocess
import time
import uuid

from smoke import ARTIFACTS, CLI, LOGS, ROOT, free_port, stop
from restart_probe import WORKFLOW_WORKER, counts, event_type

CLIENT = ARTIFACTS / 'tools/v-client'


def main():
    LOGS.mkdir(parents=True, exist_ok=True)
    address = f'127.0.0.1:{free_port()}'
    queue = f'v-client-{uuid.uuid4().hex}'
    histories = ARTIFACTS / 'client-histories'
    histories.mkdir(exist_ok=True)
    processes = []
    report = {'passed': False, 'cases': [], 'rpc_errors': {}}
    with ExitStack() as files:
        def launch(binary, label, env=None, extra=()):
            output = files.enter_context((LOGS / f'client-{label}.log').open('w'))
            process = subprocess.Popen([str(binary), *extra], cwd=ROOT, env=env,
                                       stdout=output, stderr=subprocess.STDOUT)
            processes.append(process)
            return process

        def client(command, handle=None, success=True, **options):
            args = [str(CLIENT), '--address', f'http://{address}', '--task-queue', queue]
            if handle:
                args += ['--workflow-id', handle['workflow_id'], '--run-id', handle['run_id']]
            for key, value in options.items():
                args += ['--' + key.replace('_', '-'), str(value)]
            response = subprocess.run(args + [command], text=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, timeout=40)
            if success:
                assert response.returncode == 0, response.stderr
                return json.loads(response.stdout)
            assert response.returncode != 0, response.stdout
            error = json.loads(response.stderr)
            assert error['message']
            return error

        def start(label, workflow_type='v.counter', data=None):
            return client('start', workflow_id=f'{queue}-{label}', type=workflow_type,
                          input=json.dumps(data if data is not None else {}, ensure_ascii=False))

        def query(handle, name='state', data=None):
            return json.loads(client('query', handle, name=name, input=json.dumps(data))['data'])

        def signal(handle, name, data=None):
            client('signal', handle, name=name, input=json.dumps(data, ensure_ascii=False))

        def history(handle):
            response = subprocess.run([str(CLI), 'workflow', 'show', '--address', address,
                                       '--workflow-id', handle['workflow_id'], '--run-id', handle['run_id'],
                                       '--output', 'json'], text=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, timeout=10)
            assert response.returncode == 0, response.stderr
            return json.loads(response.stdout)

        def save_history(label, handle):
            result = history(handle)
            (histories / f'{label}.json').write_text(json.dumps(result, indent=2))
            return result

        try:
            server = launch(CLI, 'server', extra=('server', 'start-dev', '--ip', '127.0.0.1',
                                                 '--port', address.split(':')[1], '--headless'))
            deadline = time.monotonic() + 30
            while True:
                assert server.poll() is None
                try:
                    with socket.create_connection(('127.0.0.1', int(address.split(':')[1])), timeout=0.2):
                        break
                except OSError:
                    assert time.monotonic() < deadline
                    time.sleep(0.1)
            main_handle = start('main', data={'initial': 10, 'label': 'Счётчик 🌍'})
            signal(main_handle, 'add', {'amount': 3})
            signal(main_handle, 'add', {'amount': -2})
            report['cases'].append('V client started a workflow and buffered two signals before any worker existed')
            env = dict(os.environ, TEMPORAL_ADDRESS=f'http://{address}', TEMPORAL_NAMESPACE='default',
                       TEMPORAL_TASK_QUEUE=queue)
            before_worker = launch(WORKFLOW_WORKER, 'worker-before', env)
            expected = {'total': 11, 'additions': [3, -2], 'label': 'Счётчик 🌍'}
            initial_state = query(main_handle)
            if initial_state != expected:
                (histories / 'initial-query-failure.json').write_text(json.dumps({'observed': initial_state, 'expected': expected, 'history': history(main_handle)}, indent=2, ensure_ascii=False))
            assert initial_state == expected, initial_state
            independent = subprocess.run([str(CLI), 'workflow', 'query', '--address', address,
                                          '--workflow-id', main_handle['workflow_id'], '--run-id', main_handle['run_id'],
                                          '--type', 'state'], text=True, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, timeout=10)
            assert independent.returncode == 0, independent.stderr
            assert 'total' in independent.stdout and '11' in independent.stdout, independent.stdout
            (LOGS / 'client-independent-query.log').write_text(independent.stdout)
            report['cases'].append('Independent Temporal CLI queried the V workflow with zero query arguments')
            before_queries = save_history('before-queries', main_handle)
            assert query(main_handle) == expected
            assert query(main_handle, 'offset', {'amount': 9}) == 20
            for name, data in [('missing', None), ('explode', None), ('offset', {'amount': 'bad'}), ('invalid-result', None)]:
                error = client('query', main_handle, name=name, input=json.dumps(data), success=False)
                report['rpc_errors'][name] = error
            assert query(main_handle) == expected
            assert history(main_handle) == before_queries, 'queries changed workflow history'
            assert before_worker.poll() is None
            report['cases'].append('Read-only queries returned state/arguments; unknown, throwing and invalid-input queries failed without history/state mutation')

            duplicate = client('start', workflow_id=main_handle['workflow_id'], type='v.counter', input='{}', success=False)
            assert int(duplicate['code']) == 6, duplicate
            missing = client('query', {'workflow_id': queue + '-missing', 'run_id': ''}, name='state', success=False)
            assert int(missing['code']) == 5, missing
            timeout = client('result', main_handle, wait_ms=150, rpc_ms=150, success=False)
            assert int(timeout['code']) == 4, timeout
            report['rpc_errors'].update(duplicate=duplicate, not_found=missing, deadline=timeout)
            report['cases'].append('Client preserved gRPC AlreadyExists, NotFound and deadline status codes')

            wait_handle = start('wait', 'v.wait')
            no_queries = client('query', wait_handle, name='state', success=False)
            assert 'does not implement queries' in no_queries['message'], no_queries
            client('cancel', wait_handle)
            assert client('result', wait_handle)['status'] == 'canceled'
            report['cases'].append('Non-queryable workflow returned a query error; V client cancellation/result reported canceled')

            failed_handle = start('invalid-signal')
            signal(failed_handle, 'unknown', None)
            failed = client('result', failed_handle)
            assert failed['status'] == 'failed' and 'unknown signal' in failed['message'], failed
            assert query(failed_handle)['total'] == 0
            report['cases'].append('Signal handler errors failed only their workflow; V result API returned typed failure status')

            unregistered = start('unknown-type', 'v.missing')
            assert client('result', unregistered)['status'] == 'failed'
            unavailable = client('query', unregistered, name='state', success=False)
            assert 'workflow state unavailable' in unavailable['message'], unavailable
            assert counts(history(unregistered))['workflowtaskfailed'] == 0
            report['cases'].append('Queries of failed and unregistered workflow types returned state or an isolated query error')

            other_handle = start('other', data={'initial': 100, 'label': 'independent'})
            signal(other_handle, 'add', {'amount': 50})
            assert query(other_handle)['total'] == 150
            assert query(main_handle) == expected
            handles = [main_handle, other_handle]
            before = [save_history(f'before-restart-{index}', handle) for index, handle in enumerate(handles)]
            before_worker.kill()
            assert before_worker.wait(timeout=5) == -9
            signal(main_handle, 'add', {'amount': 7})
            signal(other_handle, 'add', {'amount': 5})
            after_worker = launch(WORKFLOW_WORKER, 'worker-after', env)
            states = [query(main_handle), query(other_handle)]
            assert states == [dict(expected, total=18, additions=[3, -2, 7]),
                              {'total': 155, 'additions': [50, 5], 'label': 'independent'}], states
            for index, handle in enumerate(handles):
                after = save_history(f'after-restart-{index}', handle)
                assert after['events'][:len(before[index]['events'])] == before[index]['events']
                assert counts(after)['workflowtaskfailed'] == 0, counts(after)
                prefix = f'SIGNAL run={handle["run_id"]} replay=true name=add'
                assert prefix in (LOGS / 'client-worker-after.log').read_text()
            report['cases'].append('Fresh V worker replayed historical signals for two runs and handled signals queued while it was down')

            for index, handle in enumerate(handles):
                signal(handle, 'finish')
                result = client('result', handle)
                assert result['status'] == 'completed' and json.loads(result['data']) == states[index], result
                assert query(handle) == states[index], 'completed-workflow query lost state'
                final = save_history(f'completed-{index}', handle)
                assert counts(final)['workflowexecutionsignaled'] == (4 if index == 0 else 3)
                assert counts(final)['activitytaskscheduled'] == 0
                assert counts(final)['workflowexecutioncompleted'] == 1
                assert counts(final)['workflowtaskfailed'] == 0, counts(final)
            report['cases'].append('V signals completed both workflows; V result API and post-completion queries returned reconstructed state')
            assert after_worker.poll() is None
            after_worker.terminate()
            assert after_worker.wait(timeout=15) == 0
            assert 'WORKFLOW_SHUTDOWN complete cached_runs=0' in (LOGS / 'client-worker-after.log').read_text()
            report.update(passed=True, killed_worker_pid=before_worker.pid, replacement_worker_pid=after_worker.pid,
                          workflow_handles=handles, final_states=states)
            print(json.dumps(report, indent=2, ensure_ascii=False))
        finally:
            (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
            for process in reversed(processes):
                stop(process)


if __name__ == '__main__':
    main()

"""Updates (including restart), child workflows, run chains and patch markers."""

RESULT_REPORT = 'orchestration-results.json'
from contextlib import ExitStack
import json
import os
import socket
import subprocess
import time
import uuid

from smoke import ARTIFACTS, CLI, LOGS, ROOT, WORKER, free_port, stop
from restart_probe import WORKFLOW_WORKER, counts

CLIENT = ARTIFACTS / 'tools/v-client'


def main():
    address = f'127.0.0.1:{free_port()}'
    queue = f'orchestration-{uuid.uuid4().hex}'
    processes = []
    report = {'passed': False, 'cases': []}
    history_dir = ARTIFACTS / 'orchestration-histories'
    history_dir.mkdir(exist_ok=True)
    with ExitStack() as files:
        def launch(binary, label, env=None, args=()):
            output = files.enter_context((LOGS / f'orchestration-{label}.log').open('w'))
            process = subprocess.Popen([str(binary), *args], cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT)
            processes.append(process)
            return process

        def call(command, handle=None, success=True, **options):
            args = [str(CLIENT), '--address', f'http://{address}', '--task-queue', queue]
            if handle:
                args += ['--workflow-id', handle['workflow_id'], '--run-id', handle.get('run_id', '')]
            for key, value in options.items():
                args += ['--' + key.replace('_', '-'), str(value)]
            result = subprocess.run(args + [command], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40)
            if success:
                assert result.returncode == 0, result.stderr
                return json.loads(result.stdout)
            assert result.returncode != 0, result.stdout
            return json.loads(result.stderr)

        def start(label, typ, data):
            return call('start', workflow_id=f'{queue}-{label}', type=typ, input=json.dumps(data))

        def history(handle):
            result = subprocess.run([str(CLI), 'workflow', 'show', '--address', address,
                                     '--workflow-id', handle['workflow_id'], '--run-id', handle.get('run_id', ''), '--output', 'json'],
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            assert result.returncode == 0, result.stderr
            data = json.loads(result.stdout)
            (history_dir / (handle['workflow_id'] + '-' + handle.get('run_id', '') + '.json')).write_text(json.dumps(data, indent=2))
            return data

        def wait_for(predicate):
            deadline = time.monotonic() + 20
            while True:
                if predicate(): return
                assert time.monotonic() < deadline, 'condition deadline exceeded'
                time.sleep(0.1)

        try:
            server = launch(CLI, 'server', args=('server', 'start-dev', '--ip', '127.0.0.1', '--port', address.split(':')[1], '--headless'))
            deadline = time.monotonic() + 30
            while True:
                assert server.poll() is None
                try:
                    with socket.create_connection(('127.0.0.1', int(address.split(':')[1])), timeout=0.2): break
                except OSError:
                    assert time.monotonic() < deadline
                    time.sleep(0.1)
            env = dict(os.environ, TEMPORAL_ADDRESS=f'http://{address}', TEMPORAL_TASK_QUEUE=queue)
            worker = launch(WORKFLOW_WORKER, 'workflow-before', env)
            activity = launch(WORKER, 'activity', env)
            counter = start('counter', 'v.counter', {})
            accepted = call('update', counter, name='add', update_id='add-once', input='{"amount":5}')
            result = call('update-result', counter, update_id=accepted['update_id'])
            assert result['status'] == 'completed' and json.loads(result['data'])['total'] == 5, result
            duplicate = call('update', counter, name='add', update_id='add-once', input='{"amount":5}')
            assert json.loads(call('update-result', counter, update_id=duplicate['update_id'])['data'])['total'] == 5
            rejected = call('update', counter, name='add', update_id='invalid', input='{"amount":-1}')
            assert rejected['status'] == 'failed' and 'non-negative' in rejected['message'], rejected
            failed_update = call('update', counter, name='fail', update_id='failure')
            failed = call('update-result', counter, update_id=failed_update['update_id'])
            assert failed['status'] == 'failed' and 'requested update failure' in failed['message'], failed
            assert json.loads(call('query', counter, name='state')['data'])['total'] == 5
            report['cases'].append('Updates: accept, result, ID deduplication, readonly rejection, isolated handler failure')

            delayed = call('update', counter, name='delayed_add', update_id='durable-update', input='{"amount":7}')
            wait_for(lambda: counts(history(counter))['timerstarted'] == 1)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = launch(WORKFLOW_WORKER, 'workflow-after', env)
            result = call('update-result', counter, update_id=delayed['update_id'])
            assert result['status'] == 'completed' and json.loads(result['data'])['total'] == 12, result
            observed = counts(history(counter))
            assert observed['workflowexecutionupdateaccepted'] == 3, observed
            assert observed['workflowexecutionupdatecompleted'] == 3, observed
            assert observed['workflowtaskfailed'] == 0, observed
            call('signal', counter, name='finish')
            assert call('result', counter)['status'] == 'completed'
            report['cases'].append('Accepted asynchronous Update replayed after SIGKILL and completed exactly once')

            parent = start('parent', 'v.parent', {'name': 'child', 'delay_seconds': 1})
            result = call('result', parent)
            assert result['status'] == 'completed' and json.loads(result['data']) == 'Hello from V, Hello from V, child!!', result
            assert counts(history(parent))['childworkflowexecutioncompleted'] == 1
            bad_parent = start('bad-parent', 'v.parent', {'name': 'bad', 'delay_seconds': 0})
            assert call('result', bad_parent)['status'] == 'failed'
            report['cases'].append('Child workflow start/completion and child failure propagation')

            chain = start('chain', 'v.chain', {'remaining': 3, 'total': 0})
            wait_for(lambda: counts(history(chain))['markerrecorded'] == 1 and counts(history(chain))['timerstarted'] == 1)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = launch(WORKFLOW_WORKER, 'workflow-patch-replay', env)
            first = call('result', chain)
            assert first['status'] == 'continued_as_new' and first['next_run_id']
            final = call('chain-result', chain)
            assert final['status'] == 'completed' and json.loads(final['data']) == 3, final
            run = chain
            for index in range(4):
                hist = history(run)
                assert counts(hist)['markerrecorded'] == 1
                assert counts(hist)['workflowtaskfailed'] == 0
                if index < 3:
                    continuation = call('result', run)
                    run = dict(run, run_id=continuation['next_run_id'])
            report['cases'].append('Four-run continue-as-new chain, result following, and patch marker replay after SIGKILL')
            worker.terminate(); activity.terminate()
            assert worker.wait(timeout=15) == 0 and activity.wait(timeout=15) == 0
            report['passed'] = True
            print(json.dumps(report, indent=2))
        finally:
            (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
            for process in reversed(processes): stop(process)


if __name__ == '__main__': main()

"""Exercise registered workflows, isolated errors, cancellation and bounded concurrency."""

RESULT_REPORT = 'milestone-results.json'
import base64
from contextlib import ExitStack
from datetime import datetime
import json
import os
import re
import socket
import subprocess
import time
import uuid

from smoke import ARTIFACTS, CLI, LOGS, ROOT, WORKER, free_port, stop
from restart_probe import WORKFLOW_WORKER, counts, event_type


def main():
    LOGS.mkdir(parents=True, exist_ok=True)
    histories = ARTIFACTS / 'milestone-histories'
    histories.mkdir(exist_ok=True)
    address = f'127.0.0.1:{free_port()}'
    queue = f'v-milestone-{uuid.uuid4().hex}'
    processes = []
    report = {'passed': False, 'cases': [], 'worker_pids': {}, 'server_close_race_retries': {}}
    with ExitStack() as files:
        def launch(binary, label, env=None, extra=()):
            output = files.enter_context((LOGS / f'milestone-{label}.log').open('w'))
            process = subprocess.Popen([str(binary), *extra], cwd=ROOT, env=env,
                                       stdout=output, stderr=subprocess.STDOUT)
            processes.append(process)
            return process

        def cli(*args, success=True, timeout=30):
            response = subprocess.run([str(CLI), *args, '--address', address], text=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
            if success:
                assert response.returncode == 0, response.stdout
            else:
                assert response.returncode != 0, response.stdout
            return response.stdout

        def history(workflow_id):
            result = json.loads(cli('workflow', 'show', '--workflow-id', workflow_id, '--output', 'json'))
            (histories / f'{workflow_id}.json').write_text(json.dumps(result, indent=2))
            return result

        def wait_for(predicate, label, timeout=30):
            deadline = time.monotonic() + timeout
            while True:
                for process in processes:
                    assert process.poll() is None, f'Unexpected process exit ({process.pid}): {label}'
                value = predicate()
                if value:
                    return value
                assert time.monotonic() < deadline, f'Timeout: {label}'
                time.sleep(0.1)

        def start(label, workflow_type, data):
            workflow_id = f'{queue}-{label}'
            cli('workflow', 'start', '--workflow-id', workflow_id, '--type', workflow_type,
                '--task-queue', queue, '--execution-timeout', '60s', '--task-timeout', '5s',
                '--input', json.dumps(data, ensure_ascii=False))
            return workflow_id

        def terminal(workflow_id, kind):
            result = history(workflow_id)
            observed = counts(result)
            failures = [event for event in result['events'] if event_type(event) == 'workflowtaskfailed']
            assert all(event['workflowTaskFailedEventAttributes']['cause'] ==
                       'WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND' for event in failures), failures
            if failures:
                report['server_close_race_retries'][workflow_id] = len(failures)
            assert observed['workflowtasktimedout'] == 0, observed
            return result if observed[f'workflowexecution{kind}'] == 1 else None

        def complete(workflow_id):
            result = wait_for(lambda: terminal(workflow_id, 'completed'), workflow_id)
            event = next(event for event in result['events'] if event_type(event) == 'workflowexecutioncompleted')
            data = event['workflowExecutionCompletedEventAttributes']['result']['payloads'][0]['data']
            return json.loads(base64.b64decode(data))

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
            env = dict(os.environ, TEMPORAL_ADDRESS=f'http://{address}', TEMPORAL_NAMESPACE='default',
                       TEMPORAL_TASK_QUEUE=queue, TEMPORAL_ACTIVITY_CONCURRENCY='2')
            activities = launch(WORKER, 'activities', env)
            workflows = launch(WORKFLOW_WORKER, 'workflows', env)
            report['worker_pids'] = {'workflow': workflows.pid, 'activity': activities.pid}

            # All runs are started before we wait for any result.
            parallel = [start(f'parallel-{index}', 'v.parallel', {'name': f'job-{index}', 'delay_ms': 700})
                        for index in range(4)]
            restarts = [start(f'timer-{index}', 'v.restart', {'name': f'timer-{index}', 'delay_seconds': 2})
                        for index in range(2)]
            for index, workflow_id in enumerate(parallel):
                assert complete(workflow_id) == [f'job-{index}-0', f'job-{index}-1']
            for index, workflow_id in enumerate(restarts):
                assert complete(workflow_id) == f'Hello from V, Hello from V, timer-{index}!!'
            report['cases'].append('Six overlapping runs of two registered workflow types completed')

            boundaries = []
            for workflow_id in parallel + restarts:
                for event in history(workflow_id)['events']:
                    kind = event_type(event)
                    if kind not in ('activitytaskstarted', 'activitytaskcompleted'):
                        continue
                    stamp = datetime.fromisoformat(event['eventTime'].replace('Z', '+00:00')).timestamp()
                    boundaries.append((stamp, 1 if kind == 'activitytaskstarted' else -1))
            active = peak = 0
            for _, change in sorted(boundaries):
                active += change
                peak = max(peak, active)
            assert active == 0 and peak == 2, (active, peak)
            activity_log = (LOGS / 'milestone-activities.log').read_text()
            observed_active = [int(value) for value in re.findall(r'ACTIVITY_START .* active=(\d+)', activity_log)]
            assert max(observed_active) == 2 and all(value <= 2 for value in observed_active)
            report['peak_concurrent_activities_from_history'] = peak
            report['cases'].append('Actual Activity execution overlapped and stayed within configured concurrency=2')

            failures = [
                ('handler-failure', 'v.parallel', {'name': 'fail', 'fail_workflow': True}),
                ('activity-failure', 'v.parallel', {'name': 'fail', 'fail_activity': True, 'delay_ms': 100}),
                ('unknown-type', 'v.missing', {}),
                ('bad-input', 'v.restart', {'name': 'bad', 'delay_seconds': 0}),
                ('activity-timeout', 'v.parallel', {'name': 'timeout', 'delay_ms': 2000, 'timeout_seconds': 1}),
            ]
            for label, workflow_type, data in failures:
                workflow_id = start(label, workflow_type, data)
                failed = wait_for(lambda: terminal(workflow_id, 'failed'), label)
                if label == 'activity-failure':
                    assert counts(failed)['activitytaskfailed'] == 1
                if label == 'activity-timeout':
                    assert counts(failed)['activitytasktimedout'] >= 1
                report['cases'].append(f'{label}: execution failed without SDK task errors or worker exit')

            waiting = start('cancel-timer', 'v.wait', {})
            wait_for(lambda: counts(history(waiting))['timerstarted'] == 1, 'timer started')
            cli('workflow', 'cancel', '--workflow-id', waiting)
            cancelled = wait_for(lambda: terminal(waiting, 'canceled'), 'workflow cancellation')
            assert counts(cancelled)['timercanceled'] == 1
            report['cases'].append('Workflow cancellation cancelled its durable timer and closed as CANCELLED')

            running = start('cancel-running', 'v.parallel', {'name': 'cancel', 'delay_ms': 10000})
            run_id = history(running)['events'][0]['workflowExecutionStartedEventAttributes']['originalExecutionRunId']
            wait_for(lambda: (LOGS / 'milestone-activities.log').read_text().count(f'run={run_id} ') == 2,
                     'running Activities for cancellation')
            cli('workflow', 'cancel', '--workflow-id', running)
            wait_for(lambda: terminal(running, 'canceled'), 'running workflow cancellation')
            wait_for(lambda: 'ACTIVITY_CANCEL' in (LOGS / 'milestone-activities.log').read_text(),
                     'heartbeat cancellation delivered to Activity')
            report['cases'].append('Workflow cancellation requested cancellation of running Activities')

            activity_id = f'{queue}-standalone-cancel'
            cli('activity', 'start', '--activity-id', activity_id, '--type', 'v.delay', '--task-queue', queue,
                '--start-to-close-timeout', '15s', '--schedule-to-close-timeout', '20s',
                '--heartbeat-timeout', '1s', '--retry-maximum-attempts', '1',
                '--input', json.dumps({'name': 'standalone', 'delay_ms': 10000}))
            wait_for(lambda: f'id={activity_id} ' in (LOGS / 'milestone-activities.log').read_text(), 'standalone Activity running')
            cli('activity', 'cancel', '--activity-id', activity_id)
            cancellation_output = cli('activity', 'result', '--activity-id', activity_id, success=False)
            assert 'cancel' in cancellation_output.lower(), cancellation_output
            (LOGS / 'milestone-activity-cancel.log').write_text(cancellation_output)
            report['cases'].append('Standalone running Activity acknowledged heartbeat-delivered cancellation')

            final = start('after-errors', 'v.parallel', {'name': 'healthy', 'delay_ms': 10})
            assert complete(final) == ['healthy-0', 'healthy-1']
            assert activities.poll() is None and workflows.poll() is None
            report['cases'].append('Original long-running worker processes served new work after every failure/cancellation')

            shutdown_id = f'{queue}-shutdown'
            cli('activity', 'start', '--activity-id', shutdown_id, '--type', 'v.delay', '--task-queue', queue,
                '--start-to-close-timeout', '15s', '--heartbeat-timeout', '1s', '--retry-maximum-attempts', '1',
                '--input', json.dumps({'name': 'shutdown', 'delay_ms': 10000}))
            wait_for(lambda: f'id={shutdown_id} ' in (LOGS / 'milestone-activities.log').read_text(), 'shutdown Activity running')
            activities.terminate()
            assert activities.wait(timeout=10) == 0
            workflows.terminate()
            assert workflows.wait(timeout=10) == 0
            assert 'ACTIVITY_SHUTDOWN complete' in (LOGS / 'milestone-activities.log').read_text()
            assert 'WORKFLOW_SHUTDOWN complete cached_runs=0' in (LOGS / 'milestone-workflows.log').read_text()
            report['cases'].append('SIGTERM drained/cancelled in-flight Activity work and released workflow cache')
            report['passed'] = True
            print(json.dumps(report, indent=2))
        finally:
            (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
            for process in reversed(processes):
                stop(process)


if __name__ == '__main__':
    main()

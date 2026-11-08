"""SIGKILL both workers and replay two independent V workflow instances."""

RESULT_REPORT = 'restart-results.json'
import base64
from collections import Counter
from contextlib import ExitStack
import hashlib
import json
import os
import socket
import subprocess
import time
import uuid

from smoke import ARTIFACTS, CLI, LOGS, ROOT, WORKER, free_port, stop

WORKFLOW_WORKER = ARTIFACTS / 'tools/v-workflow-worker'


def event_type(event):
    return event['eventType'].removeprefix('EVENT_TYPE_').replace('_', '').lower()


def counts(history):
    return Counter(event_type(event) for event in history['events'])


def main():
    LOGS.mkdir(parents=True, exist_ok=True)
    address = f'127.0.0.1:{free_port()}'
    queue = f'v-replay-{uuid.uuid4().hex}'
    processes = []
    cases = [{'id': f'{queue}-{index}', 'name': f'restart-{index} — V 🌍'} for index in range(2)]
    (ARTIFACTS / RESULT_REPORT).write_text(json.dumps({'passed': False, 'queue': queue}) + '\n')
    with ExitStack() as files:
        def launch(binary, label, env=None, extra=()):
            output = files.enter_context((LOGS / f'restart-{label}.log').open('w'))
            process = subprocess.Popen([str(binary), *extra], cwd=ROOT, env=env,
                                       stdout=output, stderr=subprocess.STDOUT)
            processes.append(process)
            return process

        def cli(*args, timeout=10):
            result = subprocess.run([str(CLI), *args, '--address', address], text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
            assert result.returncode == 0, result.stdout
            return result.stdout

        def history(case):
            return json.loads(cli('workflow', 'show', '--workflow-id', case['id'], '--output', 'json'))

        try:
            server = launch(CLI, 'server', extra=('server', 'start-dev', '--ip', '127.0.0.1',
                                                 '--port', address.split(':')[1], '--headless'))
            deadline = time.monotonic() + 30
            while True:
                assert server.poll() is None, 'Temporal server exited'
                try:
                    with socket.create_connection(('127.0.0.1', int(address.split(':')[1])), timeout=0.2):
                        break
                except OSError:
                    assert time.monotonic() < deadline, 'Temporal did not become ready'
                    time.sleep(0.1)
            env = dict(os.environ, TEMPORAL_ADDRESS=f'http://{address}', TEMPORAL_NAMESPACE='default',
                       TEMPORAL_TASK_QUEUE=queue, TEMPORAL_ACTIVITY_CONCURRENCY='2')
            first_activity = launch(WORKER, 'activity-before', env)
            first_workflow = launch(WORKFLOW_WORKER, 'workflow-before', env)
            for case in cases:
                cli('workflow', 'start', '--workflow-id', case['id'], '--type', 'v.restart', '--task-queue', queue,
                    '--execution-timeout', '60s', '--task-timeout', '5s',
                    '--input', json.dumps({'name': case['name'], 'delay_seconds': 10}, ensure_ascii=False))
            deadline = time.monotonic() + 30
            while True:
                assert first_workflow.poll() is None and first_activity.poll() is None
                for case in cases:
                    case['before'] = history(case)
                if all(counts(case['before'])['timerstarted'] == 1 for case in cases):
                    break
                assert time.monotonic() < deadline, 'V workflows never recorded both durable timers'
                time.sleep(0.1)
            for index, case in enumerate(cases):
                observed = counts(case['before'])
                assert observed['activitytaskcompleted'] == 1 and observed['timerfired'] == 0, observed
                assert observed['workflowexecutioncompleted'] == 0
                case['run_id'] = case['before']['events'][0]['workflowExecutionStartedEventAttributes']['originalExecutionRunId']
                (ARTIFACTS / f'restart-history-before-{index}.json').write_text(json.dumps(case['before'], indent=2))
            killed = []
            for process in (first_workflow, first_activity):
                process.kill()
                assert process.wait(timeout=5) == -9
                killed.append({'pid': process.pid, 'exit_code': process.returncode})
            second_activity = launch(WORKER, 'activity-after', env)
            second_workflow = launch(WORKFLOW_WORKER, 'workflow-after', env)
            for index, case in enumerate(cases):
                cli('workflow', 'result', '--workflow-id', case['id'], timeout=40)
                after = history(case)
                (ARTIFACTS / f'restart-history-after-{index}.json').write_text(json.dumps(after, indent=2))
                observed = counts(after)
                assert observed['workflowexecutioncompleted'] == 1
                assert observed['activitytaskscheduled'] == 2 and observed['activitytaskcompleted'] == 2, observed
                assert observed['timerstarted'] == 1 and observed['timerfired'] == 1
                assert observed['workflowtaskfailed'] == 0, observed
                timeouts = [event for event in after['events'] if event_type(event) == 'workflowtasktimedout']
                assert all(event['workflowTaskTimedOutEventAttributes']['timeoutType'] ==
                           'TIMEOUT_TYPE_SCHEDULE_TO_START' for event in timeouts), timeouts
                case['sticky_failover_timeouts'] = len(timeouts)
                terminal = next(event for event in after['events'] if event_type(event) == 'workflowexecutioncompleted')
                data = terminal['workflowExecutionCompletedEventAttributes']['result']['payloads'][0]['data']
                case['result'] = json.loads(base64.b64decode(data))
                assert case['result'] == f'Hello from V, Hello from V, {case["name"]}!!'
                assert after['events'][:len(case['before']['events'])] == case['before']['events']
                case['event_counts'] = dict(observed)
                del case['before']
            assert second_workflow.poll() is None and second_activity.poll() is None
            second_workflow.terminate()
            second_activity.terminate()
            assert second_workflow.wait(timeout=15) == 0 and second_activity.wait(timeout=15) == 0
            replay_log = (LOGS / 'restart-workflow-after.log').read_text()
            assert 'cached_runs=0' in replay_log and 'WORKFLOW_SHUTDOWN complete' in replay_log
            activity_before = (LOGS / 'restart-activity-before.log').read_text()
            activity_after = (LOGS / 'restart-activity-after.log').read_text()
            for case in cases:
                prefix = f'COMMAND run={case["run_id"]} replay=true '
                assert prefix + 'schedule_activity seq=1' in replay_log
                assert prefix + 'start_timer seq=1' in replay_log
                assert activity_before.count(f'id=activity-1 run={case["run_id"]} ') == 1
                assert f'id=activity-1 run={case["run_id"]} ' not in activity_after
                assert activity_after.count(f'id=activity-2 run={case["run_id"]} ') == 1
            report = {
                'passed': True, 'runs': cases,
                'workflow_binary_sha256': hashlib.sha256(WORKFLOW_WORKER.read_bytes()).hexdigest(),
                'killed_processes': killed,
                'replacement_workflow_pid': second_workflow.pid,
                'replacement_activity_pid': second_activity.pid,
                'proof': ['Both workers killed via SIGKILL after two independent durable timers were recorded',
                          'Fresh V runtime rebuilt both runs from Core replay activations',
                          'Recorded first Activity results restored separate workflow state',
                          'Each first Activity executed once; only second Activities ran after restart',
                          'Two timers and four Activity completions; no workflow task failures or execution timeouts',
                          'Any sticky queue schedule-to-start failover timeouts are recorded separately',
                          'Replacement workers remained alive after workflow completion and exited cleanly on SIGTERM'],
            }
            (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
            print('PASS: two workflow instances recovered through replay after SIGKILL of both workers')
        finally:
            for process in reversed(processes):
                stop(process)


if __name__ == '__main__':
    main()

"""Bounded overload/restart/RSS gate; configurable for longer local soak runs.

This is a repeatable short load test, not evidence of a production-duration soak.
"""

RESULT_REPORT = 'load-results.json'
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import subprocess
import threading
import time
from pathlib import Path
from integration import TemporalFixture, ARTIFACTS, ROOT


def rss_kib(pid):
    status = Path(f'/proc/{pid}/status')
    if Path('/proc/self/status').exists():
        try:
            for line in status.read_text().splitlines():
                if line.startswith('VmRSS:'): return int(line.split()[1])
        except FileNotFoundError:
            return 0
        return 0
    output = subprocess.run(['ps', '-o', 'rss=', '-p', str(pid)], capture_output=True, text=True, timeout=3)
    return int(output.stdout.strip() or '0')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workflows', type=int, default=64)
    parser.add_argument('--batches', type=int, default=3)
    parser.add_argument('--memory-limit-mib', type=int, default=512)
    args = parser.parse_args()
    assert 8 <= args.workflows <= 4096 and 2 <= args.batches <= 100
    report = {'passed': False, 'cases': [], 'configuration': vars(args), 'samples': [], 'completed': 0, 'canceled': 0}
    try:
        with TemporalFixture('load') as fixture:
            fixture.env['TEMPORAL_MAX_CACHED_WORKFLOWS'] = '4'
            fixture.env['TEMPORAL_ACTIVITY_CONCURRENCY'] = '2'
            worker = fixture.workflows('before')
            activity = fixture.activities()
            stop_sampling = threading.Event()
            def sample():
                while not stop_sampling.wait(.25):
                    report['samples'].append({'time': time.time(), 'workflow_kib': rss_kib(worker.pid), 'activity_kib': rss_kib(activity.pid)})
            sampler = threading.Thread(target=sample)
            sampler.start()
            try:
                with ThreadPoolExecutor(max_workers=16) as pool:
                    for batch in range(args.batches):
                        def start(index):
                            cancel = index % 8 == 0
                            handle = fixture.start(f'{batch}-{index}', 'v.parallel', {'name': f'{batch}-{index}', 'delay_ms': 100, 'wait_for_cancel': cancel})
                            # Keep cancellation candidates open even if both Activities
                            # finish before this client thread gets its next timeslice.
                            if cancel: fixture.call('cancel', handle)
                            return handle, cancel, index
                        handles = list(pool.map(start, range(args.workflows)))
                        if batch == 1:
                            worker.kill(); assert worker.wait(timeout=5) == -9
                            worker = fixture.workflows('after')
                        def result(item):
                            handle, canceled, index = item
                            outcome = fixture.call('result', handle)
                            if canceled:
                                assert outcome['status'] == 'canceled', outcome
                            else:
                                assert outcome['status'] == 'completed', outcome
                                assert json.loads(outcome['data']) == [f'{batch}-{index}-0', f'{batch}-{index}-1'], outcome
                            return canceled
                        outcomes = list(pool.map(result, handles))
                        report['canceled'] += sum(outcomes)
                        report['completed'] += len(outcomes) - sum(outcomes)
                        # Independently inspect a spread of histories each batch.
                        sampled = sorted({0, 1, args.workflows // 4, args.workflows // 4 + 1,
                                          args.workflows // 2, args.workflows // 2 + 1,
                                          args.workflows - 2, args.workflows - 1})
                        for position in sampled:
                            handle, canceled, _ = handles[position]
                            history = fixture.history(handle)
                            for event in history['events']:
                                failure = event.get('workflowTaskFailedEventAttributes')
                                if not failure: continue
                                # A cancellation can race Activity completion as the
                                # workflow closes. Temporal retries this server-side
                                # UnhandledCommand; SDK/nondeterminism failures fail.
                                assert canceled and failure['cause'] == 'WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND', failure
                                assert failure['failure']['message'] == 'UnhandledCommand' and 'serverFailureInfo' in failure['failure'], failure
                                report['server_close_race_retries'] = report.get('server_close_race_retries', 0) + 1
                        handle = handles[-1][0]
                        file = ARTIFACTS / f'binary-histories/load-{batch}.bin'
                        fixture.call('history', handle, file=file)
                        replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                        assert replay.returncode == 0, replay.stderr
                        report.setdefault('drained_rss', []).append({'workflow_kib': rss_kib(worker.pid), 'activity_kib': rss_kib(activity.pid)})
            finally:
                stop_sampling.set(); sampler.join(timeout=5)
            for kind in ('workflow_kib', 'activity_kib'):
                peak = max(sample[kind] for sample in report['samples'])
                assert 0 < peak <= args.memory_limit_mib * 1024, (kind, peak)
                report.setdefault('peak_rss_kib', {})[kind] = peak
            for process in (worker, activity):
                process.terminate(); assert process.wait(timeout=20) == 0
            report['cases'] += ['Concurrent workflow batches exceeded four cached workflows and two Activity slots; every result or cancellation matched its execution',
                                'Workflow worker survived SIGKILL under load; sampled histories had no SDK/nondeterminism task failures and each batch replayed offline',
                                'Sampled workflow/Activity RSS stayed within configured per-process limit; both workers drained']
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: value for key, value in report.items() if key != 'samples'}, indent=2))

if __name__ == '__main__': main()

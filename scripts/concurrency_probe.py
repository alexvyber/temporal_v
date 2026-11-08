"""Bounded parallel workflow callbacks, run ordering and cleanup against Temporal."""
RESULT_REPORT = 'concurrency-results.json'
import argparse, json, os, signal, subprocess, tempfile, time
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture
from restart_probe import counts
BINARY = ARTIFACTS / 'tools/v-concurrency'

def invoke(f, role, *args):
    result = subprocess.run([str(BINARY), role, *map(str, args)], cwd=ROOT, env=f.env,
                            capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, (role, result.stdout, result.stderr)
    return 'OK' if result.stdout.strip() == 'OK' else json.loads(result.stdout)

def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'histories': 0, 'cases': []}
    try:
        for version, server in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
            for codec in ('', 'prefix', 'context'):
                with tempfile.TemporaryDirectory(prefix='callback-gates-', dir=ARTIFACTS) as temporary:
                    gates = Path(temporary)
                    with TemporalFixture(f'concurrency-{version}-{codec or "default"}', server_binary=server) as f:
                        f.env.update(V_SDK_EXAMPLE_CODEC=codec, V_SDK_CONCURRENCY_GATES=temporary)
                        worker = f.launch(BINARY, 'before', ('worker',))
                        handles = []
                        def start(label, mode):
                            handle = invoke(f, 'start', f.queue + '-' + label, mode)
                            handles.append(handle)
                            return handle
                        def entered(handle): return gates / (handle['workflow_id'] + '.entered')
                        def release(handle): (gates / (handle['workflow_id'] + '.release')).write_text('release')
                        def done(handle, status='completed'):
                            result = invoke(f, 'result', json.dumps(handle))
                            assert result['status'] == status, result
                            if status == 'completed': assert result['data'] == '"done"', result
                        slow = start('slow', 'hold')
                        f.wait_for(lambda: entered(slow).exists())
                        # A signal for this run must wait; other runs must progress.
                        invoke(f, 'signal', json.dumps(slow))
                        progress_started = time.monotonic()
                        for index in range(4): done(start(f'fast-{index}', 'fast'))
                        assert time.monotonic() - progress_started < 8, 'unrelated callbacks were serialized behind the held run'
                        done(start('failure', 'fail'), 'failed')
                        cancelled = start('cancelled', 'wait')
                        invoke(f, 'cancel', json.dumps(cancelled));done(cancelled, 'canceled')
                        assert not (gates / (slow['workflow_id'] + '.release')).exists()
                        release(slow);done(slow)
                        # Six held runs may occupy only three callback slots.
                        bounded = [start(f'bounded-{index}', 'hold') for index in range(6)]
                        f.wait_for(lambda: sum(entered(handle).exists() for handle in bounded) >= 2)
                        time.sleep(0.3)
                        assert sum(entered(handle).exists() for handle in bounded) <= 3
                        for handle in bounded: release(handle)
                        for handle in bounded:
                            invoke(f, 'signal', json.dumps(handle));done(handle)
                        restarted = start('restart', 'wait')
                        f.wait_for(lambda: counts(f.history(restarted))['workflowtaskcompleted'] >= 1)
                        worker.kill();worker.wait(timeout=15)
                        replacement = f.launch(BINARY, 'after', ('worker',))
                        invoke(f, 'signal', json.dumps(restarted));done(restarted)
                        draining = start('draining', 'hold_complete')
                        f.wait_for(lambda: entered(draining).exists())
                        replacement.send_signal(signal.SIGTERM)
                        time.sleep(0.2)
                        assert replacement.poll() is None, 'shutdown abandoned an active callback'
                        release(draining)
                        replacement.wait(timeout=30)
                        assert replacement.returncode == 0
                        done(draining)
                        histories = []
                        for handle in handles:
                            history = f.histories / (handle['workflow_id'] + '.bin')
                            invoke(f, 'history', json.dumps(handle), history)
                            f.assert_clean_history(handle)
                            histories.append((history, handle['workflow_id'], f.queue, dict(f.env)))
                    for history, identity, queue, environment in histories:
                        result = subprocess.run([str(BINARY), 'replay', str(history), identity, queue], cwd=ROOT,
                                                env=environment, capture_output=True, text=True, timeout=30)
                        assert result.returncode == 0 and result.stdout.strip() == 'OK', result.stderr
                        report['histories'] += 1
                    report['cases'].append(f'{version}/{codec or "default"}: unrelated progress while callback blocked; same-run ordering, 3-slot bound, failure/cancellation isolation, restart, graceful drain and offline replay')
                    print(report['cases'][-1], flush=True)
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
if __name__ == '__main__':
    parser = argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args = parser.parse_args();main(args.binary,args.report)

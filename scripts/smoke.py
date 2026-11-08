"""Exercise a real Temporal server, a V worker, and independent CLI clients."""

RESULT_REPORT = 'smoke-results.json'
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = Path(os.environ.get('SPIKE_ARTIFACTS', ROOT.parent / 'artifacts/v-sdk-spike'))
LOGS = ARTIFACTS / 'logs'
CLI = ARTIFACTS / 'tools/temporal'
WORKER = ARTIFACTS / 'tools/v-worker'


def free_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main():
    LOGS.mkdir(parents=True, exist_ok=True)
    port = free_port()
    address = f'127.0.0.1:{port}'
    queue = f'v-spike-{uuid.uuid4().hex}'
    server = worker = None
    results = []
    with (LOGS / 'smoke-server.log').open('w') as server_log, (LOGS / 'smoke-worker.log').open('w') as worker_log:
        try:
            server = subprocess.Popen([str(CLI), 'server', 'start-dev', '--ip', '127.0.0.1',
                                       '--port', str(port), '--headless'], stdout=server_log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 30
            while True:
                if server.poll() is not None:
                    raise RuntimeError('Temporal server exited; see smoke-server.log')
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise TimeoutError('Temporal did not listen within 30 seconds')
                    time.sleep(0.1)
            env = dict(os.environ, TEMPORAL_ADDRESS=f'http://{address}', TEMPORAL_NAMESPACE='default',
                       TEMPORAL_TASK_QUEUE=queue)
            worker = subprocess.Popen([str(WORKER)], cwd=ROOT, env=env, stdout=worker_log, stderr=subprocess.STDOUT)
            cases = [('unicode', 'v.echo', 'Temporal — Привет 🌍', True),
                     ('unsupported', 'v.unknown', 'ignored', False),
                     ('after-failure', 'v.echo', 'still running', True)]
            for label, activity_type, argument, success in cases:
                command = [str(CLI), 'activity', 'execute', '--address', address,
                           '--activity-id', f'{queue}-{label}', '--type', activity_type,
                           '--task-queue', queue, '--start-to-close-timeout', '10s',
                           '--schedule-to-close-timeout', '20s', '--retry-maximum-attempts', '1',
                           '--input', json.dumps(argument, ensure_ascii=False)]
                response = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
                (LOGS / f'smoke-{label}.log').write_text(response.stdout)
                expected = f'Hello from V, {argument}!' if success else 'unsupported activity: v.unknown'
                assert (response.returncode == 0) == success, response.stdout
                assert expected in response.stdout, response.stdout
                if success:
                    assert 'COMPLETED' in response.stdout, response.stdout
                results.append({'case': label, 'passed': True, 'cli_exit_code': response.returncode,
                                'expected': expected})
            assert worker.poll() is None, 'worker exited after requests'
            worker.terminate()
            assert worker.wait(timeout=20) == 0, (LOGS / 'smoke-worker.log').read_text()
            assert 'ACTIVITY_SHUTDOWN complete' in (LOGS / 'smoke-worker.log').read_text()
            (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(results, indent=2, ensure_ascii=False) + '\n')
            print('PASS: real Temporal → Rust Core → generated V task decoder → V handler → Temporal result')
            print('PASS: Unicode success, non-retryable failure, subsequent success, graceful shutdown')
        finally:
            stop(worker)
            stop(server)


if __name__ == '__main__':
    main()

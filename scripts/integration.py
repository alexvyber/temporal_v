"""Isolated real Temporal fixture shared by parity integration probes."""
from contextlib import ExitStack
import json
import os
import subprocess
import tempfile
import time
import uuid

from smoke import ARTIFACTS, CLI, LOGS, ROOT, WORKER, free_port, stop
from restart_probe import WORKFLOW_WORKER, counts


class TemporalFixture:
    def __init__(self, name, server_args=(), server_binary=None):
        self.name = name
        self.server_args = tuple(server_args)
        self.server_binary = server_binary or CLI
        self.address = f'127.0.0.1:{free_port()}'
        self.queue = f'{name}-{uuid.uuid4().hex}'
        self.processes = []
        self.files = ExitStack()
        self.histories = ARTIFACTS / f'{name}-histories'
        self.histories.mkdir(exist_ok=True)
        (ARTIFACTS / 'binary-histories').mkdir(parents=True, exist_ok=True)
        self.env = dict({key: value for key, value in os.environ.items() if not key.startswith(('TEMPORAL_', 'V_SDK_EXAMPLE_'))}, TEMPORAL_ADDRESS=f'http://{self.address}', TEMPORAL_TASK_QUEUE=self.queue)

    def launch(self, binary, label, args=(), env=None):
        output = self.files.enter_context((LOGS / f'{self.name}-{label}.log').open('w'))
        process = subprocess.Popen([str(binary), *args], cwd=ROOT, env=env or self.env,
                                   stdout=output, stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    @staticmethod
    def wait_for(predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while not predicate():
            assert time.monotonic() < deadline, 'condition deadline exceeded'
            time.sleep(0.05)

    def __enter__(self):
        LOGS.mkdir(exist_ok=True)
        server = self.launch(self.server_binary, 'server', ('server', 'start-dev', '--ip', '127.0.0.1',
                                           '--port', self.address.split(':')[1], '--headless', *self.server_args))

        def ready():
            assert server.poll() is None, 'Temporal exited before ready'
            try:
                health = subprocess.run([str(CLI), 'operator', 'cluster', 'health', '--address', self.address,
                                         '--output', 'json', '--client-connect-timeout', '1s', '--command-timeout', '1s'],
                                        cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=3)
                return health.returncode == 0 and json.loads(health.stdout).get('status') == 'SERVING'
            except (OSError, ValueError, subprocess.TimeoutExpired):
                return False
        try:
            self.wait_for(ready)
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *args):
        for process in reversed(self.processes):
            stop(process)
        self.files.close()

    def workflows(self, label='workflow'):
        return self.launch(WORKFLOW_WORKER, label)

    def activities(self, label='activity'):
        return self.launch(WORKER, label)

    def call(self, command, handle=None, success=True, **options):
        args = [str(ARTIFACTS / 'tools/v-client'), '--address', f'http://{self.address}', '--task-queue', self.queue]
        if handle:
            args += ['--workflow-id', handle['workflow_id'], '--run-id', handle.get('run_id', '')]
            if handle.get('namespace'): args += ['--namespace', handle['namespace']]
        # Linux caps each argv element at 128 KiB, independently of ARG_MAX.
        # Keep large SDK payloads intact and let the V client read their bytes.
        with ExitStack() as inputs:
            for key, value in options.items():
                if key == 'input' and len(str(value).encode('utf-8')) >= 65536:
                    payload = inputs.enter_context(tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.json', prefix='v-client-input-', dir=ARTIFACTS))
                    payload.write(str(value)); payload.flush()
                    args += ['--input-file', payload.name]
                else:
                    args += ['--' + key.replace('_', '-'), str(value)]
            response = subprocess.run(args + [command], env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40)
        if success:
            assert response.returncode == 0, response.stderr
            return json.loads(response.stdout)
        assert response.returncode != 0, response.stdout
        return json.loads(response.stderr.splitlines()[-1])

    def start(self, label, workflow_type, data):
        return self.call('start', workflow_id=f'{self.queue}-{label}', type=workflow_type, input=json.dumps(data))

    def history(self, handle):
        response = subprocess.run([str(CLI), 'workflow', 'show', '--address', self.address,
                                   '--namespace', handle.get('namespace', 'default'),
                                   '--workflow-id', handle['workflow_id'], '--run-id', handle.get('run_id', ''),
                                   '--output', 'json'], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        assert response.returncode == 0, response.stderr
        result = json.loads(response.stdout)
        (self.histories / f"{handle['workflow_id']}-{handle.get('run_id', '')}.json").write_text(json.dumps(result, indent=2))
        return result

    def assert_clean_history(self, handle):
        history = self.history(handle)
        events = counts(history)
        failures = [event['workflowTaskFailedEventAttributes'] for event in history['events'] if 'workflowTaskFailedEventAttributes' in event]
        assert events['workflowtaskfailed'] == 0, {'counts': dict(events), 'failures': failures}
        return events

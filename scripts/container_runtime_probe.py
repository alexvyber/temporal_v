"""Exercise an already-built nonroot runtime image against a disposable Temporal container.

Separate from check.sh: pass images whose source-bound validation has completed.
"""
import argparse
import json
import subprocess
import time
import uuid
from integration import ARTIFACTS, CLI, counts


def docker(*arguments, check=True):
    return subprocess.run(['docker', *arguments], capture_output=True, text=True, check=check, timeout=90).stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', required=True)
    parser.add_argument('--validation', required=True)
    parser.add_argument('--architecture', choices=('amd64', 'arm64'), required=True)
    args = parser.parse_args()
    prefix = 'temporal-v-runtime-' + uuid.uuid4().hex[:12]
    report = {'passed': False, 'architecture': args.architecture, 'cases': []}
    containers = []
    created_network = False
    try:
        runtime = json.loads(docker('image', 'inspect', args.runtime))[0]
        validation = json.loads(docker('image', 'inspect', args.validation))[0]
        assert runtime['Architecture'] == validation['Architecture'] == args.architecture
        assert runtime['Config']['User'] == '65532:65532'
        report['images'] = {'runtime': runtime['Id'], 'validation': validation['Id']}
        docker('network', 'create', prefix); created_network = True
        server = prefix + '-server'
        containers.append(server)
        docker('run', '-d', '--name', server, '--network', prefix, '--network-alias', 'temporal',
               '-p', '127.0.0.1::7233', '--entrypoint', '/build/artifacts/tools/temporal',
               args.validation, 'server', 'start-dev', '--ip', '0.0.0.0', '--port', '7233', '--headless')
        port = docker('port', server, '7233/tcp').rsplit(':', 1)[1]
        address = '127.0.0.1:' + port
        def client(command, handle=None, **options):
            call = [str(ARTIFACTS / 'tools/v-client'), '--address', 'http://' + address, '--task-queue', prefix, command]
            identity = {key: value for key, value in (handle or {}).items() if key in ('workflow_id', 'run_id')}
            for key, value in {**identity, **options}.items(): call += ['--' + key.replace('_', '-'), str(value)]
            result = subprocess.run(call, capture_output=True, text=True, timeout=45)
            assert result.returncode == 0, result.stderr
            return json.loads(result.stdout)
        deadline = time.monotonic() + 40
        while True:
            ready = subprocess.run([str(CLI), 'operator', 'cluster', 'health', '--address', address], capture_output=True, timeout=5)
            if ready.returncode == 0: break
            assert time.monotonic() < deadline, ready.stderr
            time.sleep(.2)
        def worker(kind, label):
            name = prefix + '-' + label
            containers.append(name)
            docker('run', '-d', '--name', name, '--network', prefix,
                   '--read-only', '--tmpfs', '/tmp:rw,noexec,nosuid,size=32m', '--cap-drop', 'ALL',
                   '--security-opt', 'no-new-privileges', '-e', 'TEMPORAL_ADDRESS=http://temporal:7233',
                   '-e', 'TEMPORAL_TASK_QUEUE=' + prefix, '--entrypoint', '/opt/v-sdk/bin/' + kind, args.runtime)
            return name
        workflow = worker('v-workflow-worker', 'before')
        activity = worker('v-worker', 'activity')
        handle = client('start', workflow_id=prefix, type='v.restart', input='{"name":"container","delay_seconds":3}')
        deadline = time.monotonic() + 40
        while True:
            history = subprocess.run([str(CLI), 'workflow', 'show', '--address', address, '--workflow-id', prefix, '--output', 'json'], capture_output=True, text=True, timeout=10)
            assert history.returncode == 0, history.stderr
            if counts(json.loads(history.stdout))['timerstarted'] == 1: break
            assert time.monotonic() < deadline, history.stdout
            time.sleep(.1)
        docker('kill', '--signal', 'KILL', workflow)
        worker_after = worker('v-workflow-worker', 'after')
        outcome = client('result', handle)
        assert outcome['status'] == 'completed' and 'container' in outcome['data'], outcome
        history = subprocess.run([str(CLI), 'workflow', 'show', '--address', address, '--workflow-id', prefix, '--output', 'json'], capture_output=True, text=True, check=True, timeout=15)
        events = counts(json.loads(history.stdout))
        assert events['activitytaskscheduled'] == events['activitytaskcompleted'] == 2 and events['workflowtaskfailed'] == 0, events
        for name in (worker_after, activity):
            docker('stop', '--time', '15', name)
            assert docker('inspect', '--format', '{{.State.ExitCode}}', name) == '0', docker('logs', name)
        report['cases'] += ['Nonroot read-only runtime containers loaded Rust Core and executed V workflows and remote Activities',
                            'SIGKILL/restart replay completed with two committed Activities and no workflow-task failures; SIGTERM drained both worker kinds']
        report['passed'] = True
    finally:
        for name in containers:
            output = subprocess.run(['docker', 'logs', name], capture_output=True, text=True, timeout=15)
            logs = output.stdout + output.stderr
            (ARTIFACTS / ('logs/' + name + '.log')).write_text(logs)
            docker('rm', '-f', name, check=False)
        if created_network: docker('network', 'rm', prefix, check=False)
        (ARTIFACTS / ('container-' + args.architecture + '-results.json')).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

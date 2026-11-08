"""Run the plain-V dependency consumer in a Linux container, including replay."""
import argparse
import json
import subprocess
import time
import uuid
from pathlib import Path


def docker(*arguments, check=True, timeout=60):
    result = subprocess.run(['docker', *arguments], capture_output=True, text=True, timeout=timeout)
    if check:
        result.check_returncode()
    return result


def wait_for(callback, timeout=40):
    end = time.monotonic() + timeout
    while not callback():
        if time.monotonic() >= end:
            raise RuntimeError('Container dependency check timed out')
        time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True)
    parser.add_argument('--architecture', required=True, choices=['arm64', 'amd64'])
    parser.add_argument('--server-image', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    prefix = 'v-dependency-' + uuid.uuid4().hex[:10]
    containers = []
    cli = '/build/artifacts/tools/temporal'
    report = {'passed': False, 'architecture': args.architecture, 'cases': []}
    args.output.mkdir(parents=True, exist_ok=True)
    network = False
    try:
        image = json.loads(docker('image', 'inspect', args.image).stdout)[0]
        assert image['Architecture'] == args.architecture
        report['image'] = image['Id']
        report['compiler'] = docker('run', '--rm', '--entrypoint', 'v', args.image, 'version').stdout.strip()
        docker('network', 'create', prefix)
        network = True
        server = prefix + '-server'
        containers.append(server)
        docker('run', '-d', '--name', server, '--network', prefix, '--entrypoint', cli,
               args.server_image, 'server', 'start-dev', '--ip', '0.0.0.0', '--port', '7233', '--headless')
        wait_for(lambda: docker('exec', server, cli, 'operator', 'cluster', 'health',
                                '--address', '127.0.0.1:7233', check=False).returncode == 0)
        environment = ['--network', prefix, '-e', 'TEMPORAL_ADDRESS=http://' + server + ':7233',
                       '-e', 'TEMPORAL_TASK_QUEUE=' + prefix]
        worker = prefix + '-worker'
        client = prefix + '-client'
        containers += [worker, client]
        docker('run', '-d', '--name', worker, *environment, args.image)
        docker('run', '-d', '--name', client, *environment, args.image, 'client')

        def history():
            result = docker('exec', server, cli, 'workflow', 'show', '--address', '127.0.0.1:7233',
                            '--workflow-id', 'normal-v-dependency', '--output', 'json', check=False)
            return json.loads(result.stdout) if result.returncode == 0 else {'events': []}

        wait_for(lambda: any('timerStartedEventAttributes' in event for event in history()['events']))
        docker('kill', '--signal', 'KILL', worker)
        assert docker('wait', worker).stdout.strip() == '137'
        replacement = prefix + '-replacement'
        containers.append(replacement)
        docker('run', '-d', '--name', replacement, *environment, args.image)
        assert docker('wait', client, timeout=45).stdout.strip() == '0'
        output = docker('logs', client).stdout
        assert 'DEPENDENCY_OK' in output
        replacement_logs = docker('logs', replacement).stdout
        assert 'replay=true start_timer' in replacement_logs, replacement_logs
        final = history()
        for field in ['timerStartedEventAttributes', 'workflowExecutionCompletedEventAttributes']:
            assert sum(field in event for event in final['events']) == 1
        assert not any('workflowTaskFailedEventAttributes' in event for event in final['events'])
        (args.output / 'history.json').write_text(json.dumps(final, indent=2) + '\n')
        docker('kill', '--signal', 'TERM', replacement)
        assert docker('wait', replacement).stdout.strip() == '0'
        report.update(passed=True, cases=['Plain V 0.5.2 compiled the installed module without SDK flags',
                                         'Linux V client and worker executed against Temporal',
                                         'SIGKILL replacement replayed the timer and completed once',
                                         'SIGTERM drained and exited successfully'])
    finally:
        for name in reversed(containers):
            logs = docker('logs', name, check=False)
            (args.output / (name + '.log')).write_text(logs.stdout + logs.stderr)
            docker('rm', '-f', name, check=False)
        if network:
            docker('network', 'rm', prefix, check=False)
        (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

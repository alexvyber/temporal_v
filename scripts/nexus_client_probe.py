"""Standalone Nexus executions, scoped conversion and durable callbacks."""

RESULT_REPORT = 'nexus-client-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('nexus-client') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded')
            created = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                      '--name', fixture.queue + '-endpoint', '--target-namespace', 'default',
                                      '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert created.returncode == 0, created.stderr
            worker = fixture.workflows('before')
            nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-before')
            for mode in ('nexus-client-start', 'nexus-client-finish'):
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / ('logs/' + mode + '.log')).write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-5000:]
                report['cases'] += json.loads(result.stdout)
                if mode == 'nexus-client-start':
                    for process in (worker, nexus):
                        process.kill(); assert process.wait(timeout=5) == -9
                    worker = fixture.workflows('after')
                    nexus = fixture.launch(ARTIFACTS / 'tools/v-nexus-worker', 'nexus-after')
            for process in (worker, nexus):
                process.terminate(); assert process.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

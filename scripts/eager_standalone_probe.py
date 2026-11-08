"""Actual eager standalone task dispatch, reservation fallback and worker loss."""
RESULT_REPORT = 'eager-standalone-results.json'
import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture

BINARY = ARTIFACTS / 'tools/v-eager-activity'


def invoke(role, fixture):
    result = subprocess.run([str(BINARY), role], cwd=ROOT, env=fixture.env,
                            capture_output=True, text=True, timeout=45)
    (ARTIFACTS / f'logs/eager-standalone-{role}.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stderr
    return result.stdout


def prepare_gate(fixture):
    directory = fixture.files.enter_context(tempfile.TemporaryDirectory(prefix='v-eager-activity-'))
    fixture.env['V_SDK_EXAMPLE_EAGER_GATE'] = str(Path(directory) / 'release')


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('eager-standalone-legacy') as fixture:
            prepare_gate(fixture)
            fixture.env['V_SDK_EXAMPLE_EAGER_EXPECT_INLINE'] = 'false'
            assert 'EXERCISE_OK' in invoke('exercise', fixture)
            report['cases'].append('The older embedded 1.32 server ignored eager requests; ordinary polling completed every task and released declined inline reservations')
        with TemporalFixture('eager-standalone', server_binary=ARTIFACTS / 'tools/temporal-protocol',
                             server_args=('--dynamic-config-value', 'activity.enableEagerStart=true')) as fixture:
            prepare_gate(fixture)
            assert 'EXERCISE_OK' in invoke('exercise', fixture)
            report['cases'].append('Observed actual inline started Activity tasks and their completion; no-worker, slot-exhausted, post-shutdown and throttled-worker starts used normal polling; ten rejected starts released reservations and a subsequent inline task completed')
        with TemporalFixture('eager-standalone-restart', server_binary=ARTIFACTS / 'tools/temporal-protocol',
                             server_args=('--dynamic-config-value', 'activity.enableEagerStart=true')) as fixture:
            prepare_gate(fixture)
            original = fixture.launch(BINARY, 'before', ('restart',))
            log = ARTIFACTS / 'logs/eager-standalone-restart-before.log'
            def ready():
                assert original.poll() is None, log.read_text()
                return 'RESTART_READY' in log.read_text()
            fixture.wait_for(ready)
            original.kill(); assert original.wait(timeout=10) == -9
            assert 'RECOVERED' in invoke('recover', fixture)
            report['cases'].append('An inline standalone task persisted a heartbeat before SIGKILL; a replacement V worker completed the next attempt using the server-retained heartbeat checkpoint')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'eager-standalone-sanitized-results.json'), default=RESULT_REPORT)
    args = parser.parse_args()
    main(args.binary, args.report)

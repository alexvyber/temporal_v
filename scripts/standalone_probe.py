"""Standalone Activity client lifecycle, target conversion and worker-loss retry."""

RESULT_REPORT = 'standalone-results.json'
import json
import subprocess
import tempfile
from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        for codec in ('context', 'binary-prefix'):
            with tempfile.TemporaryDirectory(prefix='v-standalone-tokens-') as token_dir, TemporalFixture('standalone') as fixture:
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec, V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded',
                                   TEMPORAL_LOG_FORMAT='json', V_SDK_EXAMPLE_ASYNC_TOKEN_DIR=token_dir)
                worker = fixture.activities('before')
                for mode in ('standalone-start', 'standalone-finish'):
                    result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env,
                                            capture_output=True, text=True, timeout=90)
                    (ARTIFACTS / ('logs/' + mode + '.log')).write_text(result.stdout + result.stderr)
                    assert result.returncode == 0, result.stderr[-5000:]
                    report['cases'] += json.loads(result.stdout)
                    if mode == 'standalone-start':
                        worker.kill()
                        assert worker.wait(timeout=5) == -9
                        worker = fixture.activities('after')
                worker.terminate()
                assert worker.wait(timeout=15) == 0
                records = [json.loads(line) for line in (ARTIFACTS / 'logs/standalone-after.log').read_text().splitlines()]
                reasons = {record['fields']['reason'] for record in records if record.get('message') == 'Activity cancellation observed'}
                assert {'paused', 'reset', 'cancelled'} <= reasons, reasons
                report['cases'].append('No workflow worker was running; independent worker cancellation logs preserved management reasons and shutdown drained')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

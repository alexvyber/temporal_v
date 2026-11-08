"""Exercise generated RPCs and concurrent client shutdown against Temporal."""

RESULT_REPORT = 'service-results.json'
import json
import subprocess

from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('service') as fixture:
            fixture.workflows()
            fixture.activities()
            for mode in ('native-validation', 'nexus-links', 'service', 'operator', 'schedules', 'updates', 'concurrency', 'cancellation'):
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], env=fixture.env,
                                        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=90)
                assert result.returncode == 0, result.stderr
                report['cases'] += json.loads(result.stdout)
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

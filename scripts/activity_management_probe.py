"""Pause/reset cancellation metadata and live recovery against Temporal."""

RESULT_REPORT = 'activity-management-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('activity-management') as fixture:
            fixture.env['TEMPORAL_LOG_FORMAT'] = 'json'
            workflow = fixture.workflows()
            activity = fixture.activities()
            result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'activity-management'], cwd=ROOT, env=fixture.env,
                                    capture_output=True, text=True, timeout=70)
            assert result.returncode == 0, result.stderr
            report['cases'] += json.loads(result.stdout)
            for process in (workflow, activity):
                process.terminate()
                assert process.wait(timeout=15) == 0
            logs = [json.loads(line) for line in (ARTIFACTS / 'logs/activity-management-activity.log').read_text().splitlines()]
            cancellations = [item['fields'] for item in logs if item.get('message') == 'Activity cancellation observed']
            assert any(item['reason'] == 'paused' and item['paused'] == 'true' for item in cancellations), cancellations
            assert any(item['reason'] == 'reset' and item['reset'] == 'true' for item in cancellations), cancellations
            report['cases'].append('ActivityCancelledError and context cancellation snapshot retained Core reason plus pause/reset detail flags; structured logs and graceful drain passed')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

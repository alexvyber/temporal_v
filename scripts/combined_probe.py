"""Single-Core workflow/Activity worker, eager delivery, limits and replay."""

RESULT_REPORT = 'combined-results.json'
import json
import subprocess
import urllib.request
from integration import TemporalFixture, ARTIFACTS, ROOT, LOGS, counts
from smoke import free_port


def scrape(port):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=5) as response:
        return response.read().decode()


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('combined') as fixture:
            port = free_port()
            fixture.env.update(TEMPORAL_LOG_FORMAT='json', V_SDK_EXAMPLE_AUTOSCALING='true', V_SDK_EXAMPLE_COMBINED_WORKER='true', TEMPORAL_METRICS_ADDRESS=f'127.0.0.1:{port}', TEMPORAL_CORE_LOG_FILTER='debug', TEMPORAL_ACTIVITY_CONCURRENCY='8')
            worker = fixture.workflows('before')
            runs = [fixture.start(f'remote-{index}', 'v.restart', {'name': f'combined-{index}', 'delay_seconds': 3}) for index in range(4)]
            local = fixture.start('local', 'v.activity-options', {'local': True, 'fail_until': 1})
            for run in runs: fixture.wait_for(lambda: counts(fixture.history(run))['timerstarted'] == 1)
            metrics = scrape(port)
            (ARTIFACTS / 'combined-metrics.prom').write_text(metrics)
            logs = (LOGS / 'combined-before.log').read_text()
            assert 'activity tasks received from WFT complete' in logs, 'no eagerly returned Activities observed'
            worker.kill()
            assert worker.wait(timeout=5) == -9
            fixture.env['TEMPORAL_METRICS_ADDRESS'] = f'127.0.0.1:{free_port()}'
            worker = fixture.workflows('after')
            for run in runs + [local]:
                result = fixture.call('result', run)
                assert result['status'] == 'completed', result
                fixture.assert_clean_history(run)
            before = (LOGS / 'combined-before.log').read_text()
            after = (LOGS / 'combined-after.log').read_text()
            # Each committed first echo is replayed from history, never executed again.
            assert before.count('ACTIVITY_START type=v.echo id=activity-1') == len(runs)
            assert 'ACTIVITY_START type=v.echo id=activity-1' not in after
            report['cases'].append('One Core worker executed concurrent workflows, remote and local Activities; debug evidence confirmed eager delivery through WFT completion')
            file = ARTIFACTS / 'binary-histories/combined.bin'
            fixture.call('history', runs[0], file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', runs[0]['workflow_id'], '--task-queue', fixture.queue], cwd=ROOT, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            for line in before.splitlines()[:-1] + after.splitlines():
                json.loads(line)
            report['cases'].append('Concurrent SDK/Core debug logs remained valid JSON records')
            report['cases'].append('Combined worker survived SIGKILL/restart without repeating committed Activities; offline replay and graceful drain passed')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

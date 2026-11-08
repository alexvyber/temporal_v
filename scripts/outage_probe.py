"""Authenticated CONNECT, rejected credentials and worker recovery after severed TCP."""

RESULT_REPORT = 'outage-results.json'
import json
import subprocess
import time
from integration import TemporalFixture, ARTIFACTS, ROOT, LOGS, counts
from network_proxy import ConnectProxy


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('outage') as fixture, ConnectProxy(fixture.address) as proxy:
            fixture.env.update(V_SDK_EXAMPLE_COMBINED_WORKER='true', TEMPORAL_PROXY_TARGET=proxy.address,
                               TEMPORAL_PROXY_USERNAME='v-test', TEMPORAL_PROXY_PASSWORD='disposable-proxy-password')
            worker = fixture.workflows()
            run = fixture.start('restart', 'v.restart', {'name': 'outage', 'delay_seconds': 5})
            counter = fixture.start('counter', 'v.counter', {'initial': 2})
            fixture.wait_for(lambda: counts(fixture.history(run))['timerstarted'] == 1)
            connections_before = proxy.accepted
            proxy.pause()
            time.sleep(7)
            assert worker.poll() is None, 'worker exited during network outage'
            proxy.resume()
            fixture.call('signal', counter, name='add', input='{"amount":4}')
            assert json.loads(fixture.call('query', counter, name='state', rpc_ms=25000)['data'])['total'] == 6
            fixture.call('signal', counter, name='finish')
            assert fixture.call('result', counter)['status'] == 'completed'
            assert fixture.call('result', run)['status'] == 'completed'
            assert proxy.accepted > connections_before, 'Core did not reconnect through proxy'
            events = counts(fixture.history(run))
            assert events['activitytaskscheduled'] == 2 and events['activitytaskcompleted'] == 2, events
            fixture.assert_clean_history(counter)
            report['cases'].append('Authenticated HTTP CONNECT carried client and combined worker traffic; severed connections recovered after seven-second outage without process restart or duplicate Activities')
            invalid = dict(fixture.env, TEMPORAL_PROXY_PASSWORD='incorrect-disposable-password')
            failed = subprocess.run([str(ARTIFACTS / 'tools/v-client'), '--address', 'http://' + fixture.address, '--workflow-id', counter['workflow_id'], 'describe'],
                                    cwd=ROOT, env=invalid, capture_output=True, text=True, timeout=20)
            assert failed.returncode != 0 and proxy.rejected > 0, failed.stdout
            assert 'disposable-proxy-password' not in failed.stderr
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            text = (LOGS / 'outage-workflow.log').read_text()
            assert 'disposable-proxy-password' not in text
            report['cases'].append('Proxy rejected incorrect credentials; SDK error and worker logs did not expose the disposable password; graceful shutdown passed after reconnect')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

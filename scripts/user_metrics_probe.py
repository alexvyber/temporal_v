"""User metrics ownership, concurrent close, handler scopes and replay suppression."""

RESULT_REPORT = 'user-metrics-results.json'
import argparse
import json
import subprocess
import urllib.request
from integration import ARTIFACTS, ROOT, CLI, TemporalFixture, counts
from metrics_options_probe import wait_line
from smoke import free_port

BINARY = ARTIFACTS / 'tools/v-user-metrics'


def scrape(port):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=5) as response:
        return response.read().decode()


def samples(text, name):
    return [(line.rsplit(' ', 1)[0], float(line.rsplit(' ', 1)[1])) for line in text.splitlines()
            if line.startswith(name + '{') or line.startswith(name + ' ')]


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        result = subprocess.run([str(BINARY), 'validation'], capture_output=True, text=True, timeout=10)
        assert result.returncode == 0 and result.stdout.strip() == 'VALIDATION_OK', result.stderr
        with TemporalFixture('user-metrics-instruments') as fixture:
            port = free_port()
            with (ARTIFACTS / 'logs/user-metrics-instruments.log').open('w') as log:
                process = subprocess.Popen([str(BINARY), 'instruments', 'http://' + fixture.address, f'127.0.0.1:{port}'],
                                           cwd=ROOT, env=fixture.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=log, text=True)
                try:
                    wait_line(process, 'READY')
                    text = scrape(port)
                    (ARTIFACTS / 'user-metrics-instruments.prom').write_text(text)
                    assert sorted(value for _, value in samples(text, 'v_user_counter_total')) == [2, 803], text
                    assert samples(text, 'v_user_integer_sum')[0][1] == 4, text
                    assert samples(text, 'v_user_float_sum')[0][1] == 1.25, text
                    assert samples(text, 'v_user_duration_seconds_sum')[0][1] == .25, text
                    assert samples(text, 'v_user_gauge')[0][1] == 7, text
                    assert samples(text, 'v_user_gauge_float')[0][1] == -1.5, text
                    assert samples(text, 'v_user_signed')[0][1] == -1, text
                    assert all(value in text for value in ('scope="bound"', 'scope="override"', 'flag="true"', 'integer="-2"', 'float="1.5"'))
                    process.stdin.write('\n'); process.stdin.flush(); wait_line(process, 'CLOSED')
                    assert process.wait(timeout=10) == 0
                finally:
                    if process.poll() is None: process.kill(); process.wait(timeout=5)
            report['cases'].append('All seven instrument kinds and four attribute types exported exact values; eight writers counted 803 events, per-record attributes overrode bound values and metrics retained their runtime after client close')
            report['cases'].append('No-exporter validation, invalid type/value/name handling, meter close racing eight writers, idempotent close and rejected use after close passed')
        with TemporalFixture('user-metrics-replay', server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            endpoint = subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', fixture.address,
                                       '--name', fixture.queue + '-endpoint', '--target-namespace', 'default',
                                       '--target-task-queue', fixture.queue], capture_output=True, text=True, timeout=20)
            assert endpoint.returncode == 0, endpoint.stderr
            before_port, after_port = free_port(), free_port()
            before = fixture.launch(BINARY, 'before', args=['worker'], env=dict(fixture.env, V_SDK_EXAMPLE_USER_METRICS_ADDRESS=f'127.0.0.1:{before_port}'))
            handle = fixture.call('start', workflow_id=fixture.queue, type='v.user-metrics', input='{}')
            fixture.wait_for(lambda: counts(fixture.history(handle))['timerstarted'] == 1, timeout=40)
            text = scrape(before_port)
            (ARTIFACTS / 'user-metrics-before.prom').write_text(text)
            assert samples(text, 'v_user_workflow_events_total')[0][1] == 4, text
            assert sorted(value for _, value in samples(text, 'v_user_activity_events_total')) == [1, 1], text
            assert samples(text, 'v_user_nexus_events_total')[0][1] == 1, text
            assert 'workflow_type="v.user-metrics"' in text and 'activity_type="v.metrics.remote"' in text
            before.kill(); assert before.wait(timeout=10) == -9
            after = fixture.launch(BINARY, 'after', args=['worker'], env=dict(fixture.env, V_SDK_EXAMPLE_USER_METRICS_ADDRESS=f'127.0.0.1:{after_port}'))
            state = fixture.call('query', handle, name='stage', input='null')
            assert state['data'] == '3', state
            replay_text = scrape(after_port)
            assert sum(value for _, value in samples(replay_text, 'v_user_workflow_events_total')) == 0, replay_text
            fixture.call('signal', handle, name='finish', input='null')
            assert fixture.call('result', handle)['status'] == 'completed'
            text = scrape(after_port)
            (ARTIFACTS / 'user-metrics-after.prom').write_text(text)
            assert samples(text, 'v_user_workflow_events_total')[0][1] == 1, text
            assert not samples(text, 'v_user_activity_events_total') and not samples(text, 'v_user_nexus_events_total'), text
            fixture.assert_clean_history(handle)
            path = ARTIFACTS / 'binary-histories/user-metrics.bin'
            result = subprocess.run([str(BINARY), 'history', str(path)], env=fixture.env, capture_output=True, text=True, timeout=15)
            assert result.returncode == 0, result.stderr
            result = subprocess.run([str(BINARY), 'replay', str(path)], env=fixture.env, capture_output=True, text=True, timeout=15)
            assert result.returncode == 0 and result.stdout.strip() == 'REPLAY_OK', result.stderr
            after.terminate(); assert after.wait(timeout=20) == 0
            report['cases'].append('Workflow/local/remote Activity/Nexus metric scopes exported contextual labels; SIGKILL/replacement and offline replay suppressed four historical emissions, then the cached workflow counter recorded one new live signal')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    from pathlib import Path
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'user-metrics-sanitized-results.json'), default=RESULT_REPORT)
    args = parser.parse_args()
    main(args.binary, args.report)

"""Custom V metric exporters: typed Core/user records, faults, ownership and replay."""
RESULT_REPORT = 'metric-export-results.json'
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
from integration import ARTIFACTS, ROOT, TemporalFixture

BINARY = ARTIFACTS / 'tools/v-metric-export'


def invoke(role, *arguments, env=None):
    result = subprocess.run([str(BINARY), role, *map(str, arguments)], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=45)
    (ARTIFACTS / f'logs/metric-export-{role}.log').write_text(result.stdout + result.stderr)
    assert 'private-custom-export-secret' not in result.stdout + result.stderr
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        with tempfile.TemporaryDirectory(prefix='v-metric-export-') as directory, TemporalFixture('metric-export') as fixture:
            fixture.env['V_SDK_EXAMPLE_METRIC_GATE'] = str(Path(directory) / 'closed-gate')
            assert invoke('validation', env=fixture.env) == 'STARTUP_OK'
            assert invoke('isolated', env=fixture.env) == 'ISOLATED_OK'
            values = json.loads(invoke('values', env=fixture.env))
            records = values['user_records']
            def rows(suffix): return [row for row in records if row['name'].endswith(suffix)]
            counter = rows('v_custom_counter')
            assert sum(int(row['value']) for row in counter if row['attributes']['scope'] == 'bound') == 803, values
            assert sum(int(row['value']) for row in counter if row['attributes']['scope'] == 'override') == 2, values
            for name, value in [('integer', str(2**64 - 1)), ('float', '1.25'), ('duration', '250'),
                                ('gauge', '7'), ('gauge_float', '-1.5'), ('signed', '-7')]:
                matching = rows('v_custom_' + name)
                assert len(matching) == 1 and matching[0]['value'] == value, matching
            for row in records:
                for key, expected in [('boolean', 'true'), ('integer', '-2'), ('float', '1.5'), ('runtime', 'test')]:
                    assert row['attributes'][key] == expected, row
            assert any(name.startswith('custom_') for name in values['core_names']), values
            for mode in ('throw', 'block', 'self-close'):
                stats = json.loads(invoke(mode, env=fixture.env))
                assert stats['finished'] and stats['core_done'] and not stats['pending'], stats
            report['cases'].append('Invalid settings failed before ownership transfer; ten failed native client constructions drained their exporters; independent live runtimes retained separate records after one closed. All seven typed user instrument kinds reached V, including u64::MAX, signed negative and exact millisecond duration values; eight writers emitted 803 bound plus two overridden counter events; Core metrics, global tags, late meter leases and shutdown/context expiration passed')
            report['cases'].append('Exporter exceptions stayed private, blocked cooperative handlers timed out with observable bounded drops, and an exporter closed its own client/meter without joining its own dispatcher')
            first = Path(directory) / 'before.jsonl'
            fixture.env['V_SDK_EXAMPLE_METRIC_EXPORT_FILE'] = str(first)
            before = fixture.launch(BINARY, 'before', ('worker',))
            handle = fixture.call('start', workflow_id=fixture.queue, type='v.custom-metrics', input='null')
            assert fixture.call('query', handle, name='stage', input='null')['data'] == '1'
            fixture.wait_for(lambda: first.exists() and len(first.read_text().splitlines()) == 1)
            before.kill(); assert before.wait(timeout=10) == -9
            second = Path(directory) / 'after.jsonl'
            fixture.env['V_SDK_EXAMPLE_METRIC_EXPORT_FILE'] = str(second)
            after = fixture.launch(BINARY, 'after', ('worker',))
            assert fixture.call('query', handle, name='stage', input='null')['data'] == '1'
            fixture.call('signal', handle, name='finish', input='null')
            assert fixture.call('result', handle)['status'] == 'completed'
            after.terminate(); assert after.wait(timeout=20) == 0
            assert len(first.read_text().splitlines()) == 1 and len(second.read_text().splitlines()) == 1
            fixture.assert_clean_history(handle)
            history = ARTIFACTS / 'binary-histories/metric-export.bin'
            invoke('history', history, env=fixture.env)
            replay_env = fixture.env.copy()
            report['cases'].append('SIGKILL/replacement reconstructed workflow state and suppressed its historical emission; one new live signal exported one event and graceful shutdown drained the exporter')
        assert invoke('replay', history, env=replay_env) == 'REPLAY_OK'
        report['cases'].append('Completed custom-exporter workflow history replayed offline with Temporal stopped')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'metric-export-sanitized-results.json'), default=RESULT_REPORT)
    args = parser.parse_args()
    main(args.binary, args.report)

"""Pending Update policies, closure, continuation and repaired worker replay."""
RESULT_REPORT = 'handler-lifecycle-results.json'
import argparse
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, counts


def main(binary, report_name):
    report = {'passed': False, 'cases': [], 'histories': 0, 'abandoned_updates': []}
    try:
        for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]:
            for codec in ['', 'context']:
                with TemporalFixture('handlers-' + version + '-' + (codec or 'default') + '-' + binary.name, server_binary=ARTIFACTS / 'tools' / server) as fixture:
                    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec, TEMPORAL_LOG_FORMAT='json')
                    worker = fixture.launch(binary, 'before', ('worker',))
                    handles = {}
                    def state(handle): return json.loads(fixture.call('query', handle, name='state')['data'])
                    for mode in ['complete', 'continue', 'cancel', 'fail', 'drain', 'handler-close']:
                        handle = fixture.start(mode, 'v.update-lifecycle', 'wait'); handles[mode] = handle
                        fixture.wait_for(lambda: state(handle)['all_finished'])
                        for name, prefix in [('hold-warn', 'warning-'), ('hold-silent', 'silent-')]:
                            accepted = fixture.call('update', handle, name=name, update_id=prefix + mode)
                            assert accepted['status'] == 'accepted', accepted
                        current = state(handle)
                        assert not current['all_finished'] and len(current['pending']) == 2, current
                        assert {item['id'] for item in current['pending']} == {'warning-' + mode, 'silent-' + mode}, current
                        assert {item['unfinished_policy'] for item in current['pending']} == {'warn_and_abandon', 'abandon'}, current
                    worker.kill(); worker.wait(timeout=10)
                    worker = fixture.launch(binary, 'after', ('worker',))
                    for mode, handle in handles.items():
                        fixture.wait_for(lambda: len(state(handle)['pending']) == 2)
                        immediate = fixture.call('update', handle, name='immediate', update_id='immediate-' + mode)
                        outcome = fixture.call('update-result', handle, update_id='immediate-' + mode)
                        assert outcome['status'] == 'completed' and outcome['data'] == '"immediate"', (immediate, outcome)
                        assert len(state(handle)['pending']) == 2
                        if mode == 'drain':
                            fixture.call('signal', handle, name='finish-updates')
                            assert state(handle)['all_finished'] and not state(handle)['pending']
                            for prefix in ['warning-', 'silent-']:
                                outcome = fixture.call('update-result', handle, update_id=prefix + mode)
                                assert outcome['status'] == 'completed' and json.loads(outcome['data']) == prefix + mode, outcome
                        if mode == 'handler-close': fixture.call('update', handle, name='close', update_id='closer')
                        else: fixture.call('signal', handle, name='complete' if mode == 'drain' else mode)
                        result = fixture.call('result', handle)
                        expected = {'continue': 'continued_as_new', 'cancel': 'canceled', 'fail': 'failed'}.get(mode, 'completed')
                        assert result['status'] == expected, (mode, result)
                        history = fixture.history(handle)
                        assert counts(history)['workflowtaskfailed'] == 0, mode
                        if mode != 'drain':
                            for prefix in ['warning-', 'silent-']:
                                polled = subprocess.run([str(ARTIFACTS/'tools/v-client'), '--address', fixture.env['TEMPORAL_ADDRESS'], '--workflow-id', handle['workflow_id'], '--run-id', handle['run_id'], '--update-id', prefix + mode, '--wait-ms', '1000', 'update-result'], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=10)
                                if polled.returncode == 0:
                                    error = json.loads(polled.stdout)
                                    assert error['status'] == 'failed' and 'Update' in error['message'] and 'Workflow' in error['message'], error
                                else:
                                    error = json.loads(polled.stderr.splitlines()[-1])
                                    assert str(error['code']) in ['5', '9', '10'], error
                                report['abandoned_updates'].append({'server': version, 'codec': codec, 'mode': mode, 'error': error})
                    histories = list(handles.values())
                    continued = fixture.history(handles['continue'])
                    next_id = next(event['workflowExecutionContinuedAsNewEventAttributes']['newExecutionRunId'] for event in continued['events'] if 'workflowExecutionContinuedAsNewEventAttributes' in event)
                    next_handle = {'workflow_id': handles['continue']['workflow_id'], 'run_id': next_id}
                    assert fixture.call('result', next_handle)['data'] == '"continued"'
                    histories.append(next_handle)
                    log = ARTIFACTS / 'logs' / (fixture.name + '-after.log')
                    entries = [json.loads(line) for line in log.read_text().splitlines() if line.startswith('{')]
                    warnings = [entry for entry in entries if entry.get('message') == 'Workflow closed with an unfinished Update handler']
                    assert len(warnings) == 5, warnings  # complete, continue, cancel, handler-close + closer itself
                    assert all(not entry['fields']['update_id'].startswith('silent-') for entry in warnings)
                    assert not any(entry['fields']['update_id'] in ['warning-fail', 'warning-drain'] for entry in warnings)
                    files = fixture.histories / fixture.queue; files.mkdir()
                    for index, handle in enumerate(histories):
                        file = files / (str(index) + '.bin'); fixture.call('history', handle, file=file)
                        replay = subprocess.run([str(binary), 'replay', str(file), handle['workflow_id'], fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                        assert replay.returncode == 0, replay.stdout + replay.stderr
                        assert 'Workflow closed with an unfinished Update handler' not in replay.stdout + replay.stderr
                        report['histories'] += 1
                    worker.terminate(); assert worker.wait(timeout=20) == 0
                    report['cases'].append(f'{version}/{codec or "default"}: two deferred Updates retained identity/policy after SIGKILL; immediate completion and explicit drain updated handler status; completion/CAN/cancel/failure/handler-driven closure abandoned pending Updates with policy-specific replay-suppressed warnings; seven histories replayed')
        report['passed'] = True
    finally: (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--binary', type=Path, default=ARTIFACTS / 'tools/v-panic'); parser.add_argument('--report', default=RESULT_REPORT)
    args = parser.parse_args(); main(args.binary, args.report)

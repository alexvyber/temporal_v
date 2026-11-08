"""Decoder repair without workflow failure; poisoned heartbeat isolation and drain."""

RESULT_REPORT = 'codec-failure-results.json'
import json
import subprocess
import tempfile
from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('codec-repair') as fixture:
            fixture.env['V_SDK_EXAMPLE_CODEC'] = 'prefix'
            incorrect = dict(fixture.env, V_SDK_EXAMPLE_CODEC='')
            worker = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'before', env=incorrect)
            handle = fixture.start('repair', 'v.counter', {'initial': 42})
            fixture.wait_for(lambda: counts(fixture.history(handle))['workflowtaskfailed'] >= 1)
            assert counts(fixture.history(handle))['workflowexecutionfailed'] == 0
            assert worker.poll() is None
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            assert json.loads(fixture.call('query', handle, name='state', rpc_ms=25000)['data'])['total'] == 42
            fixture.call('signal', handle, name='finish')
            assert fixture.call('result', handle)['status'] == 'completed'
            file = ARTIFACTS / 'binary-histories/codec-repair.bin'
            fixture.call('history', handle, file=file)
            replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue],
                                    cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
            assert replay.returncode == 0, replay.stderr
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            report['cases'].append('Unavailable workflow decoder failed the task without closing execution; repaired decoder resumed and replayed offline')
        with TemporalFixture('codec-heartbeat') as fixture, tempfile.TemporaryDirectory(prefix='v-codec-tokens-') as directory:
            fixture.env['V_SDK_EXAMPLE_ASYNC_TOKEN_DIR'] = directory
            fixture.env['TEMPORAL_LOG_FORMAT'] = 'json'
            worker = fixture.workflows()
            activity = fixture.activities()
            result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', 'codec-failures'], cwd=ROOT, env=fixture.env,
                                    capture_output=True, text=True, timeout=60)
            assert result.returncode == 0, result.stderr
            report['cases'] += json.loads(result.stdout)
            for process in (worker, activity):
                process.terminate()
                assert process.wait(timeout=15) == 0
            report['cases'].append('Both worker loops drained successfully after the heartbeat decoder rejection')
            records = [json.loads(line) for line in (ARTIFACTS / 'logs/codec-heartbeat-workflow.log').read_text().splitlines()]
            diagnostics = [record for record in records if record.get('message', '').startswith('binary diagnostic')]
            assert len(diagnostics) == 2 and all('\\xff' in record['message'] for record in diagnostics), diagnostics
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()

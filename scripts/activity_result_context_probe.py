"""Activity input/result scopes, checkpoint retry and deferred output context."""

RESULT_REPORT = 'activity-result-context-results.json'
import json
import subprocess
import tempfile
from integration import TemporalFixture, ARTIFACTS, ROOT, CLI
from context_probe import payloads, context_of


def main():
    report = {'passed': False, 'cases': []}
    try:
        with tempfile.TemporaryDirectory(prefix='v-propagated-tokens-') as tokens, TemporalFixture('activity-result-context', server_binary=ARTIFACTS / 'tools/temporal-protocol') as fixture:
            fixture.env.update(V_SDK_EXAMPLE_CODEC='context', V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded', V_SDK_EXAMPLE_ASYNC_TOKEN_DIR=tokens)
            worker = fixture.activities('before')
            for mode in ('activity-result-start', 'activity-result-finish'):
                result = subprocess.run([str(ARTIFACTS / 'tools/v-service'), '--mode', mode], cwd=ROOT, env=fixture.env,
                                        capture_output=True, text=True, timeout=90)
                (ARTIFACTS / ('logs/' + mode + '.log')).write_text(result.stdout + result.stderr)
                assert result.returncode == 0, result.stderr[-5000:]
                report['cases'] += json.loads(result.stdout)
                if mode == 'activity-result-start':
                    worker.kill(); assert worker.wait(timeout=5) == -9
                    worker = fixture.activities('after')
            observed = []
            for suffix in ('success', 'failure', 'token', 'id', 'timeout'):
                response = subprocess.run([str(CLI), 'activity', 'describe', '--address', fixture.address,
                    '--activity-id', fixture.queue + '-propagated-' + suffix, '--raw', '--output', 'json', '--no-json-shorthand-payloads'],
                    capture_output=True, text=True, timeout=15)
                assert response.returncode == 0, response.stderr
                raw = json.loads(response.stdout)
                (ARTIFACTS / ('activity-result-wire-' + suffix + '.json')).write_text(json.dumps(raw, indent=2) + '\n')
                outcome = raw.get('outcome', raw)
                for payload in payloads(raw.get('input', {})):
                    if 'temporal-v/codec-context' in payload['metadata']: assert context_of(payload)['kind'] == 'activity'
                for payload in payloads({key: outcome[key] for key in ('result', 'failure') if key in outcome}):
                    if 'temporal-v/codec-context' not in payload['metadata']: continue
                    scope = context_of(payload)
                    assert scope['kind'] == 'nexus' and scope['endpoint'] == fixture.queue + '-endpoint' and scope['operation'] == 'activity', scope
                    observed.append(scope)
            assert len(observed) >= 5, observed
            worker.terminate(); assert worker.wait(timeout=15) == 0
            report['cases'].append('Independent standalone Activity descriptions preserved Activity input metadata and Nexus output/checkpoint-failure metadata; the worker drained')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

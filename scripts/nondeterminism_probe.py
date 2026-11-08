"""Actual incompatible code replay with default, selected and global policies."""

RESULT_REPORT = 'nondeterminism-results.json'
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        for policy in ('default', 'selected', 'global'):
            with TemporalFixture('nondeterminism-' + policy) as fixture:
                worker = fixture.workflows('original')
                handles = [fixture.start(label, 'v.nondeterminism.' + label, {}) for label in ('selected', 'other')]
                for handle in handles:
                    fixture.wait_for(lambda: counts(fixture.history(handle))['timerstarted'] == 1)
                worker.kill(); assert worker.wait(timeout=5) == -9
                fixture.env.update(V_SDK_EXAMPLE_NONDETERMINISM_CHANGED='true', V_SDK_EXAMPLE_NONDETERMINISM_POLICY=policy)
                worker = fixture.workflows('incompatible')
                for index, handle in enumerate(handles):
                    fixture.call('signal', handle, name='wake')
                    should_fail = policy == 'global' or (policy == 'selected' and index == 0)
                    if should_fail:
                        result = fixture.call('result', handle)
                        assert result['status'] == 'failed' and 'nondetermin' in result['message'].lower(), result
                    else:
                        fixture.wait_for(lambda: counts(fixture.history(handle))['workflowtaskfailed'] >= 1)
                        assert counts(fixture.history(handle))['workflowexecutionfailed'] == 0
                healthy = fixture.start('healthy', 'v.counter', {'initial': 23})
                assert json.loads(fixture.call('query', healthy, name='state')['data'])['total'] == 23
                fixture.call('signal', healthy, name='finish')
                assert fixture.call('result', healthy)['status'] == 'completed'
                worker.kill(); assert worker.wait(timeout=5) == -9
                fixture.env['V_SDK_EXAMPLE_NONDETERMINISM_CHANGED'] = 'false'
                worker = fixture.workflows('repaired')
                fixture.wait_for(lambda: 'WORKFLOW_READY' in (ARTIFACTS / ('logs/' + fixture.name + '-repaired.log')).read_text())
                for index, handle in enumerate(handles):
                    if policy == 'global' or (policy == 'selected' and index == 0): continue
                    fixture.call('signal', handle, name='finish')
                    assert fixture.call('result', handle)['data'] == '"repaired"'
                    file = ARTIFACTS / f'binary-histories/nondeterminism-{policy}-{index}.bin'
                    fixture.call('history', handle, file=file)
                    replay = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file), '--workflow-id', handle['workflow_id'],
                                             '--task-queue', fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=40)
                    assert replay.returncode == 0, replay.stderr
                worker.terminate(); assert worker.wait(timeout=15) == 0
                report['cases'].append(policy + ': incompatible replay followed the configured execution/task failure policy, isolated healthy work and allowed repair of blocked executions')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

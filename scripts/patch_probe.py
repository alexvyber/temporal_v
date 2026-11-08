"""Test actual code evolution against histories produced by prior versions."""

RESULT_REPORT = 'patch-results.json'
import json
import subprocess

from integration import TemporalFixture, ARTIFACTS, ROOT, counts


def main():
    report = {'passed': False, 'cases': []}
    directory = ARTIFACTS / 'patch-histories'
    directory.mkdir(exist_ok=True)
    histories = {}
    try:
        with TemporalFixture('patch') as fixture:
            fixture.activities()
            for mode in ('legacy', 'patched', 'deprecated'):
                fixture.env['V_SDK_EXAMPLE_PATCH_MODE'] = mode
                worker = fixture.workflows(mode)
                handle = fixture.start(mode, 'v.patch', {})
                assert fixture.call('result', handle)['data'] == '"compatible"'
                events = fixture.assert_clean_history(handle)
                assert events['markerrecorded'] == (0 if mode == 'legacy' else 1), events
                assert events['activitytaskscheduled'] == (0 if mode == 'legacy' else 1), events
                file = directory / (mode + '.bin')
                fixture.call('history', handle, file=file)
                histories[mode] = (file, handle)
                worker.terminate()
                assert worker.wait(timeout=15) == 0
        for history_mode, code_mode, success in (
            ('legacy', 'patched', True),
            ('patched', 'patched', True),
            ('patched', 'deprecated', True),
            ('deprecated', 'removed', True),
            ('patched', 'removed', False),
        ):
            file, handle = histories[history_mode]
            environment = dict(fixture.env, V_SDK_EXAMPLE_PATCH_MODE=code_mode)
            result = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(file),
                                     '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue],
                                    cwd=ROOT, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=40)
            if success:
                assert result.returncode == 0, (history_mode, code_mode, result.stderr)
            else:
                assert result.returncode != 0 and 'nondeterminism' in result.stderr.lower(), result.stderr
            report['cases'].append(f'{history_mode} history → {code_mode} code: ' + ('compatible' if success else 'correctly rejected'))
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

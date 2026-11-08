"""Exercise managed dev/time-skipping environments through the V SDK."""

RESULT_REPORT = 'environment-results.json'
import json
import os
import subprocess

from integration import ARTIFACTS, CLI, LOGS, ROOT


def main():
    report = {'passed': False, 'cases': []}
    try:
        for skipping, executable in ((False, CLI), (True, ARTIFACTS / 'tools/test-server-1.40.0/temporal-test-server')):
            assert executable.is_file(), f'run bootstrap to install {executable}'
            args = [str(ARTIFACTS / 'tools/v-environment'), '--executable', str(executable)]
            if skipping:
                args.append('--time-skipping')
            result = subprocess.run(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=90)
            (LOGS / f'environment-{skipping}.log').write_text(result.stdout + '\n' + result.stderr)
            assert result.returncode == 0, result.stderr
            report['cases'] += json.loads(result.stdout.strip().splitlines()[-1])
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

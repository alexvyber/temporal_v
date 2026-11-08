"""V-owned custom gRPC transport: ownership, cancellation, shared workers and replay."""
RESULT_REPORT = 'grpc-transport-results.json'
import argparse
import json
import os
from pathlib import Path
import subprocess
from integration import ARTIFACTS, ROOT, TemporalFixture

BINARY = ARTIFACTS / 'tools/v-grpc'


def invoke(role, *arguments, env=None):
    result = subprocess.run([str(BINARY), role, *map(str, arguments)], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=45)
    (ARTIFACTS / f'logs/grpc-{role}.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('grpc-transport') as fixture:
            assert invoke('exercise', env=fixture.env) == 'EXERCISE_OK'
            assert invoke('failed-connect', env=fixture.env) == 'FAILED_CONNECT_OK'
            report['cases'].append('Invalid/conflicting settings failed before callbacks; invalid URL and ten rejected connection handshakes drained owned callback data. Actual Core callbacks crossed C queues into V handlers; forwarded workflow/health RPCs, refreshed API key and binary metadata, explicit status/details, redacted thrown errors, invalid replies, deadlines and cancellation passed; eight concurrent callers completed 80 RPCs; count/byte saturation returned RESOURCE_EXHAUSTED; control RPCs progressed while the wait lane was full and close drained accepted requests')
            before = fixture.launch(BINARY, 'before', ('worker',))
            def ready(label, process):
                path = ARTIFACTS / f'logs/grpc-transport-{label}.log'
                def check():
                    assert process.poll() is None, path.read_text()
                    return 'CLIENT_CLOSED_WORKER_READY' in path.read_text()
                fixture.wait_for(check)
            ready('before', before)
            handle = json.loads(invoke('start', env=fixture.env))
            before.kill(); assert before.wait(timeout=10) == -9
            after = fixture.launch(BINARY, 'after', ('worker',))
            ready('after', after)
            history = ARTIFACTS / 'binary-histories/grpc-transport.bin'
            assert invoke('finish', history, env=fixture.env) == 'FINISHED'
            fixture.assert_clean_history(handle)
            after.terminate(); assert after.wait(timeout=20) == 0, (ARTIFACTS / 'logs/grpc-transport-after.log').read_text()
            report['cases'].append('Shared workers continued polling and completing through the V transport after client handles closed; SIGKILL/replacement reconstructed signal/query state and completed without workflow-task failures; graceful shutdown drained native ownership')
            replay_env = fixture.env.copy()
        assert invoke('replay', history, env=replay_env) == 'REPLAY_OK'
        report['cases'].append('The completed history replayed offline after the Temporal server stopped')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'grpc-transport-sanitized-results.json'), default=RESULT_REPORT)
    args = parser.parse_args()
    main(args.binary, args.report)

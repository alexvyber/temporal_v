"""Real Temporal typed payload conversion, context rebinding and replay."""
RESULT_REPORT = 'typed-nexus-results.json'
import argparse, json, subprocess, tempfile, base64
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, CLI
from typed_details_wire import history_details, nexus_history
BINARY = ARTIFACTS / 'tools/v-typed-values'

def invoke(f, mode, *args):
    p = subprocess.run([str(BINARY), mode, *map(str, args)], cwd=ROOT, env=f.env, capture_output=True, text=True, timeout=45)
    (ARTIFACTS / f'logs/{f.name}-{mode}.log').write_text(p.stdout + p.stderr)
    assert p.returncode == 0, p.stderr
    return 'OK' if p.stdout.strip() == 'OK' else json.loads(p.stdout)

def exercise():
    replays = []
    for (label, server) in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
        for profile in ('default', 'prefix', 'context', 'opaque', 'binary-prefix', 'proto-binary', 'proto-json'):
            with TemporalFixture(f'typed-nexus-{label}-{profile}', server_binary=server) as f:
                f.env.update(V_SDK_EXAMPLE_CODEC='' if profile in ('default', 'opaque', 'proto-binary', 'proto-json') else profile, V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded', V_SDK_DETAILS_PROTO={'proto-binary': 'binary', 'proto-json': 'json'}.get(profile, ''), V_SDK_DETAILS_OPAQUE='true' if profile == 'opaque' else 'false', V_SDK_DETAILS_ENVELOPE='false' if profile == 'default' else 'true', TEMPORAL_MAX_CACHED_WORKFLOWS='2')
                worker = f.launch(BINARY, 'worker', ('worker',))
                endpoint = f.queue + '-endpoint'
                subprocess.run([str(CLI), 'operator', 'nexus', 'endpoint', 'create', '--address', f.address, '--name', endpoint, '--target-namespace', 'default', '--target-task-queue', f.queue], capture_output=True, text=True, check=True, timeout=20)
                handles = [invoke(f, 'start-nexus-workflow', f.queue + '-' + str(index), endpoint, index) for index in range(7)]
                for (index, handle) in enumerate(handles):
                    assert invoke(f, 'nexus-result', json.dumps(handle), index) == 'OK'
                    nexus_history(f.history(handle), profile, handle, index, endpoint)
                    f.assert_clean_history(handle)
                    path = ARTIFACTS / f'binary-histories/{f.queue}-{index}.bin'
                    f.call('history', handle, file=path)
                    replays.append((path, handle['workflow_id'], f.queue, f.env.copy()))
                if label == '1-33':
                    for index in range(7):
                        handle = invoke(f, 'start-nexus', f.queue + '-direct-' + str(index), endpoint, index)
                        assert invoke(f, 'direct-nexus-result', json.dumps(handle), index) == 'OK'
                worker.terminate()
                assert worker.wait(timeout=20) == 0
            print(label, profile, 'TYPED_NEXUS_LIVE_OK', flush=True)
    for (history, identity, queue, env) in replays:
        p = subprocess.run([str(BINARY), 'replay', str(history), identity, queue], cwd=ROOT, env=env, capture_output=True, text=True, timeout=40)
        assert p.returncode == 0, p.stderr
    assert len(replays) == 98
    return len(replays)

def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        validation = subprocess.run([str(BINARY), 'validation'], cwd=ROOT, capture_output=True, text=True, timeout=40)
        (ARTIFACTS / f'logs/{report_name}-validation.log').write_text(validation.stdout + validation.stderr)
        assert validation.returncode == 0 and validation.stdout.strip() == 'TYPED_DETAILS_VALIDATION_OK', validation.stderr
        report['histories'] = exercise()
        report['cases'].append('Seven semantic value types, nested failures and exact same-scope rethrow passed five converter policies with two scope-bound codecs; typed raw forwarding retained unknown metadata')
        report['cases'].append('Both servers and seven converter profiles: seven typed Nexus request/response values, whole-payload codec envelopes, independent Nexus/workflow wire scopes, 98 offline histories and 49 standalone Nexus operation results on the supporting server')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'typed-nexus-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)

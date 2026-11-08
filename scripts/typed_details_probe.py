"""Real Temporal typed payload conversion, context rebinding and replay."""
RESULT_REPORT = 'typed-details-results.json'
import argparse, json, subprocess, tempfile, base64
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, CLI
from typed_details_wire import history_details, nexus_history
BINARY = ARTIFACTS / 'tools/v-typed-values'

def invoke(f, mode, *args, success=True):
    p = subprocess.run([str(BINARY), mode, *map(str, args)], cwd=ROOT, env=f.env, capture_output=True, text=True, timeout=45)
    (ARTIFACTS / f'logs/{f.name}-{mode}.log').write_text(p.stdout + p.stderr)
    if not success:
        return p
    assert p.returncode == 0, p.stderr
    return 'OK' if p.stdout.strip() == 'OK' else json.loads(p.stdout)

def exercise():
    replays = []
    for (label, server) in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
        for profile in ('default', 'prefix', 'context', 'opaque', 'binary-prefix', 'proto-binary', 'proto-json'):
            with tempfile.TemporaryDirectory(prefix='typed-details-', dir=ARTIFACTS) as directory, TemporalFixture(f'typed-details-{label}-{profile}', server_binary=server) as f:
                f.env.update(V_SDK_DETAILS_ENVELOPE='false', V_SDK_EXAMPLE_CODEC='' if profile in ('default', 'opaque', 'proto-binary', 'proto-json') else profile, V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded', V_SDK_DETAILS_PROTO={'proto-binary': 'binary', 'proto-json': 'json'}.get(profile, ''), V_SDK_DETAILS_OPAQUE='true' if profile == 'opaque' else 'false', V_SDK_DETAILS_TOKEN_DIR=directory, TEMPORAL_MAX_CACHED_WORKFLOWS='2')
                w = f.launch(BINARY, 'before', ('worker',))
                modes = ('failure', 'rethrow', 'heartbeat', 'defer-complete', 'defer-fail', 'defer-cancel', 'wf-cancel', 'terminate')
                handles = {mode: invoke(f, 'start', f.queue + '-' + mode, mode) for mode in modes}
                for mode in ('failure', 'rethrow'):
                    h = handles[mode]
                    f.wait_for(lambda : invoke(f, 'query', json.dumps(h)).get('ready'))
                    invoke(f, 'query-failure', json.dumps(h))
                    invoke(f, 'update-failure', json.dumps(h))
                    invoke(f, 'update-failure', json.dumps(h))
                f.wait_for(lambda : all(((Path(directory) / (handles[mode]['run_id'] + '.json')).is_file() for mode in modes if mode.startswith('defer-'))))
                # This case tests durable checkpoint conversion and replay. Complete its
                # deliberate two-attempt retry before killing the worker; a crash after
                # Core accepts attempt two can legitimately exhaust that retry policy.
                assert invoke(f, 'result', json.dumps(handles['heartbeat']))['status'] == 'completed'
                w.kill()
                assert w.wait(timeout=10) == -9
                w = f.launch(BINARY, 'after', ('worker',))
                for mode in ('failure', 'rethrow'):
                    h = handles[mode]
                    invoke(f, 'query-failure', json.dumps(h))
                    invoke(f, 'update-failure', json.dumps(h))
                    invoke(f, 'finish', json.dumps(h))
                for mode in ('defer-complete', 'defer-fail', 'defer-cancel'):
                    h = handles[mode]
                    token = Path(directory) / (h['run_id'] + '.json')
                    assert token.stat().st_mode & 511 == 384
                    if mode == 'defer-cancel':
                        invoke(f, 'cancel-activity', json.dumps(h))
                        f.wait_for(lambda : any(('activityTaskCancelRequestedEventAttributes' in e for e in f.history(h)['events'])))
                        assert invoke(f, 'token-heartbeat', token)['cancel_requested']
                    else:
                        assert not invoke(f, 'token-heartbeat', token)['cancel_requested']
                    invoke(f, {'defer-complete': 'token-complete', 'defer-fail': 'token-fail', 'defer-cancel': 'token-cancel'}[mode], token)
                invoke(f, 'cancel', json.dumps(handles['wf-cancel']))
                invoke(f, 'terminate', json.dumps(handles['terminate']))
                for (mode, h) in handles.items():
                    try:
                        result = invoke(f, 'result', json.dumps(h))
                    except Exception:
                        for failed_mode, failed_handle in handles.items():
                            (ARTIFACTS / f'logs/{f.name}-{failed_mode}-failed-history.json').write_text(json.dumps(f.history(failed_handle), indent=2))
                        raise
                    assert result['status'] == {'rethrow': 'failed', 'wf-cancel': 'canceled', 'terminate': 'terminated'}.get(mode, 'completed'), result
                    history_details(f.history(h), profile, h, mode)
                    f.assert_clean_history(h)
                    path = ARTIFACTS / f'binary-histories/{f.queue}-{mode}.bin'
                    f.call('history', h, file=path)
                    replays.append((path, h['workflow_id'], f.queue, f.env.copy()))
                w.terminate()
                assert w.wait(timeout=20) == 0
            print(label, profile, 'TYPED_DETAILS_LIVE_OK', flush=True)
    for (history, identity, queue, env) in replays:
        p = subprocess.run([str(BINARY), 'replay', str(history), identity, queue], cwd=ROOT, env=env, capture_output=True, text=True, timeout=40)
        assert p.returncode == 0, p.stderr
    assert len(replays) == 112
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
        report['cases'].append('Both servers and seven converter profiles: nested Activity/query/Update/workflow failures, dedupe, retry checkpoints, external heartbeats/deferred completion/failure/cancellation, workflow cancellation/termination, two typed query/validator interceptors, worker replacement, independent wire metadata/scope/type checks and 112 offline histories')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'typed-details-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)

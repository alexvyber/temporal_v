"""Mixed per-value payloads, raw views, typed results and durable asynchronous completion."""
RESULT_REPORT = 'mixed-values-results.json'
import argparse
import sys, json, subprocess, base64, tempfile
from pathlib import Path
from integration import TemporalFixture, ARTIFACTS, ROOT
BINARY = ARTIFACTS / 'tools/v-mixed-values'

def invoke(f, mode, *args, success=True):
    p = subprocess.run([str(BINARY), mode, *map(str, args)], cwd=ROOT, env=f.env, capture_output=True, text=True, timeout=25)
    (ARTIFACTS / f'logs/{f.name}-{mode}.log').write_text(p.stdout + p.stderr)
    if not success:
        return p
    assert p.returncode == 0, p.stderr
    return 'OK' if p.stdout.strip() == 'OK' else json.loads(p.stdout)

def encodings(payloads, codec):
    assert len(payloads) == 6
    assert [base64.b64decode(p['metadata']['encoding']).decode() for p in payloads] == ['json/plain', 'binary/plain', 'binary/protobuf', 'binary/null', 'json/plain', 'binary/plain']
    data = [base64.b64decode(p.get('data', '')) for p in payloads]
    if codec:
        assert all((v[:3] == bytes([0, 255, 90]) for v in data))
        data = [v[3:] for v in data]
    assert data[0] == b'{"value":7}' and data[1] == bytes([0, 255, 0, 128]) and (data[3] == b'') and (data[4] == b'"null"') and (data[5] == b'null'), data[:2]
    assert base64.b64decode(payloads[2]['metadata']['messageType']) == b'temporal.api.common.v1.WorkflowExecution'

def payload_value(payload, codec, kind):
    encoding = base64.b64decode(payload['metadata']['encoding']).decode()
    assert encoding == {'binary': 'binary/plain', 'protobuf': 'binary/protobuf', 'null': 'binary/null', 'binary-null': 'binary/plain'}[kind], encoding
    data = base64.b64decode(payload.get('data', ''))
    if codec:
        assert data[:3] == bytes([0, 255, 90])
        data = data[3:]
    if kind == 'binary':
        assert data == bytes([0, 255, 0, 128])
    if kind == 'binary-null':
        assert data == b'null'
    if kind == 'null':
        assert data == b''
    if kind == 'protobuf':
        assert base64.b64decode(payload['metadata']['messageType']) == b'temporal.api.common.v1.WorkflowExecution' and b'typed-result' in data
    return data

def exercise_inputs():
    replays = []
    for (label, server) in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
        for codec in (False, True):
            with TemporalFixture(f'mixed-{label}-{codec}', server_binary=server) as f:
                f.env['V_SDK_MIXED_CODEC'] = 'true' if codec else 'false'
                w = f.launch(BINARY, 'before', ('worker',))
                handles = [invoke(f, 'start', f.queue + '-' + str(i)) for i in range(4)]
                for h in handles:
                    f.wait_for(lambda : invoke(f, 'query', json.dumps(h), success=False).returncode == 0)
                    assert invoke(f, 'query', json.dumps(h)) == {'seen': 1, 'ready': True}
                    invoke(f, 'signal', json.dumps(h))
                    assert invoke(f, 'update', json.dumps(h)) == {'seen': 3, 'ready': True}
                    assert invoke(f, 'update', json.dumps(h)) == {'seen': 3, 'ready': True}
                    history = f.history(h)
                    encodings(history['events'][0]['workflowExecutionStartedEventAttributes']['input']['payloads'], codec)
                    for event in history['events']:
                        for key in ('workflowExecutionSignaledEventAttributes', 'activityTaskScheduledEventAttributes'):
                            if key in event:
                                encodings(event[key]['input']['payloads'], codec)
                        if 'workflowExecutionUpdateAcceptedEventAttributes' in event:
                            encodings(event['workflowExecutionUpdateAcceptedEventAttributes']['acceptedRequest']['input']['args']['payloads'], codec)
                w.kill()
                assert w.wait(timeout=10) == -9
                w = f.launch(BINARY, 'after', ('worker',))
                for (index, h) in enumerate(handles):
                    assert invoke(f, 'query', json.dumps(h)) == {'seen': 3, 'ready': True}
                    invoke(f, 'finish', json.dumps(h))
                    assert invoke(f, 'result', json.dumps(h)) == {'seen': 3, 'ready': True}
                    f.assert_clean_history(h)
                    path = ARTIFACTS / f'binary-histories/{f.name}-{index}.bin'
                    f.call('history', h, file=path)
                    replays.append((path, h['workflow_id'], f.queue, f.env.copy()))
                w.terminate()
                assert w.wait(timeout=20) == 0
            print(label, codec, 'LIVE_OK', flush=True)
    for (history, id, queue, environment) in replays:
        p = subprocess.run([str(BINARY), 'replay', str(history), id, queue], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=40)
        assert p.returncode == 0, p.stderr
    assert len(replays) == 16
    return len(replays)

def exercise_outputs():
    replays = []
    for (label, server) in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
        for codec in (False, True):
            with tempfile.TemporaryDirectory(prefix='mixed-tokens-', dir=ARTIFACTS) as directory, TemporalFixture(f'mixed-output-{label}-{codec}', server_binary=server) as f:
                f.env.update(V_SDK_MIXED_CODEC='true' if codec else 'false', V_SDK_MIXED_TOKEN_DIR=directory, TEMPORAL_MAX_CACHED_WORKFLOWS='2')
                w = f.launch(BINARY, 'before', ('worker',))
                direct = [(kind, invoke(f, 'start', f.queue + '-' + kind, kind)) for kind in ('binary', 'protobuf', 'null', 'binary-null')]
                deferred = [(kind, invoke(f, 'start', f.queue + '-async-' + kind, 'async.' + kind)) for kind in ('binary', 'protobuf', 'null', 'binary-null')]
                for (kind, h) in direct:
                    f.wait_for(lambda : invoke(f, 'query', json.dumps(h), success=False).returncode == 0)
                f.wait_for(lambda : all(((Path(directory) / (h['run_id'] + '.json')).is_file() for (kind, h) in deferred)))
                w.kill()
                assert w.wait(timeout=10) == -9
                w = f.launch(BINARY, 'after', ('worker',))
                for (kind, h) in deferred:
                    token = Path(directory) / (h['run_id'] + '.json')
                    assert token.stat().st_mode & 511 == 384
                    try:
                        invoke(f, 'complete-async', token, kind)
                    except BaseException:
                        (ARTIFACTS / f'{f.name}-completion-failure-history.json').write_text(json.dumps(f.history(h), indent=2) + '\n')
                        raise
                for (index, (kind, h)) in enumerate(direct + deferred):
                    f.wait_for(lambda : invoke(f, 'query', json.dumps(h), success=False).returncode == 0)
                    invoke(f, 'finish', json.dumps(h))
                    outcome = invoke(f, 'result-wire', json.dumps(h))
                    assert outcome['status'] == 'completed'
                    history = f.history(h)
                    scheduled = [event['activityTaskScheduledEventAttributes'] for event in history['events'] if 'activityTaskScheduledEventAttributes' in event]
                    assert len(scheduled) == 1 and scheduled[0].get('heartbeatTimeout', '0s') == '0s'
                    assert scheduled[0]['startToCloseTimeout'] == '60s'
                    f.assert_clean_history(h)
                    completed = [event['workflowExecutionCompletedEventAttributes'] for event in history['events'] if 'workflowExecutionCompletedEventAttributes' in event]
                    data = payload_value(completed[0]['result']['payloads'][0], codec, kind)
                    assert outcome['raw_hex'] == data.hex() and outcome['raw_kind'] == {'binary': 'binary', 'protobuf': 'protobuf', 'null': 'null_value', 'binary-null': 'binary'}[kind], outcome
                    assert outcome['data_hex'] == (b'null' if kind == 'null' else data).hex(), outcome
                    if index >= len(direct):
                        activity = [event['activityTaskCompletedEventAttributes'] for event in history['events'] if 'activityTaskCompletedEventAttributes' in event]
                        assert payload_value(activity[0]['result']['payloads'][0], codec, kind) == data
                    path = ARTIFACTS / f'binary-histories/{f.name}-{index}.bin'
                    f.call('history', h, file=path)
                    replays.append((path, h['workflow_id'], f.queue, f.env.copy()))
                w.terminate()
                assert w.wait(timeout=20) == 0
            print(label, codec, 'LIVE_OK', flush=True)
    for (history, id, queue, environment) in replays:
        p = subprocess.run([str(BINARY), 'replay', str(history), id, queue], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=40)
        assert p.returncode == 0, p.stderr
    assert len(replays) == 32
    return len(replays)

def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        validated = subprocess.run([str(BINARY), 'validation'], cwd=ROOT, capture_output=True, text=True, timeout=30)
        (ARTIFACTS / f'logs/{report_name}-validation.log').write_text(validated.stdout + validated.stderr)
        assert validated.returncode == 0 and validated.stdout.strip() == 'VALIDATION_OK', validated.stderr
        report['cases'].append('Raw views own their bytes and metadata; returned copies cannot mutate retained input. Nested codecs, configured converters, empty arguments, unnamed messages and invalid null/name validation passed')
        report['input_histories'] = exercise_inputs()
        report['cases'].append('Both server versions with/without codecs: mixed JSON/binary/protobuf/null arguments through workflow/Activity/signal/query/Update and interceptors, dedupe, independent history wire assertions, SIGKILL/replacement and 16 offline histories')
        report['output_histories'] = exercise_outputs()
        report['cases'].append('Both server versions with/without codecs: typed workflow and deferred Activity outputs, null versus binary null bytes, private durable completion tokens, raw client/event views, cache pressure, SIGKILL/replacement and 32 offline histories')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'mixed-values-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)

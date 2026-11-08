"""Concrete and mixed protobuf JSON, pinned Go expectations and durable live replay."""
RESULT_REPORT = 'protobuf-json-results.json'
import argparse, base64, hashlib, json, subprocess, sys
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture
BINARY = ARTIFACTS / 'tools/v-protobuf-json'
NAME = 'temporal.api.common.v1.WorkflowExecution'

def invoke(f, mode, *args, success=True):
    p = subprocess.run([str(BINARY), mode, *map(str, args)], cwd=ROOT, env=f.env, capture_output=True, text=True, timeout=25)
    (ARTIFACTS / f'logs/{f.name}-{mode}.log').write_text(p.stdout + p.stderr)
    if not success:
        return p
    assert p.returncode == 0, p.stderr
    return 'OK' if p.stdout.strip() == 'OK' else json.loads(p.stdout)

def wire(payload, codec, named):
    metadata = {k: base64.b64decode(v) for (k, v) in payload['metadata'].items()}
    assert metadata['encoding'] == b'json/protobuf'
    assert metadata.get('messageType', b'') == (NAME.encode() if named else b'')
    data = base64.b64decode(payload['data'])
    if codec:
        assert data[:3] == bytes([0, 255, 90])
        data = data[3:]
    message = json.loads(data)
    assert set(message) == {'workflowId', 'runId'}, message
    return {1: message['workflowId'], 2: message['runId']}

def exercise(label, server, codec, named):
    replays = []
    with TemporalFixture(f'protobuf-json-{label}-{codec}-{named}', server_binary=server) as f:
        f.env.update(V_SDK_PROTOBUF_CODEC='true' if codec else 'false', V_SDK_PROTOBUF_MESSAGE_TYPE=NAME if named else '')
        w = f.launch(BINARY, 'before', ('worker',))
        handles = []
        for mode in ('payload-世界', 'fail'):
            h = invoke(f, 'start', f.queue + '-' + ('failure' if mode == 'fail' else 'success'), mode)
            f.wait_for(lambda : invoke(f, 'query', json.dumps(h), success=False).returncode == 0)
            result = invoke(f, 'query', json.dumps(h))
            assert result['workflow_id'] == mode and result['pb_unknown'] == [], result
            assert result['run_id'] == 'protobuf-世界' + ('' if mode == 'fail' else '-activity'), result
            assert invoke(f, 'metadata', json.dumps(h))['definition']['type'] == 'v.protobuf.json'
            history = f.history(h)
            start = history['events'][0]['workflowExecutionStartedEventAttributes']['input']['payloads'][0]
            assert wire(start, codec, named)[1] == mode
            if mode == 'fail':
                failures = [event['activityTaskFailedEventAttributes'] for event in history['events'] if 'activityTaskFailedEventAttributes' in event]
                assert wire(failures[0]['failure']['applicationFailureInfo']['details']['payloads'][0], codec, named)[1] == 'fail'
            handles.append(h)
        w.kill()
        assert w.wait(timeout=10) == -9
        w = f.launch(BINARY, 'after', ('worker',))
        for (index, h) in enumerate(handles):
            assert invoke(f, 'query', json.dumps(h))['pb_unknown'] == []
            invoke(f, 'finish', json.dumps(h))
            result = invoke(f, 'result', json.dumps(h))
            assert result['pb_unknown'] == []
            history = f.history(h)
            f.assert_clean_history(h)
            completed = [event['workflowExecutionCompletedEventAttributes'] for event in history['events'] if 'workflowExecutionCompletedEventAttributes' in event]
            assert wire(completed[0]['result']['payloads'][0], codec, named)[1] == result['workflow_id']
            path = ARTIFACTS / f'binary-histories/{f.queue}-{index}.bin'
            f.call('history', h, file=path)
            replays.append((path, h['workflow_id'], f.queue, f.env.copy()))
        w.terminate()
        assert w.wait(timeout=20) == 0
    for (history, identity, queue, environment) in replays:
        replay = subprocess.run([str(BINARY), 'replay', str(history), identity, queue], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=40)
        (ARTIFACTS / f'logs/{history.stem}-offline.log').write_text(replay.stdout + replay.stderr)
        assert replay.returncode == 0, replay.stderr

def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        data_path = ROOT / '_tests/data/protobuf-json.json'
        corpus = json.loads(data_path.read_text())
        for (filename, expected) in corpus['reference']['oracle_sources'].items():
            assert hashlib.sha256((ROOT / '_tests/native/protobuf_json' / filename).read_bytes()).hexdigest() == expected
        vectors = corpus['vectors']
        path = ARTIFACTS / (report_name + '-vectors.txt')
        path.write_text(''.join((value['kind'] + '|' + value['input'] + '\n' for value in vectors)))
        checked = subprocess.run([str(BINARY), 'validation'], cwd=ROOT, capture_output=True, text=True, timeout=40)
        (ARTIFACTS / f'logs/{report_name}-validation.log').write_text(checked.stdout + checked.stderr)
        assert checked.returncode == 0 and checked.stdout.strip() == 'PROTOBUF_JSON_CONVERTER_OK', checked.stderr
        report['cases'].append('Concrete generated JSON conversion, named/unnamed metadata, wrong-type/invalid UTF-8 rejection, exact integer validation and mixed per-value raw views passed')
        actual = subprocess.check_output([str(BINARY), 'wire', str(path)], cwd=ROOT, text=True, timeout=40).splitlines()
        assert len(actual) == len(vectors)

        def semantic(value):
            if value == '0':
                return (False, None)
            assert value.startswith('1|'), value
            return (True, json.loads(value[2:]))
        differences = [{'kind': value['kind'], 'input': value['input'], 'go': value['output'], 'v': got} for (value, got) in zip(vectors, actual) if semantic(value['output']) != semantic(got)]
        report['wire'] = {'vectors': len(vectors), 'differences': differences, 'corpus_sha256': hashlib.sha256(data_path.read_bytes()).hexdigest(), 'reference': corpus['reference']}
        assert not differences, differences[:3]
        report['cases'].append(str(len(vectors)) + ' cases matched recorded Go protojson behavior using exact pinned Temporal descriptors: metadata, maps, oneofs, aliases/duplicate keys, malformed JSON, 64-bit/exponent/fraction boundaries, Any and well-known types')
        for (label, server) in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
            for codec in (False, True):
                for named in (False, True):
                    exercise(label, server, codec, named)
                    report['cases'].append(f'{label} codec={codec} named={named}: schema-validated workflow/Activity/query/results and failure details matched independent history JSON; binary unknown fields were omitted, metadata queries stayed JSON, SIGKILL/replacement and both histories replayed offline')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'protobuf-json-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)

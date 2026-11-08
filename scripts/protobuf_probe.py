"""Typed protobuf payloads and pinned Rust/V decoder interoperability."""
RESULT_REPORT = 'protobuf-results.json'
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from integration import ARTIFACTS, ROOT, TemporalFixture
from protobuf_wire_vectors import wire_vectors
BINARY = ARTIFACTS / 'tools/v-protobuf'
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
    assert metadata['encoding'] == b'binary/protobuf', metadata
    assert metadata.get('messageType', b'') == (NAME.encode() if named else b''), metadata
    data = base64.b64decode(payload['data'])
    if codec:
        assert data[:3] == bytes([0, 255, 90])
        data = data[3:]
    offset = 0

    def varint():
        nonlocal offset
        value = 0
        shift = 0
        while True:
            part = data[offset]
            offset += 1
            value |= (part & 127) << shift
            if part < 128:
                return value
            shift += 7
    fields = {}
    while offset < len(data):
        tag = varint()
        number = tag >> 3
        kind = tag & 7
        if kind == 2:
            size = varint()
            fields[number] = data[offset:offset + size].decode()
            offset += size
        elif kind == 0:
            fields[number] = varint()
        else:
            raise AssertionError(kind)
    assert fields[100] == 7, fields
    return fields

def exercise(label, server, codec, named):
    replays = []
    with TemporalFixture(f'protobuf-{label}-{codec}-{named}', server_binary=server) as f:
        f.env.update(V_SDK_PROTOBUF_CODEC='true' if codec else 'false', V_SDK_PROTOBUF_MESSAGE_TYPE=NAME if named else '')
        w = f.launch(BINARY, 'before', ('worker',))
        handles = []
        for mode in ('payload-世界', 'fail'):
            h = invoke(f, 'start', f.queue + '-' + ('failure' if mode == 'fail' else 'success'), mode)
            f.wait_for(lambda : invoke(f, 'query', json.dumps(h), success=False).returncode == 0)
            result = invoke(f, 'query', json.dumps(h))
            assert result['workflow_id'] == mode and result['pb_unknown'] == [160, 6, 7], result
            assert result['run_id'] == 'protobuf-世界' + ('' if mode == 'fail' else '-activity'), result
            assert invoke(f, 'metadata', json.dumps(h))['definition']['type'] == 'v.protobuf'
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
            assert invoke(f, 'query', json.dumps(h))['pb_unknown'] == [160, 6, 7]
            invoke(f, 'finish', json.dumps(h))
            result = invoke(f, 'result', json.dumps(h))
            assert result['pb_unknown'] == [160, 6, 7]
            history = f.history(h)
            f.assert_clean_history(h)
            completed = [event['workflowExecutionCompletedEventAttributes'] for event in history['events'] if 'workflowExecutionCompletedEventAttributes' in event]
            assert wire(completed[0]['result']['payloads'][0], codec, named)[1] == result['workflow_id']
            path = ARTIFACTS / f'binary-histories/{f.name}-{index}.bin'
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
        dependency = Path(os.environ.get('VMODULES', str(ARTIFACTS / 'vmodules'))) / 'protobuf'
        compiler_environment = dict(os.environ, VMODULES=str(dependency.parent), V_MACOS_V3_NO_FALLBACK='1', V_C_ERROR_BUG_REPORT_DISABLED='1')
        subprocess.run([sys.executable, str(ROOT / 'scripts/harden_protobuf.py'), '--dependency', str(dependency), '--verify-only'], check=True)
        validated = subprocess.run([str(BINARY), 'validation'], cwd=ROOT, capture_output=True, text=True, timeout=30)
        (ARTIFACTS / f'logs/{report_name}-validation.log').write_text(validated.stdout + validated.stderr)
        assert validated.returncode == 0 and validated.stdout.strip() == 'VALIDATION_OK', validated.stderr
        report['cases'].append('Typed message validation preserved unknown bytes/Unicode, accepted empty messages and unnamed Rust-style metadata, rejected malformed bytes/wire/UTF-8/names and mismatched names, and handled a second generated type')
        if report_name == RESULT_REPORT:
            with (ARTIFACTS / 'logs/protobuf-dependency-tests.log').open('w') as output:
                tested = subprocess.run([str(ARTIFACTS / 'tools/v/v'), '-d', 'use_bundled_libgc', 'test', str(dependency)], cwd=ROOT, env=compiler_environment, stdout=output, stderr=subprocess.STDOUT, timeout=180)
            assert tested.returncode == 0, 'protobuf dependency tests failed'
            report['cases'].append('All eight patched protobuf runtime/generator test files passed, including generated JSON/WKT/oneof and recursive-message consumers')
        vectors = wire_vectors()
        path = ARTIFACTS / 'protobuf-wire-vectors.txt'
        path.write_text(''.join((kind + '|' + data.hex() + '\n' for (kind, data, label) in vectors)))
        oracle = ARTIFACTS / 'tools/protobuf-rust-oracle'
        expected = subprocess.check_output([str(oracle), str(path)], cwd=ROOT, text=True, timeout=40).splitlines()
        actual = subprocess.check_output([str(BINARY), 'wire', str(path)], cwd=ROOT, text=True, timeout=40).splitlines()
        assert len(expected) == len(actual) == len(vectors)
        differences = [{'kind': kind, 'hex': data.hex(), 'case': label, 'rust': wanted, 'v': got} for ((kind, data, label), wanted, got) in zip(vectors, expected, actual) if wanted != got]
        report['wire'] = {'vectors': len(vectors), 'differences': differences, 'corpus_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'rust_oracle_sha256': hashlib.sha256(oracle.read_bytes()).hexdigest()}
        assert not differences, differences[:2]
        report['cases'].append(str(len(vectors)) + ' vectors matched the pinned Rust decoder across malformed/valid strings, maps, oneofs, packed enums, overflows, groups, recursion boundaries and noncanonical tags')
        for (label, server) in [('1-32', ARTIFACTS / 'tools/temporal'), ('1-33', ARTIFACTS / 'tools/temporal-protocol')]:
            for codec in (False, True):
                for named in (False, True):
                    exercise(label, server, codec, named)
                    report['cases'].append(f'{label} codec={codec} named={named}: binary/protobuf workflow/Activity/query/result and failure-detail fields matched independent wire history; unknown fields, metadata queries, SIGKILL/replacement and both histories replayed offline')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'protobuf-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)

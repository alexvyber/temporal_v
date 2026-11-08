"""Optional actual Go/V handoffs and dual replay for unfinished Update handlers."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, counts


def main(binary, reference, destination):
    destination.mkdir(parents=True, exist_ok=True)
    source = ROOT / '_tests/native/handler_temporal_go'
    manifest = {'passed': False, 'reference': {'sdk': 'v1.35.0', 'go_toolchain': subprocess.check_output(['go', 'version'], text=True).strip(), 'sources': {name: hashlib.sha256((source/name).read_bytes()).hexdigest() for name in ['main.go', 'go.mod', 'go.sum']}, 'binary_sha256': hashlib.sha256(reference.read_bytes()).hexdigest()}, 'cases': []}
    try:
        for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]:
            for direction in ['go-to-v', 'v-to-go']:
                with TemporalFixture('handler-go-' + version + '-' + direction, server_binary=ARTIFACTS/'tools'/server) as fixture:
                    before, after = (reference, binary) if direction == 'go-to-v' else (binary, reference)
                    worker = fixture.launch(before, 'before', ('worker',))
                    handles = {}
                    for mode in ['complete', 'continue', 'cancel', 'fail']:
                        handle = fixture.start(mode, 'v.update-lifecycle', 'wait'); handles[mode] = handle
                        for name, prefix in [('hold-warn', 'warning-'), ('hold-silent', 'silent-')]:
                            result = fixture.call('update', handle, name=name, update_id=prefix+mode)
                            assert result['status'] == 'accepted', result
                        state = json.loads(fixture.call('query', handle, name='state')['data'])
                        assert not state['all_finished'] and len(state['pending']) == 2, state
                    worker.kill(); worker.wait(timeout=10)
                    for handle in handles.values():
                        reset = subprocess.run([str(reference), 'reset-sticky', json.dumps(handle)], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=15)
                        assert reset.returncode == 0, reset.stderr
                    worker = fixture.launch(after, 'after', ('worker',))
                    for mode, handle in handles.items():
                        fixture.wait_for(lambda: len(json.loads(fixture.call('query', handle, name='state')['data'])['pending']) == 2)
                        fixture.call('signal', handle, name=mode)
                        result = fixture.call('result', handle)
                        assert result['status'] == {'complete':'completed', 'continue':'continued_as_new', 'cancel':'canceled', 'fail':'failed'}[mode], result
                    histories = list(handles.items())
                    continued = fixture.history(handles['continue'])
                    next_id = next(event['workflowExecutionContinuedAsNewEventAttributes']['newExecutionRunId'] for event in continued['events'] if 'workflowExecutionContinuedAsNewEventAttributes' in event)
                    next_handle = {'workflow_id': handles['continue']['workflow_id'], 'run_id': next_id}
                    assert fixture.call('result', next_handle)['data'] == '"continued"'
                    histories.append(('next', next_handle))
                    for mode, handle in histories:
                        history = fixture.history(handle)
                        assert counts(history)['workflowtaskfailed'] == 0
                        name = version + '-' + direction + '-' + mode
                        json_file = destination/(name+'.json'); json_file.write_text(json.dumps(history, indent=2)+'\n')
                        binary_file = destination/(name+'.bin'); fixture.call('history', handle, file=binary_file)
                        v_replay = subprocess.run([str(binary), 'replay', str(binary_file), handle['workflow_id'], fixture.queue], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                        go_replay = subprocess.run([str(reference), 'replay', str(json_file)], cwd=ROOT, env=fixture.env, capture_output=True, text=True, timeout=45)
                        (ARTIFACTS/'logs'/('handler-go-'+name+'-v.log')).write_text(v_replay.stdout+v_replay.stderr)
                        (ARTIFACTS/'logs'/('handler-go-'+name+'-go.log')).write_text(go_replay.stdout+go_replay.stderr)
                        assert v_replay.returncode == 0, v_replay.stdout+v_replay.stderr
                        assert go_replay.returncode == 0, go_replay.stdout+go_replay.stderr
                        manifest['cases'].append({'name':name, 'workflow_id':handle['workflow_id'], 'run_id':handle['run_id'], 'task_queue':fixture.queue, 'files':{path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in [json_file,binary_file]}})
                    worker.terminate(); assert worker.wait(timeout=20) == 0
        manifest['passed'] = True
    finally: (destination/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(manifest,indent=2))


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--binary',type=Path,required=True); parser.add_argument('--reference',type=Path,required=True); parser.add_argument('--destination',type=Path,default=ROOT/'_tests/data/handler-go')
    args=parser.parse_args(); main(args.binary,args.reference,args.destination)

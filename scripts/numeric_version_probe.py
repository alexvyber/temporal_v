"""Numeric choices survive changed code; unsupported recorded choices fail closed."""
RESULT_REPORT = 'numeric-version-results.json'
import argparse
import base64
import hashlib
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, counts

PREFIX = '__temporal_v_version/v1/' + hashlib.sha256('numeric/change 🌍/one'.encode()).hexdigest() + '/'


def markers(history):
    result = []
    for event in history['events']:
        value = event.get('markerRecordedEventAttributes', {})
        if value.get('markerName') != 'core_patch': continue
        for payloads in value['details'].values():
            for payload in payloads['payloads']:
                decoded = json.loads(base64.b64decode(payload['data']))
                if decoded.get('id', '').startswith(PREFIX): result.append(decoded)
    return result


def main(binary, report_name, quick=False):
    report = {'passed': False, 'cases': [], 'histories': 0, 'rejected_replays': 0, 'transient_query_errors': []}
    try:
        validation = subprocess.run([str(binary), 'version-validation'], cwd=ROOT, capture_output=True, text=True, timeout=30)
        assert validation.returncode == 0 and validation.stdout.strip() == 'VERSIONS_OK', validation.stdout+validation.stderr
        profiles = [('', False)] if quick else [(codec, storage) for codec in ['', 'prefix', 'context'] for storage in [False, True]]
        for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]:
            for codec, storage in profiles:
                label = 'numeric-' + version + '-' + (codec or 'default') + '-' + str(storage) + '-' + binary.name
                with TemporalFixture(label, server_binary=ARTIFACTS/'tools'/server) as fixture:
                    files = fixture.histories/fixture.queue; files.mkdir()
                    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec, V_NUMERIC_VERSION_STAGE='baseline')
                    if storage: fixture.env['V_SDK_EXAMPLE_STORAGE_DIR'] = str(files/'storage')
                    worker = fixture.launch(binary, 'baseline', ('worker',))
                    handles = []
                    states = []
                    def state(handle):
                        try: return json.loads(fixture.call('query', handle, name='state', rpc_ms=5000)['data'])
                        except AssertionError as error:
                            diagnostic = json.loads(str(error).strip())
                            if str(diagnostic.get('code')) == '4' or (str(diagnostic.get('code')) == '1' and diagnostic.get('message') == 'h2 protocol error: http2 error'):
                                assert worker.poll() is None
                                report['transient_query_errors'].append({'server': version, 'codec': codec, 'storage': storage, 'error': diagnostic})
                                return {'ready': False}
                            raise
                    def ready(handle): fixture.wait_for(lambda: state(handle)['ready'], timeout=60)
                    baseline = fixture.start('baseline', 'v.numeric-version', {'padding': 320000}); handles.append(baseline)
                    ready(baseline); states.append(state(baseline)); assert states[0]['version'] == -1
                    def replace(stage):
                        nonlocal worker
                        worker.kill(); worker.wait(timeout=10)
                        fixture.env['V_NUMERIC_VERSION_STAGE'] = stage
                        worker = fixture.launch(binary, stage, ('worker',))
                    for stage, expected in [('v0', 0), ('v1', 1)]:
                        replace(stage)
                        for handle, previous in zip(handles, states): ready(handle); assert state(handle) == previous
                        handle = fixture.start(stage, 'v.numeric-version', {'padding': 320000}); handles.append(handle)
                        ready(handle); states.append(state(handle)); assert states[-1]['version'] == expected
                    fixture.env['V_NUMERIC_VERSION_SWALLOW'] = 'true'; replace('require2')
                    fixture.call('signal', baseline, name='probe')
                    fixture.wait_for(lambda: counts(fixture.history(baseline))['workflowtaskfailed'] >= 1)
                    assert counts(fixture.history(baseline))['workflowexecutionfailed'] == 0 and worker.poll() is None
                    current = fixture.start('v2', 'v.numeric-version', {'padding': 320000}); handles.append(current)
                    ready(current); states.append(state(current)); assert states[-1]['version'] == 2
                    replace('v2'); fixture.call('signal', baseline, name='repair')
                    for handle, previous in zip(handles, states):
                        ready(handle); assert state(handle) == previous
                        fixture.call('signal', handle, name='finish')
                        result = fixture.call('result', handle); assert result['status'] == 'completed'
                        completed = json.loads(result['data']); assert completed['version'] == previous['version'] and len(completed['padding']) == 320000
                        assert completed['before'] == previous['before'] and completed['after'] == previous['after']
                    def replay(handle, name, stage, succeeds=True):
                        file = files/(name+'.bin'); fixture.call('history', handle, file=file)
                        env = dict(fixture.env, V_NUMERIC_VERSION_STAGE=stage)
                        result = subprocess.run([str(binary), 'replay', str(file), handle['workflow_id'], fixture.queue], cwd=ROOT, env=env, capture_output=True, text=True, timeout=45)
                        (files/(name+'-'+stage+'.log')).write_text(result.stdout+result.stderr)
                        assert (result.returncode == 0) == succeeds, (name, stage, result.stdout+result.stderr)
                        return result
                    for index, handle in enumerate(handles):
                        history = fixture.history(handle); recorded = markers(history)
                        scheduled = next(event['activityTaskScheduledEventAttributes'] for event in history['events'] if 'activityTaskScheduledEventAttributes' in event)
                        message_type = base64.b64decode(scheduled['input']['payloads'][0].get('metadata', {}).get('messageType', '')).decode()
                        assert (message_type == 'temporal.api.sdk.v1.ExternalStorageReference') == storage
                        assert len(recorded) == (0 if index == 0 else 1), recorded
                        if recorded: assert recorded[0]['id'] == PREFIX+str(index-1) and not recorded[0].get('deprecated', False), recorded
                        replay(handle, str(index), 'v2'); report['histories'] += 1
                        if index < 3:
                            rejected = replay(handle, str(index), 'require2', False)
                            assert 'supported range' in rejected.stderr, rejected.stderr
                            report['rejected_replays'] += 1
                    replay(handles[2], 'deprecate-old', 'deprecate')
                    for index in [0, 1]:
                        rejected = replay(handles[index], 'deprecate-' + str(index), 'deprecate', False)
                        assert 'supported range' in rejected.stderr
                        report['rejected_replays'] += 1
                    replay(handles[2], 'premature-remove', 'removed', False); report['rejected_replays'] += 1
                    replace('deprecate')
                    retired = fixture.start('retired', 'v.numeric-version', {'padding': 320000}); ready(retired)
                    assert state(retired)['version'] == 1
                    replace('removed'); ready(retired)
                    fixture.call('signal', retired, name='finish'); assert fixture.call('result', retired)['status'] == 'completed'
                    deprecated = markers(fixture.history(retired))
                    assert len(deprecated) == 1 and deprecated[0]['id'] == PREFIX + '1' and deprecated[0]['deprecated'], deprecated
                    replay(retired, 'retired', 'removed'); report['histories'] += 1
                    worker.terminate(); assert worker.wait(timeout=20) == 0
                    report['cases'].append(f'{version}/{codec or "default"}/storage={storage}: unmarked history selected default; recorded 0/1 survived max=2 code and SIGKILL; unsupported range remained task-failing despite swallowed errors while a new version-2 run progressed; repair, stable RNG, one marker per choice, deprecation, five offline histories and six incompatible rejections passed')
        report['passed'] = True
    finally: (ARTIFACTS/report_name).write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic'); parser.add_argument('--report',default=RESULT_REPORT); parser.add_argument('--quick',action='store_true')
    args=parser.parse_args(); main(args.binary,args.report,args.quick)

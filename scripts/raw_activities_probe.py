"""Dynamic remote/local Activities preserve owned raw inputs and lifecycle semantics."""
RESULT_REPORT='raw-activities-results.json'
import argparse
import hashlib
import base64
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS,ROOT,TemporalFixture,counts


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0,'standalone_activities':0}
    expected_digest=hashlib.sha256(b'\xff\0\x80'+b'x'*320000).hexdigest()
    try:
        profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['','prefix','context'] for stored in [False,True]]
        if quick:profiles=profiles[:1]
        for version,server,codec,stored in profiles:
            with TemporalFixture('raw-activities-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir();audit=directory/'calls';audit.mkdir()
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_RAW_ACTIVITY_LOG=str(audit))
                if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'))
                worker=fixture.launch(binary,'before',('worker',))
                def state(handle):return json.loads(fixture.call('query',handle,name='state',rpc_ms=20000)['data'])
                bulk=fixture.start('bulk','v.raw-activities',{'mode':'bulk'})
                fixture.wait_for(lambda:len(state(bulk))==12)
                values=state(bulk)
                for suffix,local in [('-r',False),('-l',True)]:
                    named=json.loads(values['named'+suffix]);assert named['kind']=='named' and named['is_local']==local,named
                    empty=json.loads(values['empty'+suffix]);assert empty['values']==[] and empty['kind']=='dynamic',empty
                    for label,attempt in [('mixed',1),('panic',2)]:
                        record=json.loads(values[label+suffix]);assert record['kind']=='dynamic' and record['is_local']==local and record['attempt']==attempt,record
                        descriptors=record['values'];assert len(descriptors)==5,descriptors
                        assert [item['encoding'] for item in descriptors]==['application/x-temporal-v-raw-activity','json/plain','binary/plain','binary/null','binary/protobuf'],descriptors
                        assert descriptors[0]['length']==320003 and descriptors[0]['digest']==expected_digest,descriptors
                        plain_values=[b'\xff\0\x80'+b'x'*320000,b'{"key":"json"}',b'\0\xff',b'',b'\x0a\x0braw-message']
                        assert [item['digest'] for item in descriptors]==[hashlib.sha256(value).hexdigest() for value in plain_values],descriptors
                        assert descriptors[4]['message_type']=='temporal.api.common.v1.WorkflowExecution',descriptors
                    assert values['failure'+suffix]=='failed:raw dynamic failure',values['failure'+suffix]
                    assert values['named-opaque'+suffix].startswith('failed:') and 'unsupported payload encoding' in values['named-opaque'+suffix],values['named-opaque'+suffix]
                direct=subprocess.run([str(binary),'raw-standalone'],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=30)
                assert direct.returncode==0,direct.stdout+direct.stderr
                standalone=json.loads(direct.stdout)
                assert standalone['is_standalone'] and standalone['kind']=='dynamic' and standalone['values'][0]['digest']==expected_digest,standalone
                report['standalone_activities']+=1
                cancelling=fixture.start('cancel','v.raw-activities',{'mode':'cancel'})
                def waiting():
                    return len([path for path in audit.glob('*.json') if json.loads(path.read_text())['type']=='v.raw.wait'])==2
                fixture.wait_for(waiting)
                fixture.call('signal',cancelling,name='cancel')
                fixture.wait_for(lambda:state(cancelling)=={'cancel-r':'cancelled','cancel-l':'cancelled'})
                before={path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in audit.glob('*.json')}
                worker.kill();worker.wait(timeout=10);worker=fixture.launch(binary,'after',('worker',))
                assert state(bulk)==values
                assert state(cancelling)=={'cancel-r':'cancelled','cancel-l':'cancelled'}
                for index,handle in enumerate([bulk,cancelling]):
                    fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['status']=='completed'
                    value=fixture.history(handle)
                    assert counts(value)['activitytasktimedout']==0,'raw Activity timed out before explicit cancellation'
                    if handle==bulk:
                        scheduled=next(event['activityTaskScheduledEventAttributes'] for event in value['events'] if event.get('activityTaskScheduledEventAttributes',{}).get('activityType',{}).get('name')=='v.raw.mixed')
                        message_type=base64.b64decode(scheduled['input']['payloads'][0].get('metadata',{}).get('messageType',''))
                        assert (message_type==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,message_type
                    failures=[event['workflowTaskFailedEventAttributes'].get('failure',{}).get('message','') for event in value['events'] if 'workflowTaskFailedEventAttributes' in event]
                    assert not failures,failures
                    file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
                    replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0,replay.stdout+replay.stderr
                    report['histories']+=1
                after={path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in audit.glob('*.json')}
                assert after==before,'restart/offline replay redispatched a completed Activity'
                worker.terminate();assert worker.wait(timeout=20)==0
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: named priority, empty/mixed raw inputs and owned views, remote/local panic retries and nonretryable failures, standalone raw execution, explicit cancellation, concurrent progress, SIGKILL/replacement and two offline histories without redispatch passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

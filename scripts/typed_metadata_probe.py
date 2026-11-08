"""Typed header/memo transport, information ownership and durable replay."""
RESULT_REPORT = 'typed-metadata-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture,CLI
from typed_details_wire import details,unwrap
BINARY=ARTIFACTS/'tools/v-typed-values'

def invoke(f,role,*args):
    result=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=40)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,result.stderr
    return 'OK' if result.stdout.strip()=='OK' else json.loads(result.stdout)

def inspect_map(values,profile,handle,scope):
    return details([values['item-'+str(index)] for index in range(7)],profile,handle,scope,packed_array=False)

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary
    report={'passed':False,'cases':[]}
    histories=[]
    try:
        validation=subprocess.run([str(BINARY),'metadata-validation'],cwd=ROOT,capture_output=True,text=True,timeout=40)
        (ARTIFACTS/f'logs/{report_name}-validation.log').write_text(validation.stdout+validation.stderr)
        assert validation.returncode==0 and validation.stdout.strip()=='TYPED_METADATA_VALIDATION_OK',validation.stderr
        for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for profile in ('default','prefix','context','opaque','binary-prefix','proto-binary','proto-json'):
                for packed in (False,True):
                    with TemporalFixture(f'typed-metadata-{label}-{profile}-{packed}',server_binary=server) as f:
                        f.env.update(V_SDK_DETAILS_ENVELOPE='false',V_SDK_DETAILS_PACK=str(packed).lower(),V_SDK_EXAMPLE_CODEC='' if profile in ('default','opaque','proto-binary','proto-json') else profile,V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded',V_SDK_DETAILS_PROTO={'proto-binary':'binary','proto-json':'json'}.get(profile,''),V_SDK_DETAILS_OPAQUE='true' if profile=='opaque' else 'false',TEMPORAL_MAX_CACHED_WORKFLOWS='2')
                        worker=f.launch(BINARY,'before',('worker',))
                        handle=invoke(f,'start-metadata',f.queue+'-workflow')
                        try:
                            f.wait_for(lambda:invoke(f,'metadata-query',json.dumps(handle))['ready'])
                        except Exception:
                            (ARTIFACTS/f'logs/{f.name}-failed-history.json').write_text(json.dumps(f.history(handle),indent=2))
                            raise
                        invoke(f,'metadata-signal',json.dumps(handle))
                        invoke(f,'metadata-update',json.dumps(handle))
                        worker.kill();assert worker.wait(timeout=10)==-9
                        worker=f.launch(BINARY,'after',('worker',))
                        assert invoke(f,'metadata-query',json.dumps(handle))['ready']
                        invoke(f,'metadata-update',json.dumps(handle))
                        invoke(f,'finish',json.dumps(handle))
                        assert invoke(f,'result',json.dumps(handle))['status']=='completed'
                        history=f.history(handle);found=set();updates=0
                        for event in history['events']:
                            for kind,attribute in event.items():
                                if kind=='workflowExecutionStartedEventAttributes':
                                    inspect_map(attribute['header']['fields'],profile,handle,'workflow')
                                    memo=attribute['memo']['fields']
                                    if profile=='default' and not packed:
                                        assert 'item-4' not in memo and 'item-5' not in memo
                                        for index in (0,1,2,3,6):
                                            metadata,data=unwrap(memo['item-'+str(index)],profile,handle,'workflow')
                                            assert metadata['encoding']==[b'json/plain',b'binary/plain',b'binary/protobuf',b'json/protobuf',b'binary/null',b'binary/plain',b'json/plain'][index]
                                    else:inspect_map(memo,profile,handle,'workflow')
                                    found.add('start')
                                if kind=='activityTaskScheduledEventAttributes':
                                    inspect_map(attribute['header']['fields'],profile,handle,'activity');found.add('activity')
                                if kind=='workflowExecutionSignaledEventAttributes' and attribute['signalName']=='meta':
                                    inspect_map(attribute['header']['fields'],profile,handle,'workflow');found.add('signal')
                                if kind=='workflowExecutionUpdateAcceptedEventAttributes':
                                    inspect_map(attribute['acceptedRequest']['input']['header']['fields'],profile,handle,'workflow');updates+=1;found.add('update')
                                if kind=='workflowPropertiesModifiedEventAttributes':
                                    values=attribute.get('upsertedMemo',{}).get('fields',{})
                                    for key in ('null-kept','signal','update'):
                                        if key in values:
                                            metadata,data=unwrap(values[key],profile,handle,'workflow')
                                            assert metadata['encoding']=={'null-kept':b'binary/null','signal':b'binary/protobuf','update':b'binary/plain'}[key]
                                            if key=='null-kept':assert data==b''
                                            found.add('memo-'+key)
                        expected={'start','activity','signal','update','memo-signal','memo-update','memo-null-kept'}
                        assert found==expected,found
                        assert updates==1,updates
                        described=subprocess.run([str(CLI),'workflow','describe','--raw','--no-json-shorthand-payloads','--address',f.address,'--workflow-id',handle['workflow_id'],'--run-id',handle['run_id'],'--output','json'],capture_output=True,text=True,timeout=15)
                        (ARTIFACTS/f'logs/{f.name}-raw-description.log').write_text(described.stdout+described.stderr)
                        assert described.returncode==0,described.stderr
                        description=json.loads(described.stdout)
                        (ARTIFACTS/f'logs/{f.name}-description.json').write_text(json.dumps(description,indent=2))
                        stored=description['workflowExecutionInfo'].get('memo',{}).get('fields',{})
                        expected_keys={'item-'+str(index) for index in range(7)}|{'signal','update','null-kept'}
                        if profile=='default' and not packed:expected_keys-= {'item-4','item-5','null-kept'}
                        assert set(stored)==expected_keys,(set(stored),expected_keys)
                        f.assert_clean_history(handle)
                        path=ARTIFACTS/f'binary-histories/{f.queue}.bin';f.call('history',handle,file=path)
                        histories.append((path,handle['workflow_id'],f.queue,f.env.copy()))
                        worker.terminate();assert worker.wait(timeout=20)==0
                    print(label,profile,packed,'TYPED_METADATA_LIVE_OK',flush=True)
        for history,identity,queue,environment in histories:
            result=subprocess.run([str(BINARY),'replay',str(history),identity,queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=40)
            assert result.returncode==0,result.stderr
        assert len(histories)==28
        report['histories']=len(histories)
        report['cases'].append('Both servers, seven converter profiles and packed/unpacked codec composition: seven typed workflow/local/remote Activity/signal/query/Update headers and memo values; typed null versus deletion, mutable information snapshots, deduplicated Update, SIGKILL/replacement, independent wire/scope checks and 28 offline histories')
        report['passed']=True
    finally:
        (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',choices=(RESULT_REPORT,'typed-metadata-sanitized-results.json'),default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

"""Captured non-application Nexus failures remain valid Core completions."""
RESULT_REPORT='nexus-failure-results.json'
import argparse,base64,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture
from typed_details_wire import details,wire_payload,unpack_payloads
BINARY=ARTIFACTS/'tools/v-typed-values'

def invoke(f,role,*args):
    result=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,result.stderr
    return 'OK' if result.stdout.strip()=='OK' else json.loads(result.stdout)

def inspect(value,profile,handle,scope,endpoint,index):
    count=0
    information=value.get('applicationFailureInfo',{})
    if information.get('type')=='VNexusOperationError':
        cause=value['cause']
        assert ['activityFailureInfo','timeoutFailureInfo','childWorkflowExecutionFailureInfo'][index] in cause,cause
    payloads=information.get('details',{}).get('payloads',[])
    if payloads:
        payloads=[wire_payload(base64.b64decode(p['data'])) if base64.b64decode(p['metadata']['encoding'])==b'binary/test-envelope' else p for p in payloads]
        payloads=unpack_payloads(payloads)
        if payloads:count+=details(payloads,profile,handle,scope,packed_array=False,endpoint=endpoint,operation='captured')
    if value.get('cause'):count+=inspect(value['cause'],profile,handle,scope,endpoint,index)
    return count

def exercise():
    histories=[]
    for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
        for profile in ('default','prefix','context','opaque','binary-prefix','proto-binary','proto-json'):
            for packed in (False,True):
                with TemporalFixture(f'nexus-failure-{label}-{profile}-{packed}',server_binary=server) as f:
                    f.env.update(V_SDK_DETAILS_ENVELOPE='false' if profile=='default' else 'true',V_SDK_DETAILS_PACK=str(packed).lower(),V_SDK_EXAMPLE_CODEC='' if profile in ('default','opaque','proto-binary','proto-json') else profile,V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded',V_SDK_DETAILS_PROTO={'proto-binary':'binary','proto-json':'json'}.get(profile,''),V_SDK_DETAILS_OPAQUE='true' if profile=='opaque' else 'false',TEMPORAL_MAX_CACHED_WORKFLOWS='2')
                    worker=f.launch(BINARY,'before',('worker',))
                    endpoint=f.queue+'-endpoint'
                    subprocess.run([str(CLI),'operator','nexus','endpoint','create','--address',f.address,'--name',endpoint,'--target-namespace','default','--target-task-queue',f.queue],capture_output=True,text=True,check=True,timeout=20)
                    handles=[invoke(f,'start-captured-nexus-workflow',f.queue+'-'+str(index),endpoint,index) for index in range(3)]
                    for handle in handles:
                        f.wait_for(lambda:invoke(f,'query',json.dumps(handle))['ready'])
                        invoke(f,'query-failure',json.dumps(handle));invoke(f,'update-failure',json.dumps(handle))
                    worker.kill();assert worker.wait(timeout=10)==-9
                    worker=f.launch(BINARY,'after',('worker',))
                    for index,handle in enumerate(handles):
                        invoke(f,'query-failure',json.dumps(handle));invoke(f,'update-failure',json.dumps(handle))
                        invoke(f,'finish',json.dumps(handle));assert invoke(f,'result',json.dumps(handle))['status']=='completed'
                        history=f.history(handle);seen=0;updates=0;nexus=0
                        for event in history['events']:
                            for key,attributes in event.items():
                                if key=='nexusOperationFailedEventAttributes':
                                    seen+=inspect(attributes['failure'],profile,handle,'nexus',endpoint,index);nexus+=1
                                if key=='workflowExecutionUpdateCompletedEventAttributes':
                                    seen+=inspect(attributes['outcome']['failure'],profile,handle,'workflow',endpoint,index);updates+=1
                        assert (seen,updates,nexus)==(2,1,1),(seen,updates,nexus)
                        f.assert_clean_history(handle)
                        path=ARTIFACTS/f'binary-histories/{f.queue}-{index}.bin';f.call('history',handle,file=path);histories.append((path,handle['workflow_id'],f.queue,f.env.copy()))
                    worker.terminate();assert worker.wait(timeout=20)==0
                print(label,profile,packed,'NEXUS_FAILURE_LIVE_OK',flush=True)
    for path,identity,queue,env in histories:
        result=subprocess.run([str(BINARY),'replay',str(path),identity,queue],cwd=ROOT,env=env,capture_output=True,text=True,timeout=40);assert result.returncode==0,result.stderr
    return len(histories)

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary;report={'passed':False,'cases':[]}
    try:
        result=subprocess.run([str(BINARY),'nexus-failure-validation'],cwd=ROOT,capture_output=True,text=True,timeout=40)
        assert result.returncode==0 and result.stdout.strip()=='NEXUS_FAILURE_VALIDATION_OK',result.stderr
        report['histories']=exercise();assert report['histories']==84
        report['cases'].append('Captured Activity/timeout/child roots normalized to valid Nexus application failures with typed causes; both servers, seven converter profiles, packed/unpacked codecs, query/Update scope rebinding and dedupe, worker replacement, independent wire checks and 84 offline histories')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

"""Thrown workflow/Activity/Nexus cancellation errors retain typed details."""
RESULT_REPORT='cancellation-details-results.json'
import argparse,base64,json,shutil,subprocess,tempfile
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture
from typed_details_wire import details,wire_payload,unpack_payloads
BINARY=ARTIFACTS/'tools/v-typed-values'

def invoke(f,role,*args):
    p=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(p.stdout+p.stderr)
    assert p.returncode==0,p.stderr
    return 'OK' if p.stdout.strip()=='OK' else json.loads(p.stdout)

def check_details(payloads,profile,handle,scope,endpoint):
    payloads=[wire_payload(base64.b64decode(p['data'])) if base64.b64decode(p['metadata']['encoding'])==b'binary/test-envelope' else p for p in payloads]
    payloads=unpack_payloads(payloads)
    return details(payloads,profile,handle,scope,packed_array=False,endpoint=endpoint,operation='cancel')

def inspect_failure(failure,profile,handle,scope,endpoint):
    count=0
    if 'canceledFailureInfo' in failure:
        count+=check_details(failure['canceledFailureInfo']['details']['payloads'],profile,handle,scope,endpoint)
    if failure.get('cause'):count+=inspect_failure(failure['cause'],profile,handle,scope,endpoint)
    return count

def exercise(profiles=('default','prefix','context','opaque','binary-prefix','proto-binary','proto-json'),packing=(False,True),cache=2):
    histories=[];direct=0
    for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
        for profile in profiles:
            for packed in packing:
                with tempfile.TemporaryDirectory(prefix='cancel-details-',dir=ARTIFACTS) as directory,TemporalFixture(f'cancel-details-{label}-{profile}-{packed}',server_binary=server) as f:
                    f.env.update(V_SDK_DETAILS_TOKEN_DIR=directory,V_SDK_DETAILS_ENVELOPE='false' if profile=='default' else 'true',V_SDK_DETAILS_PACK=str(packed).lower(),V_SDK_EXAMPLE_CODEC='' if profile in ('default','opaque','proto-binary','proto-json') else profile,V_SDK_EXAMPLE_FAILURE_CONVERTER='encoded',V_SDK_DETAILS_PROTO={'proto-binary':'binary','proto-json':'json'}.get(profile,''),V_SDK_DETAILS_OPAQUE='true' if profile=='opaque' else 'false',TEMPORAL_MAX_CACHED_WORKFLOWS=str(cache))
                    worker=f.launch(BINARY,'before',('worker',))
                    endpoint=f.queue+'-endpoint'
                    subprocess.run([str(CLI),'operator','nexus','endpoint','create','--address',f.address,'--name',endpoint,'--target-namespace','default','--target-task-queue',f.queue],capture_output=True,text=True,check=True,timeout=20)
                    activity=invoke(f,'start',f.queue+'-activities','cancel-activities')
                    thrown=invoke(f,'start',f.queue+'-throw','cancel-error')
                    nexus=invoke(f,'start-cancel-nexus-workflow',f.queue+'-nexus',endpoint)
                    f.wait_for(lambda:all((Path(directory)/(activity['run_id']+'-'+local)).exists() for local in ('false','true')))
                    invoke(f,'cancel-all',json.dumps(activity))
                    try:
                        f.wait_for(lambda:invoke(f,'query',json.dumps(activity))['ready'])
                        assert invoke(f,'result',json.dumps(nexus))['status']=='completed'
                    except Exception:
                        if shutil.which('sample'):subprocess.run(['sample',str(worker.pid),'1','1','-file',str(ARTIFACTS/f'logs/{f.name}-worker-sample.txt')],capture_output=True,text=True,timeout=10)
                        for name,handle in [('activity',activity),('nexus',nexus)]:
                            (ARTIFACTS/f'logs/{f.name}-{name}-failure-history.json').write_text(json.dumps(f.history(handle),indent=2))
                        raise
                    invoke(f,'query-failure',json.dumps(activity));invoke(f,'update-failure',json.dumps(activity))
                    worker.kill();assert worker.wait(timeout=10)==-9
                    worker=f.launch(BINARY,'after',('worker',))
                    invoke(f,'query-failure',json.dumps(activity));invoke(f,'update-failure',json.dumps(activity))
                    invoke(f,'finish',json.dumps(activity));invoke(f,'finish',json.dumps(thrown))
                    assert invoke(f,'result',json.dumps(activity))['status']=='completed'
                    assert invoke(f,'result',json.dumps(thrown))['status']=='canceled'
                    if label=='1-33':
                        operation=invoke(f,'start-cancel-nexus',f.queue+'-direct',endpoint)
                        assert invoke(f,'cancel-nexus-result',json.dumps(operation))=='OK';direct+=1
                    for mode,handle in [('activity',activity),('throw',thrown),('nexus',nexus)]:
                        history=f.history(handle);seen=0;updates=0
                        for event in history['events']:
                            for key,attributes in event.items():
                                if key=='activityTaskCanceledEventAttributes':seen+=check_details(attributes['details']['payloads'],profile,handle,'activity',endpoint)
                                if key=='workflowExecutionCanceledEventAttributes':seen+=check_details(attributes['details']['payloads'],profile,handle,'workflow',endpoint)
                                if key=='workflowExecutionUpdateCompletedEventAttributes':seen+=inspect_failure(attributes['outcome']['failure'],profile,handle,'workflow',endpoint);updates+=1
                                if key=='nexusOperationCanceledEventAttributes':seen+=inspect_failure(attributes['failure'],profile,handle,'nexus',endpoint)
                        assert seen=={'activity':2,'throw':1,'nexus':1}[mode],(mode,seen)
                        assert updates==(1 if mode=='activity' else 0)
                        f.assert_clean_history(handle)
                        path=ARTIFACTS/f'binary-histories/{f.queue}-{mode}.bin';f.call('history',handle,file=path);histories.append((path,handle['workflow_id'],f.queue,f.env.copy()))
                    worker.terminate();assert worker.wait(timeout=20)==0
                print(label,profile,packed,'CANCELLATION_DETAILS_LIVE_OK',flush=True)
    for history,identity,queue,env in histories:
        p=subprocess.run([str(BINARY),'replay',str(history),identity,queue],cwd=ROOT,env=env,capture_output=True,text=True,timeout=40);assert p.returncode==0,p.stderr
    return len(histories),direct

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary;report={'passed':False,'cases':[]}
    try:
        p=subprocess.run([str(BINARY),'cancellation-validation'],cwd=ROOT,capture_output=True,text=True,timeout=40);assert p.returncode==0 and p.stdout.strip()=='CANCELLATION_DETAILS_VALIDATION_OK',p.stderr
        report['histories'],report['standalone_nexus']=exercise()
        assert report['histories']==84 and report['standalone_nexus']==14
        report['cases'].append('Five converter policies retained typed cancellation error details across scopes while pause/reset stayed retryable; both servers, seven profiles and packed/unpacked codecs preserved thrown workflow, local/remote Activity and Nexus cancellation details through query/Update rebinding, dedupe, worker replacement, independent wire checks and 84 offline histories; 14 supporting-server standalone Nexus cancellations')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',choices=(RESULT_REPORT,'cancellation-details-sanitized-results.json'),default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

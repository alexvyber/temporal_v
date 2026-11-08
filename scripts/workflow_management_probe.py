"""Workflow description, reset deduplication/post-signals and run-specific deletion."""
RESULT_REPORT = 'workflow-management-results.json'
import argparse, hashlib, json, signal, subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture
BINARY = ARTIFACTS / 'tools/v-management'

def invoke(f, role, *args):
    response = subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(response.stdout+response.stderr)
    assert response.returncode == 0, (role,response.stderr)
    return 'OK' if response.stdout.strip()=='OK' else json.loads(response.stdout)

def deleted(f, handle):
    result=subprocess.run([str(BINARY),'missing',json.dumps(handle)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
    if result.returncode==0:
        assert result.stdout.strip()=='OK',result.stdout
        return True
    assert result.stderr.strip()=='deleted workflow is still described',result.stderr
    return False

def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary
    report={'passed':False,'histories':0,'cases':[],'storage_fixtures':[]}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for codec in ('','prefix','context'):
                for storage in (False,True):
                    with TemporalFixture(f'management-{version}-{codec or "default"}-{storage}',server_binary=server,server_args=('--dynamic-config-value','history.transferProcessorUpdateAckInterval="100ms"')) as f:
                        directory=ARTIFACTS/'management-storage'/f.queue
                        if storage:directory.mkdir(parents=True)
                        f.env.update(V_SDK_EXAMPLE_CODEC=codec,V_SDK_EXAMPLE_STORAGE_DIR=str(directory) if storage else '')
                        worker=f.launch(BINARY,'before',('worker',))
                        handles=invoke(f,'start')
                        for handle in handles:
                            assert invoke(f,'check',json.dumps(handle))=='OK'
                            assert invoke(f,'describe',json.dumps(handle))=='OK'
                        original=handles[0]
                        assert invoke(f,'invalid-reset',json.dumps(original))=='OK'
                        assert invoke(f,'finish',json.dumps(original))=='OK'
                        events=f.history(original)['events']
                        event=next(item for item in events if 'WORKFLOW_TASK_COMPLETED' in item['eventType'])
                        event_id=event['eventId']
                        request_id=f.queue+'-reset'
                        post=invoke(f,'post-reset',json.dumps(original),event_id,request_id+'-post')
                        post_supported=not post.get('unsupported',False)
                        if post_supported:
                            reset=post
                            reset_role='post-reset'
                            reset_request=request_id+'-post'
                        else:
                            reset=invoke(f,'reset',json.dumps(original),event_id,request_id)
                            reset_role='reset'
                            reset_request=request_id
                        assert reset['workflow_id']==original['workflow_id'] and reset['run_id']!=original['run_id']
                        first_reset=reset
                        duplicate=invoke(f,reset_role,json.dumps(original),event_id,reset_request)
                        reset_deduplicated=duplicate==reset
                        superseded=[]
                        if not reset_deduplicated:
                            superseded=[first_reset]
                            reset=duplicate
                        if not post_supported:assert invoke(f,'check',json.dumps(reset))=='OK'
                        assert invoke(f,'reset-state',json.dumps(reset))=='OK'
                        assert invoke(f,'describe',json.dumps(reset))=='OK'
                        if storage:
                            before=(directory/'retrievals.log').read_bytes()
                            assert invoke(f,'raw-describe',json.dumps(reset))=='OK'
                            assert (directory/'retrievals.log').read_bytes()==before
                        worker.kill();worker.wait(timeout=15)
                        replacement=f.launch(BINARY,'after',('worker',))
                        assert invoke(f,'reset-state',json.dumps(reset))=='OK'
                        for handle in handles[1:]+[reset]:assert invoke(f,'finish',json.dumps(handle))=='OK'
                        histories=[]
                        for handle in handles+superseded+[reset]:
                            history=f.history(handle)
                            for item in history['events']:
                                failure=item.get('workflowTaskFailedEventAttributes')
                                if failure:
                                    expected_reset='RESET_WORKFLOW' in failure.get('cause','')
                                    forced_by_server=failure.get('cause')=='WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND' and failure.get('identity')=='history-service'
                                    assert expected_reset or forced_by_server,item
                            path=f.histories/(handle['workflow_id']+'-'+handle['run_id']+'.bin')
                            invoke(f,'history',json.dumps(handle),path)
                            histories.append((path,handle['workflow_id'],f.queue,dict(f.env)))
                        # Delete the original run, leaving its reset successor intact.
                        assert invoke(f,'delete',json.dumps(original))=='OK'
                        assert invoke(f,'describe',json.dumps(reset))=='OK'
                        f.wait_for(lambda:deleted(f,original),timeout=90)
                        for handle in handles[1:]+superseded+[reset]:
                            assert invoke(f,'delete',json.dumps(handle))=='OK'
                            f.wait_for(lambda:deleted(f,handle))
                        replacement.send_signal(signal.SIGTERM);replacement.wait(timeout=30)
                        assert replacement.returncode==0
                    before={str(p.relative_to(directory)):hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('*') if p.is_file() and p.name!='retrievals.log'} if storage else {}
                    for path,identity,queue,environment in histories:
                        result=subprocess.run([str(BINARY),'replay',str(path),identity,queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
                        assert result.returncode==0 and result.stdout.strip()=='OK',result.stderr
                        report['histories']+=1
                    if storage:
                        after={str(p.relative_to(directory)):hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('*') if p.is_file() and p.name!='retrievals.log'}
                        assert before==after,'offline replay uploaded or modified stored payloads'
                        report['storage_fixtures'].append({'directory':str(directory),'files':after})
                    report['cases'].append(f'{version}/{codec or "default"}/storage={storage}: typed/raw description, invalid reset, reset deduplication={reset_deduplicated}, excluded old signals, post-reset signal support={post_supported}, typed signal/header, replacement, run-specific deletion and offline replay')
                    print(report['cases'][-1],flush=True)
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

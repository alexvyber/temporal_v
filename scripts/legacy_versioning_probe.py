"""Legacy compatibility sets, matching-safe eager fallback and restart/replay."""
RESULT_REPORT = 'legacy-versioning-results.json'
import argparse
from pathlib import Path
import json
import subprocess
from integration import TemporalFixture, ARTIFACTS, ROOT
BINARY=ARTIFACTS/'tools/v-legacy'
def invoke(fixture,mode,*arguments):
    result=subprocess.run([str(BINARY),mode,*arguments],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=90)
    (ARTIFACTS/f'logs/{fixture.name}-command-{mode}.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,result.stderr
    return result.stdout.strip()
def identity(fixture,handle):
    events=fixture.history(handle)['events']
    completed=[event['workflowTaskCompletedEventAttributes'] for event in events if 'workflowTaskCompletedEventAttributes' in event]
    return completed[-1]['identity'] if completed else ''
def route(fixture,handle,target):
    def ready():
        fixture.call('signal',handle,name='add',input='0')
        # Completed-task history proves which worker processed the signal.
        # A separate query can time out while matching propagates a new build
        # even though ordinary workflow tasks are routing successfully.
        return identity(fixture,handle)==target
    fixture.wait_for(ready,timeout=20)

def exercise(label,server):
    replays=[]
    with TemporalFixture('legacy-versioning-' + label, server_binary=server, server_args=('--dynamic-config-value','frontend.workerVersioningDataAPIs=true','--dynamic-config-value','frontend.workerVersioningWorkflowAPIs=true')) as fixture:
        assert invoke(fixture,'validation') == 'VALIDATION_OK'
        invoke(fixture,'new','legacy-a')
        a=fixture.launch(BINARY,'a',('worker',),env=dict(fixture.env,TEMPORAL_BUILD_ID='legacy-a',TEMPORAL_IDENTITY='legacy-a'))
        first=fixture.start('first','v.legacy',{})
        route(fixture,first,'legacy-a')
        invoke(fixture,'compatible','legacy-b','legacy-a')
        b=fixture.launch(BINARY,'b',('worker',),env=dict(fixture.env,TEMPORAL_BUILD_ID='legacy-b',TEMPORAL_IDENTITY='legacy-b'))
        route(fixture,first,'legacy-b')
        invoke(fixture,'new','legacy-c')
        c=fixture.launch(BINARY,'c',('worker',),env=dict(fixture.env,TEMPORAL_BUILD_ID='legacy-c',TEMPORAL_IDENTITY='legacy-c'))
        second=fixture.start('second','v.legacy',{})
        route(fixture,second,'legacy-c')
        route(fixture,first,'legacy-b')
        invoke(fixture,'promote-set','legacy-a')
        invoke(fixture,'promote-build','legacy-a')
        route(fixture,first,'legacy-a')
        invoke(fixture,'merge','legacy-a','legacy-c')
        route(fixture,second,'legacy-a')
        sets=json.loads(invoke(fixture,'describe'))['major_version_sets']
        assert sets[-1]['build_ids'][-1]=='legacy-a',sets
        reachability=json.loads(invoke(fixture,'reachability'))
        assert reachability['build_id_reachability'][0]['build_id']=='legacy-a',reachability
        for worker in (a,b,c):worker.kill();assert worker.wait(timeout=10)==-9
        a=fixture.launch(BINARY,'a-after',('worker',),env=dict(fixture.env,TEMPORAL_BUILD_ID='legacy-a',TEMPORAL_IDENTITY='legacy-a'))
        for handle in (first,second):
            route(fixture,handle,'legacy-a')
            fixture.call('signal',handle,name='finish')
            assert fixture.call('result',handle)['status']=='completed'
            fixture.assert_clean_history(handle)
            path=ARTIFACTS/f'binary-histories/{fixture.name}-{handle["workflow_id"]}.bin'
            fixture.call('history',handle,file=path)
            replays.append((path,handle['workflow_id'],fixture.queue,fixture.env.copy()))
        a.terminate();assert a.wait(timeout=20)==0

    with TemporalFixture('legacy-eager-' + label, server_binary=server, server_args=('--dynamic-config-value','frontend.workerVersioningDataAPIs=true','--dynamic-config-value','frontend.workerVersioningWorkflowAPIs=true')) as fixture:
        invoke(fixture,'new','legacy-a')
        c=fixture.launch(BINARY,'remote-c',('worker',),env=dict(fixture.env,TEMPORAL_BUILD_ID='legacy-c',TEMPORAL_IDENTITY='remote-c'))
        output=invoke(fixture,'eager')
        assert 'ORDINARY_IDENTITY=remote-c' in output and 'INLINE=false' in output,output
        handle={'workflow_id':fixture.queue+'-eager'}
        assert identity(fixture,handle)=='remote-c'
        fixture.assert_clean_history(handle)
        path=ARTIFACTS/f'binary-histories/{fixture.name}.bin'
        fixture.call('history',handle,file=path)
        replays.append((path,handle['workflow_id'],fixture.queue,fixture.env.copy()))
        c.terminate();assert c.wait(timeout=20)==0

    for path,workflow_id,queue,environment in replays:
        replay=subprocess.run([str(BINARY),'replay',str(path),workflow_id,queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=40)
        (ARTIFACTS/f'logs/{path.stem}-offline.log').write_text(replay.stdout+replay.stderr)
        assert replay.returncode==0,replay.stderr

def main(binary,report_name):
    global BINARY
    BINARY=binary
    report={'passed':False,'cases':[]}
    try:
        for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            exercise(label,server)
            report['cases'].append(label+': compatible takeover, incompatible defaults, set/build promotion, merge, SIGKILL/replacement and offline replay passed; invalid typed inputs failed before RPC')
            report['cases'].append(label+': after an ordinary start proved default C routing, shared legacy A declined eager reservation and the eager-requested workflow completed on C through server matching')
        report['passed']=True
    finally:
        (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--binary',type=Path,default=BINARY)
    parser.add_argument('--report',choices=(RESULT_REPORT,'legacy-versioning-sanitized-results.json'),default=RESULT_REPORT)
    args=parser.parse_args()
    main(args.binary,args.report)

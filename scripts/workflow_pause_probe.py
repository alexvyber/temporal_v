"""Namespace-gated workflow pause/unpause, queued signals/cancellation and replay."""
RESULT_REPORT = 'workflow-pause-results.json'
import argparse,json,signal,subprocess,time
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
from restart_probe import counts
BINARY=ARTIFACTS/'tools/v-workflow-pause'

def invoke(f,role,*args):
    response=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(response.stdout+response.stderr)
    assert response.returncode==0,(role,response.stderr)
    return 'OK' if response.stdout.strip()=='OK' else json.loads(response.stdout)

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary;report={'passed':False,'histories':0,'cases':[]}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for codec,enabled in [('',False),('',True),('prefix',True),('context',True)]:
                flags=('--dynamic-config-value','frontend.WorkflowPauseEnabled=true') if enabled else ()
                with TemporalFixture(f'workflow-pause-{version}-{codec or "default"}-{enabled}',server_binary=server,server_args=flags) as f:
                    f.env['V_SDK_EXAMPLE_CODEC']=codec
                    worker=f.launch(BINARY,'before',('worker',))
                    handles=invoke(f,'start')
                    for handle in handles:assert invoke(f,'check',json.dumps(handle))=='OK'
                    if enabled:
                        baseline={}
                        for index,handle in enumerate(handles[:5]):
                            assert invoke(f,'pause',json.dumps(handle),f.queue+str(index))=='OK'
                            assert invoke(f,'paused',json.dumps(handle))=='OK'
                            baseline[handle['run_id']]=counts(f.history(handle))['workflowtaskstarted']
                        assert invoke(f,'pause',json.dumps(handles[0]),f.queue+'0')=='OK'
                        for handle in handles[:3]:assert invoke(f,'send-finish',json.dumps(handle))=='OK'
                        for handle in handles[3:5]:assert invoke(f,'cancel',json.dumps(handle))=='OK'
                        assert invoke(f,'finish',json.dumps(handles[5]))=='OK'
                        time.sleep(0.2)
                        for handle in handles[:5]:
                            observed=counts(f.history(handle))
                            assert observed['workflowexecutioncompleted']==observed['workflowexecutioncanceled']==0,observed
                            assert observed['workflowtaskstarted']==baseline[handle['run_id']],observed
                        worker.kill();worker.wait(timeout=15)
                        worker=f.launch(BINARY,'after',('worker',))
                        for index,handle in enumerate(handles[:5]):
                            assert invoke(f,'paused',json.dumps(handle))=='OK'
                            assert invoke(f,'unpause',json.dumps(handle),f.queue+'-unpause-'+str(index))=='OK'
                            result=invoke(f,'result',json.dumps(handle))
                            assert result['status']==('completed' if index<3 else 'canceled'),result
                    else:
                        assert invoke(f,'pause-disabled',json.dumps(handles[0]))=='OK'
                        for handle in handles:assert invoke(f,'finish',json.dumps(handle))=='OK'
                    histories=[]
                    for handle in handles:
                        history=f.history(handle)
                        for event in history['events']:
                            failure=event.get('workflowTaskFailedEventAttributes')
                            if failure:assert failure.get('cause')=='WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND' and failure.get('identity')=='history-service',event
                        path=f.histories/(handle['run_id']+'.bin');invoke(f,'history',json.dumps(handle),path)
                        histories.append((path,handle['workflow_id'],f.queue,dict(f.env)))
                    worker.send_signal(signal.SIGTERM);worker.wait(timeout=30);assert worker.returncode==0
                for path,identity,queue,environment in histories:
                    replay=subprocess.run([str(BINARY),'replay',str(path),identity,queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stderr
                    report['histories']+=1
                report['cases'].append(f'{version}/{codec or "default"}/enabled={enabled}: ' + ('pause metadata, duplicate pause, held signal/cancellation, unrelated workflow progress, worker replacement, unpause and offline replay' if enabled else 'disabled namespace rejected pause without affecting workflow execution; all six histories replayed offline'))
                print(report['cases'][-1],flush=True)
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

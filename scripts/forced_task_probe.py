"""Explicit task failure discards commands and wins over later callback outcomes."""
RESULT_REPORT='forced-task-results.json'
import argparse
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS,ROOT,TemporalFixture,counts


def main(binary,report_name):
    report={'passed':False,'cases':[],'histories':0}
    try:
        for version,server in [('1-32','temporal'),('1-33','temporal-protocol')]:
            for codec in ['default','prefix','context']:
                for stored in [False,True]:
                    with TemporalFixture('forced-'+version+'-'+codec+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
                        directory=fixture.histories/fixture.queue;directory.mkdir()
                        fixture.env.update(V_SDK_EXAMPLE_CODEC='' if codec=='default' else codec,V_PANIC_FAIL_WORKFLOW='true',V_FORCED_FIXED='false')
                        if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory))
                        worker=fixture.launch(binary,'before',('worker',))
                        healthy=fixture.start('healthy','v.panic',{'mode':'wait'})
                        fixture.wait_for(lambda:fixture.call('query',healthy,name='state')['data']=='0')
                        handles=[]
                        for mode in ['start','after-close','error','panic','signal','update']:
                            handle=fixture.start(mode,'v.forced-task',{'mode':mode});handles.append(handle)
                            if mode=='signal':fixture.call('signal',handle,name='force')
                            if mode=='update':fixture.call('update',handle,name='force',update_id='force',wait_ms=100,success=False)
                            fixture.wait_for(lambda:counts(fixture.history(handle))['workflowtaskfailed']>=1)
                            history=fixture.history(handle);events=counts(history)
                            assert events['workflowexecutionfailed']==0 and events['workflowexecutioncompleted']==0,history
                            assert events['timerstarted']==0 and events['activitytaskscheduled']==0,history
                            failures=[event['workflowTaskFailedEventAttributes']['failure'] for event in history['events'] if 'workflowTaskFailedEventAttributes' in event]
                            assert failures and all(item['message']=='forced task failure' for item in failures),failures
                        assert worker.poll() is None
                        fixture.call('signal',healthy,name='finish');assert fixture.call('result',healthy)['status']=='completed'
                        worker.kill();worker.wait(timeout=10)
                        fixture.env['V_FORCED_FIXED']='true';worker=fixture.launch(binary,'after',('worker',))
                        for handle in handles:
                            result=fixture.call('result',handle);assert result['status']=='completed' and result['data']=='"repaired"',result
                            assert counts(fixture.history(handle))['workflowexecutionfailed']==0
                        outcome=fixture.call('update',handles[-1],name='force',update_id='force');assert outcome['status']=='completed' and outcome['data']=='"repaired"',outcome
                        for index,handle in enumerate([healthy,*handles]):
                            file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
                            replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                            assert replay.returncode==0,replay.stdout+replay.stderr
                            report['histories']+=1
                        worker.terminate();assert worker.wait(timeout=20)==0
                        report['cases'].append(f'{version}/{codec}/storage={stored}: six forced failures discarded commands; first request won over completion, application error and panic despite fail-workflow policy; signals/Updates, unrelated progress, SIGKILL/repair and seven offline histories passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

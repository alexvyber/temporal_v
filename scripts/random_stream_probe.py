"""Named RNG state survives replacement, new independent streams and server reset."""
RESULT_REPORT='random-stream-results.json'
import argparse
import base64
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS,ROOT,TemporalFixture,counts


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0}
    try:
        profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['','prefix','context'] for stored in [False,True]]
        if quick:profiles=profiles[:1]
        for version,server,codec,stored in profiles:
            with TemporalFixture('random-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir()
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_RANDOM_NOISE='false')
                if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory))
                worker=fixture.launch(binary,'before',('worker',))
                handle=fixture.start('original','v.random-stream',{})
                def state(target):return json.loads(fixture.call('query',target,name='state')['data'])
                def ready(target,size):fixture.wait_for(lambda:len(state(target))==size)
                ready(handle,1);original=state(handle)
                fixture.call('signal',handle,name='sample');ready(handle,2);baseline=state(handle)
                worker.kill();worker.wait(timeout=10)
                fixture.env['V_RANDOM_NOISE']='true';worker=fixture.launch(binary,'noise',('worker',))
                assert state(handle)==baseline
                fixture.call('signal',handle,name='sample');ready(handle,3)
                fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['status']=='completed'
                history=fixture.history(handle)
                event=next(item for item in history['events'] if 'workflowTaskCompletedEventAttributes' in item)
                reset=subprocess.run([str(binary),'random-reset',json.dumps(handle),str(event['eventId'])],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=20)
                assert reset.returncode==0,reset.stdout+reset.stderr
                replacement=json.loads(reset.stdout)
                assert replacement['workflow_id']==handle['workflow_id'] and replacement['run_id']!=handle['run_id']
                ready(replacement,1);assert state(replacement)==original
                fixture.call('signal',replacement,name='sample');ready(replacement,2)
                reset_samples=state(replacement)
                for key in ['alpha','beta','default_value','uuid']:
                    assert reset_samples[1][key]!=baseline[1][key],(key,baseline,reset_samples)
                worker.kill();worker.wait(timeout=10);worker=fixture.launch(binary,'after-reset',('worker',))
                assert state(replacement)==reset_samples
                fixture.call('signal',replacement,name='sample');ready(replacement,3)
                fixture.call('signal',replacement,name='finish');assert fixture.call('result',replacement)['status']=='completed'
                for index,target in enumerate([handle,replacement]):
                    value=fixture.history(target)
                    failures=[event['workflowTaskFailedEventAttributes'] for event in value['events'] if 'workflowTaskFailedEventAttributes' in event]
                    expected_failures=1 if target==replacement else 0
                    assert len(failures)==expected_failures,[(failure.get('cause'),failure.get('failure',{}).get('message')) for failure in failures]
                    assert all(failure.get('cause')=='WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW' for failure in failures)
                    # The large random snapshot actually traversed storage when enabled.
                    scheduled=next(item['activityTaskScheduledEventAttributes'] for item in value['events'] if 'activityTaskScheduledEventAttributes' in item)
                    payload=scheduled['input']['payloads'][0]
                    metadata={key:base64.b64decode(value) for key,value in payload.get('metadata',{}).items()}
                    assert (metadata.get('messageType')==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,metadata
                    file=directory/(str(index)+'.bin');fixture.call('history',target,file=file)
                    replay=subprocess.run([str(binary),'replay',str(file),target['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0,replay.stdout+replay.stderr
                    report['histories']+=1
                worker.terminate();assert worker.wait(timeout=20)==0
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: shared handles, independent noise/default RNG, SIGKILL/replacement, reset reseeding of old handles and fresh lookup, second replacement and two offline histories passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

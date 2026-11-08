"""Dynamic workflow fallback receives opaque raw start arguments."""
RESULT_REPORT='raw-workflow-results.json'
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS, ROOT, TemporalFixture, counts


def main(binary, report_name, quick=False):
    report={'passed':False,'cases':[],'histories':0}
    try:
        profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')]
                  for codec in ['', 'prefix', 'context'] for stored in [False,True]]
        if quick: profiles=profiles[:1]
        for version,server,codec,stored in profiles:
            with TemporalFixture('raw-workflow-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,
                                 server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir()
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec)
                if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'))
                worker=fixture.launch(binary,'before',('worker',))
                started=subprocess.run([str(binary),'raw-workflow-start'],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=30)
                assert started.returncode==0,started.stdout+started.stderr
                handles=json.loads(started.stdout)
                def state(handle):return json.loads(fixture.call('query',handle,name='state',rpc_ms=20000)['data'])
                expected={}
                for label in ['empty','mixed','named']:
                    fixture.wait_for(lambda:state(handles[label]) is not None)
                    value=state(handles[label]);expected[label]=value
                    assert value['kind']==('named' if label=='named' else 'dynamic'),value
                    assert len(value['values'])=={'empty':0,'mixed':5,'named':1}[label],value
                mixed=expected['mixed']['values']
                assert mixed[0]['digest']==hashlib.sha256(b'\xff\0\x80'+b'x'*320000).hexdigest(),mixed
                assert mixed[0]['encoding']=='application/x-temporal-v-raw-activity',mixed
                assert mixed[4]['message_type']=='temporal.api.common.v1.WorkflowExecution',mixed
                fixture.wait_for(lambda:counts(fixture.history(handles['bad']))['workflowtaskfailed']>0)
                assert counts(fixture.history(handles['bad']))['workflowexecutionfailed']==0
                worker.kill();assert worker.wait(timeout=10)==-9
                fixture.env['V_RAW_WORKFLOW_REPAIR']='true'
                worker=fixture.launch(binary,'after',('worker',))
                for label in ['empty','mixed','named']:assert state(handles[label])==expected[label]
                repaired=state(handles['bad']);assert repaired['kind']=='dynamic' and repaired['values']==mixed,repaired
                for label,handle in handles.items():
                    fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['status']=='completed'
                    history=fixture.history(handle)
                    if label=='mixed':
                        first=next(event['workflowExecutionStartedEventAttributes'] for event in history['events'] if 'workflowExecutionStartedEventAttributes' in event)['input']['payloads'][0]
                        reference=base64.b64decode(first.get('metadata',{}).get('messageType',''))
                        assert (reference==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,first
                    if label!='bad':assert counts(history)['workflowtaskfailed']==0
                    file=directory/(label+'.bin');fixture.call('history',handle,file=file)
                    replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
                    report['histories']+=1
                worker.terminate();assert worker.wait(timeout=20)==0
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: empty/mixed owned raw workflow inputs, named converter priority/rejection, failed-task repair by fallback registration, concurrent progress, SIGKILL/replacement and four offline histories passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

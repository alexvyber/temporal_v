"""Runtime context scopes isolate branches and survive command deferral/replay."""
RESULT_REPORT='context-values-results.json'
import argparse,base64,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture,counts

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('context-values-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir()
    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_PANIC_FAIL_WORKFLOW='false',V_PANIC_FIXED='true')
    if stored:fixture.env.update(V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'),V_PANIC_STORAGE_DRIVER='true')
    worker=fixture.launch(binary,'before',('worker',))
    handles=[fixture.start('scope-'+str(index),'v.context-values',{}) for index in range(8)]
    panic=fixture.start('panic','v.context-values',{'mode':'panic'})
    fixture.wait_for(lambda:counts(fixture.history(panic))['workflowtaskfailed']>0)
    assert 'scoped context panic' in json.dumps(fixture.history(panic))
    fixture.wait_for(lambda:any('timerStartedEventAttributes' in event and event['timerStartedEventAttributes']['timerId']=='7' for event in fixture.history(handles[0])['events']))
    before=fixture.history(handles[0]);assert not any(event.get('timerFiredEventAttributes',{}).get('timerId')=='7' for event in before['events']),before
    worker.kill();assert worker.wait(timeout=10)==-9
    fixture.env['V_CONTEXT_PANIC_FIXED']='true'
    worker=fixture.launch(binary,'after',('worker',))
    def ready(handle):return json.loads(fixture.call('query',handle,name='state')['data'])['ready']=='true'
    for handle in handles:fixture.wait_for(lambda:ready(handle))
    for handle in handles:fixture.call('signal',handle,name='finish')
    for handle in handles:
     assert fixture.call('result',handle)['data']=='"context-ok"'
     history=fixture.history(handle);assert counts(history)['workflowtaskfailed']==0,history
     assert counts(history)['timerstarted']==7,counts(history)
     scheduled=next(event['activityTaskScheduledEventAttributes'] for event in history['events'] if 'activityTaskScheduledEventAttributes' in event)
     first=scheduled['input']['payloads'][0]
     assert (base64.b64decode(first.get('metadata',{}).get('messageType',''))==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,first
     assert 'context_values' not in json.dumps(history) and 'ContextCapture' not in json.dumps(history)
    assert fixture.call('result',panic)['data']=='"panic-repaired"'
    assert not counts(fixture.history(panic))['workflowexecutionfailed']
    worker.terminate();assert worker.wait(timeout=20)==0
    for index,handle in enumerate(handles+[panic]):
     file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     (directory/(str(index)+'-replay.log')).write_text(replay.stdout+replay.stderr)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     report['histories']+=1
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: eight concurrent executions, typed/key isolation, nested shadow/error restoration, captured continuation, per-command interceptor snapshots, durable local retry, JSON omission, scope panic task repair, SIGKILL/replacement and offline replay passed')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

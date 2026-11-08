"""Deterministic condition waits, cancellation and replay on real Temporal."""
RESULT_REPORT='conditions-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture,counts

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('conditions-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir()
    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_EVENT_GROUPS='true',V_PANIC_FAIL_WORKFLOW='false')
    if stored:fixture.env.update(V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'),V_PANIC_STORAGE_DRIVER='true')
    worker=fixture.launch(binary,'before',('worker',))
    handles=[fixture.start('wait-'+str(index),'v.conditions',{}) for index in range(4)]
    panic=fixture.start('panic','v.conditions',{'mode':'panic'})
    abandoned=fixture.start('abandon','v.conditions',{'mode':'abandon'})
    failed=fixture.start('failure','v.conditions',{'mode':'failure'})
    assert fixture.call('result',abandoned)['data']=='"abandoned-ok"'
    failure=fixture.call('result',failed);assert failure['status']=='failed' and 'condition handler failed' in json.dumps(failure),failure
    fixture.wait_for(lambda:counts(fixture.history(panic))['workflowtaskfailed']>0)
    assert 'workflow condition predicate panic' in json.dumps(fixture.history(panic))
    def state(handle):return json.loads(fixture.call('query',handle,name='state')['data'])
    for handle in handles:
     fixture.wait_for(lambda:'cascade' in state(handle))
     initial=state(handle)
     assert initial=={'explicit':'condition_cancelled','immediate':'condition_satisfied','ready-over-cancel':'condition_satisfied','cascade':'condition_satisfied'},initial
    worker.kill();assert worker.wait(timeout=10)==-9
    fixture.env['V_CONDITION_PANIC_FIXED']='true'
    worker=fixture.launch(binary,'after',('worker',))
    for handle in handles:
     fixture.call('signal',handle,name='counter');fixture.call('signal',handle,name='counter')
     fixture.call('signal',handle,name='cancel-scope')
    for handle in handles:
     fixture.wait_for(lambda:len(state(handle))==9)
     progress=state(handle)
     assert progress['counter']=='condition_satisfied' and progress['captured']=='condition_satisfied'
     assert progress['scope']=='condition_cancelled' and progress['late']=='condition_cancelled' and progress['timeout']=='condition_timed_out',progress
     fixture.call('cancel',handle)
     fixture.wait_for(lambda:state(handle).get('root')=='condition_cancelled')
     assert 'shield' not in state(handle),'shielded condition inherited root cancellation'
    for handle in handles:fixture.call('signal',handle,name='shield')
    for handle in handles:
     result=fixture.call('result',handle);assert result['status']=='completed',result
     values=json.loads(result['data']);assert len(values)==11 and values['shield']=='condition_satisfied',values
     history=fixture.history(handle);assert counts(history)['workflowtaskfailed']==0,history
     def keys(event):return [marker.get('label',{}).get('id') for marker in event.get('eventGroupMarkers',[])]
     assert any(keys(event)==['condition-timeout'] for event in history['events'] if 'timerStartedEventAttributes' in event)
     assert any(keys(event)==['late-condition'] for event in history['events'] if 'timerCanceledEventAttributes' in event)
     assert any(keys(event)==['condition'] for event in history['events'] if 'timerStartedEventAttributes' in event),'captured condition group missing'
    assert fixture.call('result',panic)['data']=='"predicate-repaired"'
    assert counts(fixture.history(abandoned))['timerstarted']==0,'unpolled abandoned condition created a timer'
    worker.terminate();assert worker.wait(timeout=20)==0
    fixture.env['V_EVENT_GROUPS']='false'
    plain_worker=fixture.launch(binary,'plain',('worker',));plain=fixture.start('plain','v.conditions',{'mode':'plain'})
    assert fixture.call('result',plain)['data']=='"plain-ok"'
    assert all(not event.get('eventGroupMarkers') for event in fixture.history(plain)['events'])
    plain_worker.terminate();assert plain_worker.wait(timeout=20)==0
    plain_file=directory/'plain.bin';fixture.call('history',plain,file=plain_file)
    plain_replay=subprocess.run([str(binary),'replay',str(plain_file),plain['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
    assert plain_replay.returncode==0 and plain_replay.stdout.strip()=='OK',plain_replay.stdout+plain_replay.stderr
    report['histories']+=1
    fixture.env['V_EVENT_GROUPS']='true'
    for index,handle in enumerate(handles+[panic,abandoned,failed]):
     file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     (directory/(str(index)+'-replay.log')).write_text(replay.stdout+replay.stderr)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     report['histories']+=1
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: four concurrent waits, immediate/cascade/captured conditions, typed scope reads, timeout, ready-over-cancel, scoped/root/shielded/idempotent cancellation and causes, nonexperimental execution, abandonment, handler failure, predicate panic repair, SIGKILL/replacement and offline replay passed')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

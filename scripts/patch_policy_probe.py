"""Live rollout policy never overrides cached or historical patch decisions."""
RESULT_REPORT='patch-policy-results.json'
import argparse,base64,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts

def patch_markers(history):
 result=[]
 for event in history['events']:
  marker=event.get('markerRecordedEventAttributes',{})
  if marker.get('markerName')!='core_patch':continue
  for details in marker['details'].values():
   for payload in details['payloads']:
    value=json.loads(base64.b64decode(payload['data']))
    if value.get('id','').startswith('policy-'):result.append(value)
 return result

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('patch-policy-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir();audit=directory/'audit';audit.mkdir()
    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_PATCH_POLICY='true',V_PATCH_POLICY_AUDIT=str(audit),V_PATCH_POLICY_STAGE='poison',V_PATCH_POLICY_CODE='legacy',V_PANIC_FAIL_WORKFLOW='false')
    if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
    created=subprocess.run([str(CLI),'operator','search-attribute','create','--address',fixture.address,'--name','VPatchPolicyKeyword','--type','Keyword'],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
    def start(label,cohort,mode='normal'):
     result=subprocess.run([str(binary),'patch-policy-start',label,cohort,mode],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=30)
     assert result.returncode==0,result.stdout+result.stderr
     return json.loads(result.stdout)
    def state(handle):return json.loads(fixture.call('query',handle,name='state')['data'])
    worker=fixture.launch(binary,'legacy',('worker',));legacy=start('legacy','allow')
    fixture.wait_for(lambda:state(legacy)['version']=='2');initial=state(legacy)
    assert initial=={'on':'false','off':'false','retired':'false','late_on':'false','late_off':'false','version':'2'},initial
    assert not list(audit.iterdir())
    worker.terminate();assert worker.wait(timeout=20)==0
    fixture.env.update(V_PATCH_POLICY_CODE='current',V_PATCH_POLICY_STAGE='baseline')
    worker=fixture.launch(binary,'baseline',('worker',))
    handles=[legacy];cohorts=['allow'];expected=[initial]
    for index,cohort in enumerate(['allow','deny','allow','deny']):
     handle=start('cohort-'+str(index),cohort);handles.append(handle);cohorts.append(cohort)
     fixture.wait_for(lambda:state(handle)['version']=='2');current=state(handle)
     assert current=={'on':str(cohort=='allow').lower(),'off':'false','retired':'true','late_on':'false','late_off':'false','version':'2'},current
     expected.append(current)
    before={p.name:p.read_text() for p in audit.iterdir()}
    assert len(before)==8 and all(len(value.splitlines())==1 for value in before.values()),before
    panic=start('panic','allow','panic')
    fixture.wait_for(lambda:counts(fixture.history(panic))['workflowtaskfailed']>0)
    assert 'patch policy panic' in json.dumps(fixture.history(panic))
    worker.kill();assert worker.wait(timeout=10)==-9
    fixture.env['V_PATCH_POLICY_STAGE']='poison';worker=fixture.launch(binary,'poison',('worker',))
    for handle,value in zip(handles,expected):assert state(handle)==value
    assert before=={p.name:p.read_text() for p in audit.iterdir()},'history called policy'
    worker.terminate();assert worker.wait(timeout=20)==0
    fixture.env['V_PATCH_POLICY_STAGE']='inverse';worker=fixture.launch(binary,'inverse',('worker',))
    for handle in handles:fixture.call('signal',handle,name='late')
    for handle,cohort,value in zip(handles,cohorts,expected):
     fixture.wait_for(lambda:len([p for p in audit.iterdir() if p.name.startswith(handle['run_id']+'-')])==(2 if handle==legacy else 4))
     current=state(handle);assert current==dict(value,late_off=str(cohort=='allow').lower()),current
     fixture.call('signal',handle,name='finish')
     assert json.loads(fixture.call('result',handle)['data'])==current
     history=fixture.history(handle);assert counts(history)['workflowtaskfailed']==0,counts(history)
     markers=patch_markers(history);ids=[value['id'] for value in markers]
     expected_ids=[] if handle==legacy else (['policy-on','policy-retired'] if cohort=='allow' else ['policy-retired'])
     if cohort=='allow':expected_ids+=['policy-late-off']
     assert sorted(ids)==sorted(expected_ids),(ids,expected_ids)
     for marker in markers:assert marker.get('deprecated',False)==(marker['id']=='policy-retired'),marker
    calls={p.name:p.read_text() for p in audit.iterdir()};assert len(calls)==18 and all(len(value.splitlines())==1 for value in calls.values()),calls
    worker.terminate();assert worker.wait(timeout=20)==0
    fixture.env['V_PATCH_POLICY_STAGE']='repaired';worker=fixture.launch(binary,'repaired',('worker',))
    assert fixture.call('result',panic,wait_ms=20000)['data']=='"policy-repaired"'
    assert not counts(fixture.history(panic))['workflowexecutionfailed']
    worker.terminate();assert worker.wait(timeout=20)==0
    fixture.env['V_PATCH_POLICY_STAGE']='poison'
    before={p.name:p.read_text() for p in audit.iterdir()}
    for index,handle in enumerate(handles+[panic]):
     file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     (directory/(str(index)+'-replay.log')).write_text(replay.stdout+replay.stderr)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     report['histories']+=1
    assert before=={p.name:p.read_text() for p in audit.iterdir()},'offline replay called policy'
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: allow/deny rollout, true/false memoization, legacy unmarked history, changed policy and live fresh patches, deprecation/cache bypass, numeric version independence, owned retry metadata, current memo/search/raw headers, command read-only guard, panic repair, SIGKILL/poison replacement and offline replay passed')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

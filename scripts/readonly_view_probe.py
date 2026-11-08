"""Owned workflow context snapshots, interaction scopes and command exclusion."""
RESULT_REPORT='readonly-view-results.json'
import argparse,json,os,subprocess
from pathlib import Path
import workflow_headers_probe
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  # The full opaque-header/codec/storage matrix also asserts all view accessors,
  # owned retained snapshots, two interceptor scopes and captured-context guards.
  os.environ['V_READONLY_VIEWS']='true'
  baseline=report_name.replace('.json','-headers.json')
  workflow_headers_probe.main(binary,baseline,quick)
  base=json.loads((ARTIFACTS/baseline).read_text());assert base['passed']
  report['histories']+=base['histories'];report['header_matrix']=baseline
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('readonly-view-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir()
    namespace='workflow-headers' if codec=='context' else 'default'
    if namespace!='default':
     created=subprocess.run([str(CLI),'operator','namespace','create','--address',fixture.address,'--namespace',namespace,'--retention','1d'],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
    fixture.env.update(V_READONLY_VIEWS='true',V_SDK_EXAMPLE_CODEC=codec,V_WORKFLOW_HEADER_CONVERTER='true',TEMPORAL_NAMESPACE=namespace)
    if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
    def call(mode,handle=None,*extra,failure=None):
     command=[str(binary),'workflow-headers-'+mode]
     if handle:command += [handle['workflow_id'],handle['run_id']]
     value=subprocess.run([*command,*extra],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     if failure:
      assert value.returncode!=0 and failure in value.stdout+value.stderr,value.stdout+value.stderr
     else:assert value.returncode==0,value.stdout+value.stderr
     return value.stdout.strip()
    worker=fixture.launch(binary,'before',('worker',))
    handle=dict(json.loads(call('start',None,'guards')),namespace=namespace)
    assert json.loads(call('query',handle))=={'signals':0,'updates':0}
    for query,message in [('view-panic','read-only query panic'),('view-force-fail','forbidden in query handler')]:
     call('query',handle,query,failure=message)
     assert json.loads(call('query',handle))=={'signals':0,'updates':0}
    for name,message in [('view-reject','read-only validation rejection'),('view-panic','read-only validator panic')]:
     call('update',handle,name,name,failure=message)
     assert json.loads(call('query',handle))=={'signals':0,'updates':0}
    assert call('update',handle,'increment','accepted')=='1'
    assert call('signal',handle,'increment')=='OK'
    assert json.loads(call('query',handle))=={'signals':1,'updates':1}
    worker.kill();assert worker.wait(timeout=10)==-9
    fixture.env['V_WORKFLOW_HEADER_REPLAY']='true'
    worker=fixture.launch(binary,'after',('worker',))
    assert json.loads(call('query',handle))=={'signals':1,'updates':1}
    assert call('update',handle,'increment','accepted')=='1'
    assert call('signal',handle,'finish')=='OK'
    result=fixture.call('result',handle);assert result['status']=='completed' and json.loads(result['data'])=={'signals':1,'updates':1},result
    history=fixture.history(handle);events=counts(history)
    assert events['workflowtaskfailed']==0,events
    assert events.get('timerstarted',0)==0,events
    accepted=[event for event in history['events'] if 'workflowExecutionUpdateAcceptedEventAttributes' in event]
    assert len(accepted)==1,accepted
    worker.terminate();assert worker.wait(timeout=20)==0
    file=directory/'guards.bin';fixture.call('history',handle,file=file)
    storage=directory/'storage'
    before={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
    replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
    (directory/'replay.log').write_text(replay.stdout+replay.stderr)
    assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
    after={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
    assert before==after,'read-only offline replay wrote storage'
    report['histories']+=1
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: query panic, captured command/task-failure exclusion, typed Update rejection and validator panic leave workflow usable without timers or failed tasks; scopes do not escape; accepted Update dedupes after SIGKILL and replays without validation or storage writes')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

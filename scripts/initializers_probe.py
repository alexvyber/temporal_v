"""Contextual named workflow factories and fallible initializer replay."""
RESULT_REPORT='initializers-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts
from panic_stack_assertions import assert_stack

def assert_initializer_panic(fixture,handle):
 history=fixture.history(handle);failures=[]
 def visit(value):
  if isinstance(value,dict):
   if value.get('message')=='initializer panic' and 'stackTrace' in value:failures.append(value)
   for child in value.values():visit(child)
  elif isinstance(value,list):
   for child in value:visit(child)
 visit(history)
 assert failures,'no initializer panic failure with original stack'
 for failure in failures:assert_stack(failure,'initializer_factory')

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('initializers-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir()
    namespace='workflow-headers' if codec=='context' else 'default'
    if namespace!='default':
     created=subprocess.run([str(CLI),'operator','namespace','create','--address',fixture.address,'--namespace',namespace,'--retention','1d'],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
    fixture.env.update(V_INITIALIZER_FIXED='false',V_SDK_EXAMPLE_CODEC=codec,V_WORKFLOW_HEADER_CONVERTER='true',TEMPORAL_NAMESPACE=namespace)
    if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
    def start(mode,value,label):
     result=subprocess.run([str(binary),'initializer-start',mode,str(value),label],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=30)
     assert result.returncode==0,result.stdout+result.stderr
     return dict(json.loads(result.stdout),namespace=namespace)
    def query(handle):return fixture.call('query',handle,name='state',rpc_ms=20000)['data']
    worker=fixture.launch(binary,'before',('worker',))
    healthy=[start('wait',index,str(index)) for index in range(4)]
    for index,handle in enumerate(healthy):assert query(handle)==str(index)
    immediate=start('immediate',17,'immediate');assert fixture.call('result',immediate)['data']=='17'
    blocked=[start('error',20,'error'),start('panic',30,'panic')]
    for handle,message in zip(blocked,['initializer typed error','initializer panic']):
     fixture.wait_for(lambda: counts(fixture.history(handle))['workflowtaskfailed']>=1)
     assert counts(fixture.history(handle))['workflowexecutionfailed']==0
     assert message in json.dumps(fixture.history(handle))
    assert_initializer_panic(fixture,blocked[1])
    assert worker.poll() is None
    for index,handle in enumerate(healthy):
     fixture.call('signal',handle,name='increment');assert query(handle)==str(index+1)
    worker.kill();assert worker.wait(timeout=10)==-9
    fixture.env['V_INITIALIZER_FIXED']='true'
    worker=fixture.launch(binary,'after',('worker',))
    for handle,expected in zip(blocked,['20','30']):
     fixture.wait_for(lambda: counts(fixture.history(handle))['workflowtaskcompleted']>=1)
     assert query(handle)==expected
     fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['data']==expected
    for index,handle in enumerate(healthy[:3]):
     assert query(handle)==str(index+1)
     fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['data']==str(index+1)
    fixture.call('cancel',healthy[3]);assert fixture.call('result',healthy[3])['status']=='canceled'
    for handle in healthy+[immediate]:assert counts(fixture.history(handle))['workflowtaskfailed']==0
    worker.terminate();assert worker.wait(timeout=20)==0
    fixture.env.update(V_INITIALIZER_FIXED='false',V_PANIC_FAIL_WORKFLOW='true')
    worker=fixture.launch(binary,'fail-policy',('worker',))
    failed=start('panic',40,'failed-panic');result=fixture.call('result',failed)
    assert result['status']=='failed' and result['message']=='initializer panic',result
    assert_initializer_panic(fixture,failed)
    worker.terminate();assert worker.wait(timeout=20)==0
    storage=directory/'storage'
    before={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
    for index,handle in enumerate(healthy+[immediate]+blocked+[failed]):
     fixture.env.update(V_INITIALIZER_FIXED=str(handle!=failed).lower(),V_PANIC_FAIL_WORKFLOW=str(handle==failed).lower())
     file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     (directory/(str(index)+'-replay.log')).write_text(replay.stdout+replay.stderr)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     report['histories']+=1
    after={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
    assert before==after,'initializer replay wrote storage'
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: contextual typed/binary constructor inputs, owned buffers, deterministic clock and metadata, raw opaque headers and memo, seeded state before first callback; concurrent progress, cancellation, typed-error/panic task retry and fail-workflow panic policy, SIGKILL/repair and eight offline replays')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

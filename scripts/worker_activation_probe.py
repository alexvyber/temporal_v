"""Codec-decoded activation snapshots and fatal observer errors drain workers."""
RESULT_REPORT='worker-activation-results.json'
import argparse,hashlib,json,os,subprocess,tempfile
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts
import worker_hooks_probe,panic_nexus_probe

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[],'subreports':{}}
 directory=Path(tempfile.mkdtemp(prefix='activation-audit-'+report_name.removesuffix('.json')+'-',dir=ARTIFACTS));report['audit_directory']=str(directory)
 previous=os.environ.get('V_ACTIVATION_AUDIT_DIR');os.environ['V_ACTIVATION_AUDIT_DIR']=str(directory)
 try:
  subprocess.run(['python3',str(ROOT/'scripts/generate_activation_decoder.py'),'--check'],check=True,capture_output=True,text=True)
  name=report_name.removesuffix('.json')+'-lifecycle.json';worker_hooks_probe.main(binary,name,quick)
  child=json.loads((ARTIFACTS/name).read_text());assert child['passed'];report['histories']+=child['histories'];report['subreports'][name]=hashlib.sha256((ARTIFACTS/name).read_bytes()).hexdigest()
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('activation-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    files=fixture.histories/fixture.queue;files.mkdir();namespace='activation-hooks' if codec=='context' else 'default'
    if namespace!='default':
     created=subprocess.run([str(CLI),'operator','namespace','create','--address',fixture.address,'--namespace',namespace,'--retention','1d'],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
    fixture.env.update(TEMPORAL_NAMESPACE=namespace,V_SDK_EXAMPLE_CODEC=codec,V_PANIC_FIXED='true',V_ACTIVATION_PACKED='true')
    if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage'))
    def packed(action,handle):
     result=subprocess.run([str(binary),'activation-packed',action,handle['workflow_id']],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=40)
     assert result.returncode==0,result.stdout+result.stderr
     return result.stdout.strip()
    worker=fixture.launch(binary,'packed-before',('worker',))
    parent={'workflow_id':fixture.queue+'-packed','namespace':namespace};parent.update(json.loads(packed('start',parent)))
    fixture.wait_for(lambda:json.loads(packed('query',parent))['ready']==2)
    assert packed('signal',parent)=='OK';assert packed('update',parent)=='2'
    worker.kill();assert worker.wait(timeout=10)==-9
    worker=fixture.launch(binary,'packed-after',('worker',))
    assert json.loads(packed('query',parent))=={'ready':2,'count':2}
    assert packed('finish',parent)=='OK';assert packed('result',parent)=='3'
    worker.terminate();assert worker.wait(timeout=30)==0
    packed_child={'workflow_id':parent['workflow_id']+'-child','namespace':namespace}
    handles=[parent,packed_child]
    def save_replay(handle,label):
     history=fixture.history(handle);file=files/(label+'.bin');fixture.call('history',handle,file=file)
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     report['histories']+=1
     return history
    for index,handle in enumerate(handles):assert counts(save_replay(handle,'packed-'+str(index)))['workflowtaskfailed']==0
    fixture.env['V_ACTIVATION_PACKED']='false'
    for failure in ['error','panic']:
     environment=dict(fixture.env,V_ACTIVATION_HOOK_FAILURE=failure,V_ACTIVATION_EXPECT_FATAL=failure)
     worker=fixture.launch(binary,failure+'-before',('worker',),env=environment)
     waiting=[dict(fixture.call('start',workflow_id=fixture.queue+'-'+failure+'-'+str(index),type='v.panic',input='{"mode":"await"}',namespace=namespace),namespace=namespace) for index in range(4)]
     for handle in waiting:
      assert fixture.call('query',handle,name='state',rpc_ms=20000)['data']=='0';fixture.call('signal',handle,name='panic');assert fixture.call('query',handle,name='state',rpc_ms=20000)['data']=='1'
     fixture.call('signal',waiting[0],name='worker-fatal')
     assert worker.wait(timeout=30)==1
     proof=directory/('fatal-'+failure+'-'+str(worker.pid)+'.proof');assert proof.exists(),proof
     assert 'worker activation interceptor failed' in proof.read_text()
     worker=fixture.launch(binary,failure+'-replacement',('worker',))
     for handle in waiting:
      assert fixture.call('query',handle,name='state',rpc_ms=20000)['data']=='1'
      fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['data']=='1'
     worker.terminate();assert worker.wait(timeout=30)==0
     for index,handle in enumerate(waiting):
      events=counts(save_replay(handle,failure+'-'+str(index)))
      if index==0:assert events['workflowtaskfailed']>=1
      else:assert events['workflowtaskfailed']==0
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: batched two-to-one codec inputs restored for workflow/child, queries, signals and Updates; Activity/child scopes, memo/header decoding, SIGKILL replay, fatal typed error/native-stack panic with four concurrent workflows, drained stop, configuration repair and ten offline histories')
  if not quick:
   name=report_name.removesuffix('.json')+'-nexus.json';panic_nexus_probe.main(binary,name)
   child=json.loads((ARTIFACTS/name).read_text());assert child['passed'];report['histories']+=child['histories'];report['subreports'][name]=hashlib.sha256((ARTIFACTS/name).read_bytes()).hexdigest()
  records={};hashes={}
  for path in directory.glob('*.json'):
   value=json.loads(path.read_text());records.setdefault((value['run_id'],value['before']),{})[value['label']]=value;hashes[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
  assert records
  for key,value in records.items():
   expected={'mutate','guard'} if value.get('guard',{}).get('guard_failed') else {'mutate','guard','after'}
   assert set(value)==expected,(key,value)
   assert value['mutate']['before']==value['guard']['before']
   if 'after' in value:assert value['after']['before']==value['mutate']['before']==value['after']['after']
   if value['mutate']['mutated']:assert value['mutate']['before']!=value['mutate']['after']
  assert any(value['mutate']['initial_arguments']==2 for value in records.values()),'batch cardinality was not restored'
  assert any(value['guard']['guard_failed'] for value in records.values()),'fatal guard was not exercised'
  report['observed_activations']=len(records);report['mutated_snapshots']=sum(value['mutate']['mutated'] for value in records.values());report['audit_files']=hashes
  report['cases'].append('Generated traversal matches all 66 reachable payload-bearing schema types; all observed payloads were retrieved/codec-decoded, independent callback snapshots isolated nested mutation, fatal callbacks stopped subsequent observers, typed/native-stack causes reached the caller, and original wire activations still executed/replayed')
  report['passed']=True
 finally:
  if previous is None:os.environ.pop('V_ACTIVATION_AUDIT_DIR',None)
  else:os.environ['V_ACTIVATION_AUDIT_DIR']=previous
  (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({key:value for key,value in report.items() if key!='audit_files'},indent=2))

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

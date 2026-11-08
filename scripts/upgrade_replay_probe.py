"""Live SDK120 <-> candidate handoffs plus dual offline replay on each platform."""
RESULT_REPORT='upgrade-replay-results.json'
import argparse,base64,hashlib,json,os,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture,counts

def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


PRODUCER_SOURCE = "module main\nimport sdk\nimport example_converter\nimport json2\nimport os\nfn run() ! {\n converter := example_converter.from_env()!\n client := sdk.connect(sdk.ClientOptions{address: os.getenv('TEMPORAL_ADDRESS'), data_converter: converter, rpc_timeout_millis: 20000})!\n defer {client.close()}\n label := if os.args[2] == 'large' { 'upgrade-' + 'x'.repeat(300000) } else { 'upgrade-state' }\n handle := client.start(sdk.WorkflowStart{workflow_id: os.args[1], workflow_type: 'v.counter', task_queue: os.getenv('TEMPORAL_TASK_QUEUE'), arguments: json2.encode({'initial': json2.Any(0), 'label': json2.Any(label)})})!\n println(json2.encode(handle))\n}\nfn main() {run() or { eprintln(err.msg()); exit(1) }}\n"

def compile_producer():
 import platform
 folder=ARTIFACTS/'upgrade-producer';folder.mkdir(exist_ok=True)
 (folder/'v.mod').write_text("Module { name: 'upgrade_producer', dependencies: ['protobuf'] }\n")
 (folder/'main.v').write_text(PRODUCER_SOURCE)
 for name in ['sdk','corebridge','native','temporal','example_converter','example_storage']:
  target=folder/name
  if not target.exists():target.symlink_to(ROOT/name,target_is_directory=True)
 lib=ARTIFACTS/'rust-target'/os.environ.get('SPIKE_BUILD_PROFILE','debug')
 link=(f'{lib}/libtemporalio_sdk_core_c_bridge.dylib {lib}/libtemporal_v_payloads.dylib' if platform.system()=='Darwin' else f'-L{lib} -ltemporalio_sdk_core_c_bridge -ltemporal_v_payloads')+f' -Wl,-rpath,{lib}'
 binary=ARTIFACTS/'tools/v-upgrade-producer'
 command=[str(ARTIFACTS/'tools/v/v'),'-d','use_bundled_libgc','-ldflags',link,'-o',str(binary),str(folder)]
 environment=dict(os.environ,VMODULES=str(ARTIFACTS/'vmodules'),V_MACOS_V3_NO_FALLBACK='1',V_C_ERROR_BUG_REPORT_DISABLED='1')
 with (ARTIFACTS/'logs/upgrade-producer-build.log').open('w') as log:
  subprocess.run(command,cwd=ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT,check=True)
 return binary

def main(worker,replayer,report_name,quick=False):
 old_worker=ARTIFACTS/'tools/v-upgrade-baseline-worker';old_replayer=ARTIFACTS/'tools/v-upgrade-baseline-replay'
 build=json.loads((ARTIFACTS/'upgrade-baseline-build.json').read_text());assert build['passed']
 for name,expected in build['images'].items():assert digest(Path(name))==expected,'baseline executable changed'
 producer=compile_producer()
 report={'passed':False,'baseline':build['baseline'],'histories':0,'replays':0,'cases':[],'images':{str(p):digest(p) for p in [worker,replayer,old_worker,old_replayer,producer]}}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:2]
  for direction,first,second in [('upgrade',old_worker,worker),('rollback',worker,old_worker)]:
   for version,server,codec,stored in profiles:
    with TemporalFixture('sdk-'+direction+'-'+version+'-'+(codec or 'default')+'-'+str(stored),server_binary=ARTIFACTS/'tools'/server) as fixture:
     directory=fixture.histories/fixture.queue;directory.mkdir()
     fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_SDK_EXAMPLE_COMBINED_WORKER='true')
     if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
     before=fixture.launch(first,'before')
     started=subprocess.run([str(producer),fixture.queue+'-counter','large' if stored else 'small'],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=40)
     assert started.returncode==0,started.stdout+started.stderr
     counter=json.loads(started.stdout)
     if stored:
      initial=fixture.history(counter)['events'][0]['workflowExecutionStartedEventAttributes']['input']['payloads']
      assert len(initial)==1 and base64.b64decode(initial[0]['metadata'].get('messageType',''))==b'temporal.api.sdk.v1.ExternalStorageReference','input history did not contain a real external storage reference'
     added=fixture.call('update',counter,name='add',update_id='once',input='{"amount":5}');assert added['status']=='completed',added
     local=fixture.start('local','v.default-retry',{'local':True,'fail_until':2});assert fixture.call('result',local)['status']=='completed'
     failed=fixture.start('failed','v.failure-chain.remote',{});assert fixture.call('result',failed)['status']=='failed'
     restart=fixture.start('restart','v.restart',{'name':'upgrade','delay_seconds':8})
     parent=fixture.start('parent','v.parent',{'name':'child-upgrade','delay_seconds':8})
     cancellation=fixture.start('cancel','v.parallel',{'name':'cancel-upgrade','delay_ms':20,'wait_for_cancel':True})
     fixture.wait_for(lambda: counts(fixture.history(restart))['timerstarted']==1)
     fixture.wait_for(lambda: any('childWorkflowExecutionStartedEventAttributes' in event for event in fixture.history(parent)['events']))
     child_event=next(event['childWorkflowExecutionStartedEventAttributes'] for event in fixture.history(parent)['events'] if 'childWorkflowExecutionStartedEventAttributes' in event)
     child={'workflow_id':child_event['workflowExecution']['workflowId'],'run_id':child_event['workflowExecution']['runId']}
     fixture.wait_for(lambda: counts(fixture.history(child))['timerstarted']==1)
     chain=fixture.start('chain','v.chain',{'remaining':1,'total':0})
     pending=fixture.call('update',counter,name='delayed_add',update_id='pending',input='{"amount":2}',wait_ms=1000)
     assert pending['status'] in ['accepted','completed'],pending
     fixture.wait_for(lambda: counts(fixture.history(counter))['timerstarted']==1)
     assert before.poll() is None
     before.kill();assert before.wait(timeout=10)==-9
     after=fixture.launch(second,'after')
     pending=fixture.call('update-result',counter,update_id='pending',rpc_ms=20000);assert pending['status']=='completed',pending
     state=json.loads(fixture.call('query',counter,name='state',rpc_ms=20000)['data']);assert state['total']==7,state['total']
     duplicate=fixture.call('update',counter,name='add',update_id='once',input='{"amount":5}');assert json.loads(duplicate['data'])['total']==5
     fixture.call('signal',counter,name='add',input='{"amount":7}')
     assert json.loads(fixture.call('query',counter,name='state')['data'])['total']==14
     fixture.call('signal',counter,name='finish');assert json.loads(fixture.call('result',counter)['data'])['total']==14
     assert fixture.call('result',restart)['status']=='completed'
     assert fixture.call('result',parent)['status']=='completed'
     assert fixture.call('chain-result',chain)['data']=='1'
     fixture.call('cancel',cancellation);assert fixture.call('result',cancellation)['status']=='canceled'
     chain_event=next(event['workflowExecutionContinuedAsNewEventAttributes'] for event in fixture.history(chain)['events'] if 'workflowExecutionContinuedAsNewEventAttributes' in event)
     continuation={'workflow_id':chain['workflow_id'],'run_id':chain_event['newExecutionRunId']}
     handles=[counter,local,failed,restart,parent,child,cancellation,chain,continuation]
     after.terminate();assert after.wait(timeout=30)==0
     storage=directory/'storage'
     if stored:assert any(path.is_file() and len(path.name)==64 for path in storage.rglob('*')),'large input was not actually offloaded'
     before_files={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
     for index,handle in enumerate(handles):
      history=fixture.history(handle);events=counts(history);assert events['workflowtaskfailed']==0,(index,events)
      file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
      for label,binary in [('baseline',old_replayer),('candidate',replayer)]:
       result=subprocess.run([str(binary),'--history',str(file),'--workflow-id',handle['workflow_id'],'--task-queue',fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=60)
       (directory/(str(index)+'-'+label+'-replay.log')).write_text(result.stdout+result.stderr)
       assert result.returncode==0,(index,label,result.stdout[-1000:],result.stderr[-2000:]);report['replays']+=1
       if handle==restart:
        wrong=subprocess.run([str(binary),'--wrong-code','--history',str(file),'--workflow-id',handle['workflow_id'],'--task-queue',fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=60)
        (directory/(str(index)+'-'+label+'-wrong.log')).write_text(wrong.stdout+wrong.stderr)
        assert wrong.returncode!=0,'negative control accepted incompatible workflow code'
      report['histories']+=1
     after_files={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
     assert before_files==after_files,'offline replay uploaded storage payloads'
     report['cases'].append(f'{direction}/{version}/{codec or "default"}/storage={stored}: SIGKILL across SDKs with accepted pending Update, dedupe, signals, timer, local retry, protected failure, child, cancellation and patch/continue-as-new; nine histories replayed by both SDKs; incompatible-code negative controls rejected')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--worker',type=Path,default=ARTIFACTS/'tools/v-workflow-worker');parser.add_argument('--replayer',type=Path,default=ARTIFACTS/'tools/v-replay');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.worker,args.replayer,args.report,args.quick)

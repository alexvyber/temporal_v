"""Worker/replay middleware ordering, one-use continuations and drained shutdown."""
RESULT_REPORT='worker-hooks-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  unit=subprocess.run([str(binary),'hook-unit'],cwd=ROOT,capture_output=True,text=True,timeout=30)
  assert unit.returncode==0 and 'WORKER_HOOK_UNIT_OK cases=12' in unit.stdout,unit.stdout+unit.stderr
  report['cases'].append('Twelve offline middleware cases: short circuit, typed error, original-stack panic, duplicate and escaped continuations, role protection and simultaneous-call exclusion')
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('worker-hooks-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir();drain=directory/'drain';drain.mkdir()
    namespace='worker-hooks' if codec=='context' else 'default'
    if namespace!='default':
     created=subprocess.run([str(CLI),'operator','namespace','create','--address',fixture.address,'--namespace',namespace,'--retention','1d'],capture_output=True,text=True,timeout=20)
     assert created.returncode==0,created.stderr
    fixture.env.update(TEMPORAL_NAMESPACE=namespace,V_SDK_EXAMPLE_CODEC=codec,V_PANIC_FIXED='true',V_WORKER_HOOK_SHUTDOWN_PANIC='true',V_WORKER_HOOK_DRAIN_DIR=str(drain))
    if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'))
    def launch(label,command='worker',draining=False):
     journal=directory/(label+'.jsonl');journal.touch()
     environment=dict(fixture.env,V_WORKER_HOOK_LOG=str(journal),V_WORKER_HOOK_DRAIN=str(draining).lower())
     process=fixture.launch(binary,label,(command,),env=environment)
     fixture.wait_for(lambda:len(journal.read_text().splitlines())>=2)
     return process,journal
    def stop_worker(process,journal,role):
     process.terminate();assert process.wait(timeout=30)==0,journal.read_text()
     lines=journal.read_text().splitlines()
     expected=[f'run-before:outer:{role}:{namespace}:{fixture.queue}',f'run-before:inner:{role}:{namespace}:{fixture.queue}',f'shutdown:outer:{role}:false:{namespace}:{fixture.queue}',f'shutdown:inner:{role}:false:{namespace}:{fixture.queue}',f'shutdown:observer:{role}:false:{namespace}:{fixture.queue}','run-after:inner','run-after:outer']
     assert lines==expected,lines
    def start(label,mode):
     return dict(fixture.call('start',workflow_id=fixture.queue+'-'+label,type='v.panic',input=json.dumps({'mode':mode}),namespace=namespace),namespace=namespace)
    worker,journal=launch('before')
    waiting=[start(str(index),'await') for index in range(4)]
    for handle in waiting:
     assert fixture.call('query',handle,name='state')['data']=='0'
     fixture.call('signal',handle,name='panic')
     assert fixture.call('query',handle,name='state')['data']=='1'
    completed=[start('remote','activity-retry'),start('local','local-retry')]
    for handle in completed:assert fixture.call('result',handle)['data']=='"activity-recovered"'
    stop_worker(worker,journal,'combined')
    workflows,wjournal=launch('split-workflows','hook-workflows')
    activities,ajournal=launch('split-activities','hook-activities',True)
    nexus,njournal=launch('split-nexus','hook-nexus')
    remote=start('split-remote','activity-retry');assert fixture.call('result',remote)['data']=='"activity-recovered"';completed.append(remote)
    failed=start('failed','activity-fail');assert fixture.call('result',failed)['status']=='failed';completed.append(failed)
    draining=start('draining','activity-hook-drain');completed.append(draining)
    fixture.wait_for(lambda:(drain/('begin-'+str(activities.pid))).exists())
    stop_worker(activities,ajournal,'activity')
    assert (drain/('end-'+str(activities.pid))).exists()
    stop_worker(nexus,njournal,'nexus')
    for handle in waiting:assert fixture.call('query',handle,name='state')['data']=='1'
    workflows.kill();assert workflows.wait(timeout=10)==-9
    assert len(wjournal.read_text().splitlines())==2,wjournal.read_text()
    replacement,rjournal=launch('replacement')
    assert fixture.call('result',draining)['data']=='"activity-recovered"'
    for handle in waiting[:3]:
     assert fixture.call('query',handle,name='state')['data']=='1'
     fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['data']=='1'
    fixture.call('cancel',waiting[3])
    assert fixture.call('result',waiting[3])['status']=='canceled'
    stop_worker(replacement,rjournal,'combined')
    for index,handle in enumerate(waiting+completed):
     history=fixture.history(handle);assert counts(history)['workflowtaskfailed']==0
     file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
     replay_journal=directory/('replay-'+str(index)+'.jsonl');replay_journal.touch()
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=dict(fixture.env,V_WORKER_HOOK_LOG=str(replay_journal)),capture_output=True,text=True,timeout=45)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     expected=['replay-before:outer','replay-before:inner',f'shutdown:outer:workflow:true:{namespace}:{fixture.queue}',f'shutdown:inner:workflow:true:{namespace}:{fixture.queue}',f'shutdown:observer:workflow:true:{namespace}:{fixture.queue}','replay-after:inner','replay-after:outer']
     assert replay_journal.read_text().splitlines()==expected,replay_journal.read_text()
     report['histories']+=1
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: combined and split workflow/Activity/Nexus middleware, actual Activity cancellation/drain, isolated shutdown-hook panic with later hook and outer middleware completion, concurrent workflows, failure/cancellation, SIGKILL, replacement and nine offline replays')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

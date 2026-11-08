"""Nonthrowing cancellation polls and heartbeat-preserving waits on real tasks."""
RESULT_REPORT='activity-wait-results.json'
import argparse,json,subprocess,time
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('activity-wait-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir();fixture.env.update(V_ACTIVITY_WAIT_DIR=str(directory),V_SDK_EXAMPLE_CODEC=codec)
    namespace='activity-waits' if codec=='context' else 'default'
    if namespace!='default':
     created=subprocess.run([str(CLI),'operator','namespace','create','--address',fixture.address,'--namespace',namespace,'--retention','1d'],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
    fixture.env['TEMPORAL_NAMESPACE']=namespace
    def start(label,data):return dict(fixture.call('start',workflow_id=fixture.queue+'-'+label,type='v.activity-wait',input=json.dumps(data),namespace=namespace),namespace=namespace)

    if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'))
    def pending(handle):
     result=subprocess.run([str(CLI),'workflow','describe','--address',fixture.address,'--workflow-id',handle['workflow_id'],'--namespace',namespace,'--raw','--no-json-shorthand-payloads','--output','json'],capture_output=True,text=True,timeout=15)
     assert result.returncode==0,result.stderr
     return json.loads(result.stdout).get('pendingActivities',[])
    def replay(handle,label):
     file=directory/(label+'.bin');fixture.call('history',handle,file=file)
     result=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     assert result.returncode==0 and result.stdout.strip()=='OK',result.stdout+result.stderr
     report['histories']+=1
    for local in [False,True]:
     worker=fixture.launch(binary,'cancel-'+str(local),('worker',))
     handle=start('cancel-'+str(local),{'local':local})
     fixture.wait_for(lambda:(directory/(handle['workflow_id']+'-begin')).exists())
     if not local:
      fixture.wait_for(lambda:len(pending(handle))==1 and len(pending(handle)[0].get('heartbeatDetails',{}).get('payloads',[]))==2)
      first=pending(handle)[0];time.sleep(0.7);second=pending(handle)[0]
      assert second['heartbeatDetails']==first['heartbeatDetails'] and second['lastHeartbeatTime']!=first['lastHeartbeatTime'],(first,second)
      (directory/'heartbeat-preservation.json').write_text(json.dumps([first,second],indent=2)+'\n')
     fixture.call('signal',handle,name='cancel')
     assert fixture.call('result',handle)['data']=='"cancelled"'
     audit=json.loads((directory/(handle['workflow_id']+'-cancel.json')).read_text());assert audit['stable'] and audit['local']==local and audit['reason']=='cancelled',audit
     worker.terminate();assert worker.wait(timeout=30)==0
     assert counts(fixture.history(handle))['workflowtaskfailed']==0;replay(handle,'cancel-'+str(local))
     worker=fixture.launch(binary,'shutdown-'+str(local),('activity-wait-control' if local else 'worker',))
     handle=start('shutdown-'+str(local),{'local':local,'shutdown':True})
     fixture.wait_for(lambda:(directory/(handle['workflow_id']+'-begin')).exists())
     began_stop=time.monotonic()
     if local:(directory/('stop-'+str(worker.pid))).touch()
     else:worker.terminate()
     assert worker.wait(timeout=30)==0
     assert time.monotonic()-began_stop>=0.75,'grace period was skipped'
     audit=json.loads((directory/(handle['workflow_id']+'-cancel.json')).read_text());assert audit['stable'] and audit['local']==local and audit['reason']=='worker_shutdown',audit
     fixture.env['V_WAIT_REPAIRED']='true';worker=fixture.launch(binary,'repaired-'+str(local),('worker',))
     assert fixture.call('result',handle)['data']=='"repaired"'
     worker.terminate();assert worker.wait(timeout=30)==0
     replay(handle,'shutdown-'+str(local));fixture.env.pop('V_WAIT_REPAIRED')
    for local in [False,True]:
     worker=fixture.launch(binary,'timeout-'+str(local),('worker',))
     handle=start('timeout-'+str(local),{'local':local,'timeout':True})
     assert fixture.call('result',handle)['data']=='"timedout"'
     fixture.wait_for(lambda:(directory/(handle['workflow_id']+'-cancel.json')).exists())
     audit=json.loads((directory/(handle['workflow_id']+'-cancel.json')).read_text());assert audit['stable'] and audit['reason'] in ['timed_out','not_found'],audit
     assert json.loads(audit['details'])['is_'+audit['reason']],audit
     worker.terminate();assert worker.wait(timeout=30)==0
     replay(handle,'timeout-'+str(local))
    worker=fixture.launch(binary,'concurrent-control',('activity-wait-control',))
    concurrent=[start('concurrent-'+str(index),{'local':index<2,'shutdown':True}) for index in range(4)]
    fixture.wait_for(lambda:all((directory/(handle['workflow_id']+'-begin')).exists() for handle in concurrent))
    (directory/('stop-'+str(worker.pid))).touch();assert worker.wait(timeout=30)==0
    for handle in concurrent:
     audit=json.loads((directory/(handle['workflow_id']+'-cancel.json')).read_text());assert audit['stable'] and audit['reason']=='worker_shutdown',audit
    fixture.env['V_WAIT_REPAIRED']='true';worker=fixture.launch(binary,'concurrent-repaired',('worker',))
    for handle in concurrent:assert fixture.call('result',handle)['data']=='"repaired"'
    worker.terminate();assert worker.wait(timeout=30)==0
    for index,handle in enumerate(concurrent):
     if index<2:assert counts(fixture.history(handle))['workflowtaskfailed']>=1
     replay(handle,'concurrent-'+str(index))
    fixture.env.pop('V_WAIT_REPAIRED')
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: remote/local cancellation, SIGTERM/control shutdown respecting grace and workflow-task interruption, timeout/not-found delivery, retained first cause, repeat poll/wait/throwing checks, preserved two-value progress and advancing heartbeats, drain, replacement and four concurrent mixed Activities, namespace isolation and ten offline replays')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

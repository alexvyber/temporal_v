"""Raw workflow and interaction headers retain codec/storage scopes and replay."""
RESULT_REPORT='workflow-headers-results.json'
import argparse,base64,json,subprocess
from pathlib import Path
from fixture_payload_codec import decode_fixture_payload
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'queries':0,'updates':0,'cases':[]}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('workflow-headers-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir()
    namespace='workflow-headers' if codec=='context' else 'default'
    if namespace!='default':
     created=subprocess.run([str(CLI),'operator','namespace','create','--address',fixture.address,'--namespace',namespace,'--retention','1d'],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_WORKFLOW_HEADER_CONVERTER='true',TEMPORAL_NAMESPACE=namespace)
    if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
    def call(mode,handle=None,*extra):
     args=[str(binary),'workflow-headers-'+mode]
     if handle:args += [handle['workflow_id'],handle['run_id']]
     result=subprocess.run([*args,*extra],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     assert result.returncode==0,result.stdout+result.stderr
     return result.stdout.strip()
    worker=fixture.launch(binary,'before',('worker',))
    handles=[dict(json.loads(call('start',None,'outer-'+str(index))),namespace=namespace) for index in range(4)]
    for handle in handles:
     assert json.loads(call('query',handle))=={'signals':0,'updates':0};report['queries']+=1
     assert call('signal',handle,'increment')=='OK'
     assert call('update',handle)=='1';report['updates']+=1
     assert json.loads(call('query',handle))=={'signals':1,'updates':1};report['queries']+=1
    worker.kill();assert worker.wait(timeout=10)==-9
    fixture.env['V_WORKFLOW_HEADER_REPLAY']='true'
    worker=fixture.launch(binary,'after',('worker',))
    for handle in handles:
     assert json.loads(call('query',handle))=={'signals':1,'updates':1};report['queries']+=1
     assert call('update',handle)=='1';report['updates']+=1
     assert call('signal',handle,'finish')=='OK'
     result=fixture.call('result',handle);assert result['status']=='completed' and json.loads(result['data'])=={'signals':1,'updates':1},result
     history=fixture.history(handle);assert counts(history)['workflowtaskfailed']==0,counts(history)
     seen=[]
     for event in history['events']:
      attributes=event.get('workflowExecutionStartedEventAttributes') or event.get('workflowExecutionSignaledEventAttributes')
      if 'workflowExecutionUpdateAcceptedEventAttributes' in event:
       attributes=event['workflowExecutionUpdateAcceptedEventAttributes']['acceptedRequest']['input']
      if not attributes or 'header' not in attributes:continue
      fields=attributes['header']['fields'];assert sorted(fields)==['a-opaque','m-sentinel','z-known']
      payload=fields['a-opaque'];metadata={key:base64.b64decode(value) for key,value in payload['metadata'].items()}
      assert (metadata.get('messageType')==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,metadata
      if not stored:
       metadata,data=decode_fixture_payload(payload,codec)
       assert data==bytes([0,255,42])*100001 and metadata['private']==b'opaque\x00\xff\x00'
      seen.append(event)
     assert len(seen)==4,[(event['eventId'],event.get('eventType')) for event in seen]
    worker.terminate();assert worker.wait(timeout=20)==0
    storage=directory/'storage'
    before={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
    for index,handle in enumerate(handles):
     file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     (directory/(str(index)+'-replay.log')).write_text(replay.stdout+replay.stderr)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     report['histories']+=1
    after={str(p.relative_to(storage)):(p.stat().st_size,p.stat().st_mtime_ns) for p in storage.rglob('*') if p.is_file()} if storage.exists() else {}
    assert before==after,'offline replay wrote external payload storage'
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}/namespace={namespace}: concurrent starts/signals/query interceptors/Update validators and handlers retained 300003-byte opaque headers, metadata, owned buffers and explicit scoped errors; completed Update dedupe, poison-validator SIGKILL replacement and offline replay without storage writes passed')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

"""Opaque Activity headers reach named/dynamic handlers before explicit conversion."""
RESULT_REPORT='raw-headers-results.json'
import argparse,base64,json,subprocess
from pathlib import Path
from fixture_payload_codec import decode_fixture_payload
from integration import ARTIFACTS,ROOT,TemporalFixture,counts

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'activities':0,'cases':[]}
 try:
  profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['', 'prefix','context'] for stored in [False,True]]
  if quick:profiles=profiles[:1]
  for version,server,codec,stored in profiles:
   with TemporalFixture('raw-headers-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
    directory=fixture.histories/fixture.queue;directory.mkdir()
    fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_RAW_HEADER_CONVERTER='true')
    if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
    worker=fixture.launch(binary,'before',('worker',))
    handles=[fixture.start('headers-'+str(index),'v.raw-headers',{}) for index in range(4)]
    def state(handle):return json.loads(fixture.call('query',handle,name='state')['data'])
    for handle in handles:fixture.wait_for(lambda:state(handle)==4)
    worker.kill();assert worker.wait(timeout=10)==-9
    worker=fixture.launch(binary,'after',('worker',))
    for handle in handles:fixture.wait_for(lambda:state(handle)==4)
    for handle in handles:fixture.call('signal',handle,name='finish')
    for handle in handles:
     result=fixture.call('result',handle);assert result['status']=='completed' and json.loads(result['data'])=={'completed':4},result
     history=fixture.history(handle);events=counts(history)
     assert events['workflowtaskfailed']==0 and events['activitytaskscheduled']==2 and events['activitytaskcompleted']==2,events
     for event in history['events']:
      if 'activityTaskScheduledEventAttributes' not in event:continue
      headers=event['activityTaskScheduledEventAttributes']['header']['fields'];assert sorted(headers)==['a-opaque','m-sentinel','z-known']
      payload=headers['a-opaque'];metadata={key:base64.b64decode(value) for key,value in payload['metadata'].items()}
      assert (metadata.get('messageType')==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,metadata
      if not stored:
       metadata,data=decode_fixture_payload(payload,codec)
       assert data==bytes([0,255,42])*100001
       assert metadata['encoding']==b'application/x-opaque-header' and metadata['private']==b'opaque\x00\xff\x00'
     report['activities']+=4
    worker.terminate();assert worker.wait(timeout=20)==0
    for index,handle in enumerate(handles):
     file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
     replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
     (directory/(str(index)+'-replay.log')).write_text(replay.stdout+replay.stderr)
     assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
     report['histories']+=1
    report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: four concurrent workflows, named/dynamic remote/local handlers, 300003-byte opaque headers and binary metadata, explicit scoped converter failures, owned snapshots, JSON omission, actual storage references, SIGKILL/replacement and offline replay passed')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

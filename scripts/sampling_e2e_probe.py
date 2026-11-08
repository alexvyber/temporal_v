"""Automatic sampling applies to propagation without changing durable execution or replay."""
RESULT_REPORT='sampling-e2e-results.json'
import argparse,base64,json,subprocess,uuid
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
from propagation_wire import carriers,inspect_propagation

CASES=[
 ('on','on',False,'low','tenant=sampling','all'),
 ('off','off',True,'low','tenant=sampling','none'),
 ('ratio-low','ratio',True,'low','tenant=sampling','all'),
 ('ratio-high','ratio',True,'high','tenant=sampling','none'),
 ('parent-on','parent-off-root',True,'low','tenant=sampling','all'),
 ('parent-off','parent-on-root',False,'low','tenant=sampling','none'),
 ('record-only','record-only',True,'low','tenant=sampling','none'),
 ('commands-only','commands-only',True,'low','tenant=sampling','producer'),
 ('remote-only','remote-only',True,'low','tenant=sampling','server'),
 ('tenant-keep','tenant',True,'low','tenant=sampled','all'),
 ('tenant-drop','tenant',True,'low','tenant=other','none')
]

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
   for label,policy,parent_sampled,tail,baggage,exported in (CASES[:2] if quick else CASES):
    with TemporalFixture(f'sampling-{version}-{label}',server_binary=server) as fixture:
     files=ARTIFACTS/'sampling-fixtures'/fixture.queue;files.mkdir(parents=True);journal=files/'live.jsonl'
     trace_id=uuid.uuid4().hex[:16]+('0000000000000001' if tail=='low' else 'ffffffffffffffff')
     fixture.env.update(V_TRACE_ID=trace_id,V_TRACE_BAGGAGE=baggage,V_TRACE_JOURNAL=str(journal),V_TRACE_SAMPLER=policy,V_TRACE_PARENT_SAMPLED='1' if parent_sampled else '0',V_SDK_EXAMPLE_CODEC='context',V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage'))
     def invoke(role,*args,env=None):
      result=subprocess.run([str(binary),role,*map(lambda value:json.dumps(value) if isinstance(value,dict) else str(value),args)],cwd=ROOT,env=env or fixture.env,capture_output=True,text=True,timeout=45)
      with (files/'calls.log').open('a') as output:output.write(role+'\n'+result.stdout+result.stderr)
      assert result.returncode==0,(role,result.stdout,result.stderr)
      return result.stdout.strip()
     def records():return [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []
     worker=fixture.launch(binary,'before',('trace-worker',));handles=json.loads(invoke('trace-start'))
     for handle in handles:fixture.wait_for(lambda:invoke('trace-state',handle)=='3')
     if exported in ('all','producer'):fixture.wait_for(lambda:len([span for span in records() if span['kind']=='producer'])==12)
     worker.kill();worker.wait(timeout=10);fixture.launch(binary,'after',('trace-worker',))
     for handle in handles:fixture.wait_for(lambda:invoke('trace-state',handle)=='3')
     for handle in handles:assert invoke('trace-update',handle)=='OK'
     for index,handle in enumerate(handles):assert invoke('trace-cancel' if index==2 else 'trace-finish',handle)=='OK'
     for index,handle in enumerate(handles):
      result=json.loads(invoke('trace-result',handle));assert result['status']==['completed','failed','canceled','completed'][index],result
     if exported in ('all','producer'):fixture.wait_for(lambda:len([span for span in records() if span['kind']=='producer'])==13)
     if exported in ('all','server'):fixture.wait_for(lambda:len([span for span in records() if span['name']=='RunWorkflow:v.trace.workflow'])==9)
     all_handles=list(handles)
     for handle in handles:
      history=fixture.history(handle);inspect_propagation(history,trace_id,baggage,require_memo_storage=True)
      for event in history['events']:
       if 'childWorkflowExecutionStartedEventAttributes' in event:
        execution=event['childWorkflowExecutionStartedEventAttributes']['workflowExecution'];all_handles.append({'workflow_id':execution['workflowId'],'run_id':execution['runId']})
       if 'workflowExecutionContinuedAsNewEventAttributes' in event:all_handles.append({'workflow_id':handle['workflow_id'],'run_id':event['workflowExecutionContinuedAsNewEventAttributes']['newExecutionRunId']})
     assert len(all_handles)==9
     roots={handle['run_id'] for handle in handles};offline=files/'offline.jsonl';env=dict(fixture.env,V_TRACE_JOURNAL=str(offline))
     for index,handle in enumerate(all_handles):
      history=fixture.history(handle)
      assert not any('workflowTaskFailedEventAttributes' in event for event in history['events'])
      for event in history['events']:
       for key,attributes in event.items():
        if not key.endswith('EventAttributes') or not isinstance(attributes,dict):continue
        for payload in carriers(attributes):
         carrier=json.loads(base64.b64decode(payload['data']));flags=int(carrier['traceparent'].split('-')[3],16)
         if exported=='producer':
          expected=(key in ('activityTaskScheduledEventAttributes','startChildWorkflowExecutionInitiatedEventAttributes','childWorkflowExecutionStartedEventAttributes','workflowExecutionContinuedAsNewEventAttributes') or (key=='workflowExecutionStartedEventAttributes' and handle['run_id'] not in roots))
          assert bool(flags&1)==expected,(label,key,carrier)
          assert carrier['tracestate']==('sampler=producer' if expected else 'sampler=drop'),carrier
         else:assert bool(flags&1)==(exported=='all'),(label,key,carrier)
      history_file=files/f'{index}.bin';assert invoke('trace-history',handle,history_file)=='OK'
      assert invoke('trace-replay',history_file,handle['workflow_id'],fixture.queue,env=env)=='OK';report['histories']+=1
     assert not offline.exists() or offline.stat().st_size==0
     spans=records()
     assert all(span['context']['trace_id']==trace_id for span in spans)
     if exported=='none':assert not spans,(label,len(spans))
     elif exported=='server':assert spans and all(span['parent_remote'] for span in spans),(label,{(span['kind'],span['parent_remote']) for span in spans})
     elif exported!='all':assert spans and all(span['kind']==exported for span in spans),(label,{span['kind'] for span in spans})
     if exported in ('all','server'):assert len([span for span in spans if span['name']=='RunActivity:v.trace.activity'])==8
     if policy=='tenant':assert all(span['attributes']['sampler.policy']=='tenant' for span in spans)
     report['cases'].append(f'{version}/{label}: sampling/recording policy changed only telemetry; actual inline flags/tracestate, concurrent Activities, children, query/Update, failure/cancellation and Continue-as-New survived SIGKILL/replacement; nine histories replayed without exports')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

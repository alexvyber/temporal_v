"""Nexus propagation uses sampled contexts, including retained helper client scopes."""
RESULT_REPORT='sampling-nexus-results.json'
import argparse,base64,json,subprocess,uuid
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
from propagation_wire import carriers,inspect_propagation

POLICIES=[('on','all'),('off','none'),('commands-only','producer'),('remote-only','server')]

def main(binary,report_name):
 report={'passed':False,'histories':0,'cases':[]}
 try:
  for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
   for policy,exported in POLICIES:
    with TemporalFixture(f'sample-nexus-{version}-{policy}',server_binary=server) as fixture:
     files=ARTIFACTS/'sampling-nexus-fixtures'/fixture.queue;files.mkdir(parents=True);journal=files/'live.jsonl';endpoint=fixture.queue+'-endpoint';trace_id=uuid.uuid4().hex
     fixture.env.update(V_TRACE_ID=trace_id,V_TRACE_BAGGAGE='tenant=sampling',V_TRACE_JOURNAL=str(journal),V_TRACE_SAMPLER=policy,V_TRACE_NEXUS_ENDPOINT=endpoint,V_SDK_EXAMPLE_CODEC='context',V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage'))
     created=subprocess.run([str(server),'operator','nexus','endpoint','create','--address',fixture.address,'--name',endpoint,'--target-namespace','default','--target-task-queue',fixture.queue],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
     def invoke(role,*args,env=None):
      result=subprocess.run([str(binary),role,*map(lambda value:json.dumps(value) if isinstance(value,dict) else str(value),args)],cwd=ROOT,env=env or fixture.env,capture_output=True,text=True,timeout=45)
      with (files/'calls.log').open('a') as output:output.write(role+'\n'+result.stdout+result.stderr)
      assert result.returncode==0,(role,result.stdout,result.stderr)
      return result.stdout.strip()
     def spans():return [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []
     worker=fixture.launch(binary,'before',('trace-worker',));handles=json.loads(invoke('trace-nexus-start'))
     for handle in handles[:2]:fixture.wait_for(lambda:invoke('trace-state',handle)=='1')
     assert json.loads(invoke('trace-result',handles[2]))['status']=='failed'
     def backing(handle):
      history=fixture.history(handle);started=[event['nexusOperationStartedEventAttributes'] for event in history['events'] if 'nexusOperationStartedEventAttributes' in event]
      if not started:return None
      token=started[0]['operationToken'];decoded=json.loads(base64.urlsafe_b64decode(token+'='*(-len(token)%4)))
      return {'workflow_id':decoded['workflow_id'],'run_id':decoded['first_run_id']}
     fixture.wait_for(lambda:backing(handles[3]) is not None);cancelled=backing(handles[3]);fixture.wait_for(lambda:invoke('trace-state',cancelled)=='3')
     if exported in ('all','producer'):fixture.wait_for(lambda:len([span for span in spans() if span['kind']=='producer'])==7)
     worker.kill();worker.wait(timeout=10);fixture.launch(binary,'after',('trace-worker',))
     for handle in handles[:2]:fixture.wait_for(lambda:invoke('trace-state',handle)=='1')
     for handle in (handles[0],handles[1],handles[3]):assert invoke('trace-finish',handle)=='OK'
     for handle in (handles[0],handles[1],handles[3]):assert json.loads(invoke('trace-result',handle))['status']=='completed'
     if exported in ('all','server'):fixture.wait_for(lambda:any(span['name']=='CancelNexusOperation:v.trace/cancel' for span in spans()))
     all_handles=handles+[backing(handles[1]),cancelled]
     history=fixture.history(cancelled)
     for event in history['events']:
      if 'childWorkflowExecutionStartedEventAttributes' in event:
       execution=event['childWorkflowExecutionStartedEventAttributes']['workflowExecution'];all_handles.append({'workflow_id':execution['workflowId'],'run_id':execution['runId']})
     assert len(all_handles)==7
     offline=files/'offline.jsonl';env=dict(fixture.env,V_TRACE_JOURNAL=str(offline));nexus_headers=0
     for index,handle in enumerate(all_handles):
      history=fixture.history(handle);inspect_propagation(history,trace_id,'tenant=sampling')
      assert not any('workflowTaskFailedEventAttributes' in event for event in history['events'])
      for event in history['events']:
       if 'nexusOperationScheduledEventAttributes' in event:
        header=event['nexusOperationScheduledEventAttributes']['nexusHeader'];nexus_headers+=1
        assert header['application']=='value' and header['baggage']=='tenant=sampling' and header['traceparent'].split('-')[1]==trace_id,header
        assert bool(int(header['traceparent'].split('-')[3],16)&1)==(exported in ('all','producer')),header
        if exported=='producer':assert header['tracestate']=='sampler=producer',header
       for key,attributes in event.items():
        if not key.endswith('EventAttributes') or not isinstance(attributes,dict):continue
        for payload in carriers(attributes):
         carrier=json.loads(base64.b64decode(payload['data']))
         if exported=='producer':
          expected=key in ('activityTaskScheduledEventAttributes','startChildWorkflowExecutionInitiatedEventAttributes','childWorkflowExecutionStartedEventAttributes') or (key=='workflowExecutionStartedEventAttributes' and handle['workflow_id'].endswith('-child'))
          assert bool(int(carrier['traceparent'].split('-')[3],16)&1)==expected,(key,carrier)
         else:assert bool(int(carrier['traceparent'].split('-')[3],16)&1)==(exported=='all'),(policy,key,carrier)
      history_file=files/f'{index}.bin';assert invoke('trace-history',handle,history_file)=='OK'
      assert invoke('trace-replay',history_file,handle['workflow_id'],fixture.queue,env=env)=='OK';report['histories']+=1
     assert nexus_headers==4 and (not offline.exists() or offline.stat().st_size==0)
     records=spans();assert all(span['context']['trace_id']==trace_id for span in records)
     if exported=='none':assert not records
     elif exported=='server':assert records and all(span['parent_remote'] for span in records),(policy,{(span['kind'],span['parent_remote']) for span in records})
     elif exported!='all':assert records and all(span['kind']==exported for span in records),(policy,{span['kind'] for span in records})
     if exported in ('all','producer'):assert len([span for span in records if span['kind']=='producer'])==7,'replay duplicated producer spans'
     report['cases'].append(f'{version}/{policy}: actual Nexus string headers and workflow-backed helper payload headers preserved sampling decisions/state and baggage across sync/failure/async cancellation, worker replacement and seven offline histories')
  report['passed']=True
 finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

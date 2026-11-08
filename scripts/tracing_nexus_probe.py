"""Nexus tracing through real endpoints, async workflow callbacks and cancellation."""
RESULT_REPORT='tracing-nexus-results.json'
import argparse,json,signal,subprocess,uuid
from pathlib import Path
from propagation_wire import inspect_propagation
from integration import ARTIFACTS,ROOT,TemporalFixture

def main(binary,report_name,profiles=('', 'prefix','context'),storage_modes=(False,True),baggage=''):
    report={'passed':False,'histories':0,'cases':[]}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for profile in profiles:
                for storage in storage_modes:
                    with TemporalFixture(f'trace-nexus-{version}-{profile or "default"}-{storage}',server_binary=server) as fixture:
                        files=ARTIFACTS/'tracing-nexus-fixtures'/fixture.queue;files.mkdir(parents=True);journal=files/'live.jsonl';endpoint=fixture.queue+'-endpoint'
                        fixture.env.update(V_TRACE_ID=uuid.uuid4().hex,V_TRACE_JOURNAL=str(journal),V_TRACE_NEXUS_ENDPOINT=endpoint,V_SDK_EXAMPLE_CODEC=profile,V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage') if storage else '')
                        created=subprocess.run([str(server),'operator','nexus','endpoint','create','--address',fixture.address,'--name',endpoint,'--target-namespace','default','--target-task-queue',fixture.queue],capture_output=True,text=True,timeout=20);assert created.returncode==0,created.stderr
                        fixture.env['V_TRACE_BAGGAGE']=baggage
                        def invoke(role,*args,env=None):
                            result=subprocess.run([str(binary),role,*map(lambda value:json.dumps(value) if isinstance(value,dict) else str(value),args)],cwd=ROOT,env=env or fixture.env,capture_output=True,text=True,timeout=45)
                            with (files/'calls.log').open('a') as output:output.write(role+'\n'+result.stdout+result.stderr)
                            assert result.returncode==0,(role,result.stdout,result.stderr)
                            return result.stdout.strip()
                        def spans():return [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []
                        worker=fixture.launch(binary,'before',('trace-worker',));handles=json.loads(invoke('trace-nexus-start'))
                        for handle in handles[:2]:fixture.wait_for(lambda:invoke('trace-state',handle)=='1')
                        failed=json.loads(invoke('trace-result',handles[2]));assert failed['status']=='failed',failed
                        def backing(handle):
                            history=fixture.history(handle)
                            inspect_propagation(history,fixture.env['V_TRACE_ID'],baggage)
                            started=[event for event in history['events'] if 'nexusOperationStartedEventAttributes' in event]
                            if not started:return None
                            # Closed operations can have their tokens redacted.
                            # The workflow event link retains the backing identity.
                            assert len(started)==1,started
                            links=[link['workflowEvent'] for link in started[0].get('links',[]) if 'workflowEvent' in link]
                            assert len(links)==1,links
                            linked=links[0]
                            assert linked['namespace']=='default' and linked['eventRef']['eventType']=='EVENT_TYPE_WORKFLOW_EXECUTION_STARTED',linked
                            assert linked['workflowId'] and linked['runId'],linked
                            return {'workflow_id':linked['workflowId'],'run_id':linked['runId']}
                        fixture.wait_for(lambda:backing(handles[3]) is not None)
                        cancelled_backing=backing(handles[3]);fixture.wait_for(lambda:invoke('trace-state',cancelled_backing)=='3')
                        before=len([span for span in spans() if span['name'].startswith('StartNexusOperation:') and span['kind']=='server'])
                        assert before==4,before
                        worker.kill();worker.wait(timeout=10);fixture.launch(binary,'after',('trace-worker',))
                        for handle in handles[:2]:fixture.wait_for(lambda:invoke('trace-state',handle)=='1')
                        for handle in (handles[0],handles[1],handles[3]):assert invoke('trace-finish',handle)=='OK'
                        for handle in (handles[0],handles[1],handles[3]):assert json.loads(invoke('trace-result',handle))['status']=='completed'
                        fixture.wait_for(lambda:any(span['name']=='CancelNexusOperation:v.trace/cancel' for span in spans()))
                        observed=spans();assert all(span['context']['trace_id']==fixture.env['V_TRACE_ID'] for span in observed)
                        assert len([span for span in observed if span['name'].startswith('StartNexusOperation:') and span['kind']=='server'])==before,'replay called Nexus handlers'
                        by_id={span['context']['span_id']:span for span in observed}
                        for span in observed:
                            if span['name'].startswith('StartNexusOperation:') and span['kind']=='server':assert by_id[span['parent_span_id']]['kind']=='producer'
                            if span['name']=='StartWorkflowExecution' and span['parent_span_id'] in by_id:assert by_id[span['parent_span_id']]['name'] in ('StartNexusOperation:v.trace/async','StartNexusOperation:v.trace/cancel')
                        assert any(span['name']=='StartNexusOperation:v.trace/fail' and span['kind']=='server' and span['status']=='error' for span in observed)
                        all_handles=handles+[backing(handles[1]),cancelled_backing]
                        history=fixture.history(cancelled_backing)
                        all_handles += [{'workflow_id':event['childWorkflowExecutionStartedEventAttributes']['workflowExecution']['workflowId'],'run_id':event['childWorkflowExecutionStartedEventAttributes']['workflowExecution']['runId']} for event in history['events'] if 'childWorkflowExecutionStartedEventAttributes' in event]
                        offline=files/'offline.jsonl';env=dict(fixture.env,V_TRACE_JOURNAL=str(offline))
                        for index,handle in enumerate(all_handles):
                            history_file=files/f'{index}.bin';assert invoke('trace-history',handle,history_file)=='OK'
                            assert invoke('trace-replay',history_file,handle['workflow_id'],fixture.queue,env=env)=='OK';report['histories']+=1
                        assert not offline.exists() or offline.stat().st_size==0
                        report['cases'].append(f'{version}/{profile or "default"}/storage={storage}: Nexus sync/failure/async-workflow/cancel spans and helper RPC ancestry survived worker replacement; callback and child histories replayed without handler or span duplication')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true')
    args=parser.parse_args();main(args.binary,args.report,profiles=('',) if args.quick else ('','prefix','context'),storage_modes=(False,) if args.quick else (False,True))

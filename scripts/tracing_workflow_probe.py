"""Tracing across live commands, worker replacement, failure, cancellation and offline replay."""
RESULT_REPORT='tracing-workflow-results.json'
import argparse,collections,json,os,signal,subprocess,uuid
from pathlib import Path
from propagation_wire import inspect_propagation
from integration import ARTIFACTS,ROOT,TemporalFixture

def main(binary,report_name,profiles=('', 'prefix','context'),storage_modes=(False,True),baggage=''):
    report={'passed':False,'histories':0,'cases':[]}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for profile in profiles:
                for storage in storage_modes:
                    with TemporalFixture(f'tracing-{version}-{profile or "default"}-{storage}',server_binary=server) as fixture:
                        files=ARTIFACTS/'tracing-fixtures'/fixture.queue;files.mkdir(parents=True)
                        journal=files/'live.jsonl'
                        fixture.env.update(V_TRACE_ID=uuid.uuid4().hex,V_TRACE_JOURNAL=str(journal),V_SDK_EXAMPLE_CODEC=profile,V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage') if storage else '')
                        fixture.env['V_TRACE_BAGGAGE']=baggage
                        def invoke(role,*args,env=None):
                            result=subprocess.run([str(binary),role,*map(lambda value:json.dumps(value) if isinstance(value,dict) else str(value),args)],cwd=ROOT,env=env or fixture.env,capture_output=True,text=True,timeout=45)
                            with (files/'calls.log').open('a') as output:output.write(role+'\n'+result.stdout+result.stderr)
                            assert result.returncode==0,(role,result.stdout,result.stderr)
                            return result.stdout.strip()
                        def records():return [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []
                        worker=fixture.launch(binary,'before',('trace-worker',))
                        handles=json.loads(invoke('trace-start'))
                        for handle in handles:
                            fixture.wait_for(lambda:invoke('trace-state',handle)=='3')
                        fixture.wait_for(lambda:len([span for span in records() if span['name']=='WorkflowEvent:started'])==8)
                        started={span['context']['span_id'] for span in records() if span['name']=='WorkflowEvent:started'}
                        worker.send_signal(signal.SIGKILL);worker.wait(timeout=10)
                        fixture.launch(binary,'after',('trace-worker',))
                        for handle in handles:fixture.wait_for(lambda:invoke('trace-state',handle)=='3')
                        for handle in handles:assert invoke('trace-update',handle)=='OK'
                        for index,handle in enumerate(handles):assert invoke('trace-cancel' if index==2 else 'trace-finish',handle)=='OK'
                        for index,handle in enumerate(handles):
                            result=json.loads(invoke('trace-result',handle))
                            assert result['status']==['completed','failed','canceled','completed'][index],result
                        fixture.wait_for(lambda:len([span for span in records() if span['name']=='RunWorkflow:v.trace.workflow'])==9)
                        spans=records();identities=[span['context']['span_id'] for span in spans]
                        assert len(identities)==len(set(identities)),collections.Counter(identities)
                        assert {span['context']['span_id'] for span in spans if span['name']=='WorkflowEvent:started' and span['context']['span_id'] in started}==started
                        assert len([span for span in spans if span['name']=='WorkflowEvent:started'])==9,'replay duplicated workflow start spans'
                        assert all(span['context']['trace_id']==fixture.env['V_TRACE_ID'] for span in spans)
                        by_id={span['context']['span_id']:span for span in spans}
                        for span in spans:
                            if span['name']=='RunActivity:v.trace.activity':
                                parent=by_id[span['parent_span_id']]
                                assert parent['name'] in ('StartActivity:v.trace.activity','StartLocalActivity:v.trace.activity'),parent
                            if span['name']=='RunWorkflow:v.trace.workflow':
                                parent=by_id[span['parent_span_id']]
                                assert parent['name'] in ('StartWorkflowExecution','StartChildWorkflow:v.trace.workflow','ContinueAsNew'),parent
                        assert len([span for span in spans if span['name']=='RunActivity:v.trace.activity'])==8
                        failures=[span for span in spans if span['status']=='error']
                        assert any(span['name']=='RunWorkflow:v.trace.workflow' for span in failures),failures
                        all_handles=list(handles)
                        for handle in handles:
                            history=fixture.history(handle)
                            inspect_propagation(history,fixture.env['V_TRACE_ID'],baggage,require_memo_storage=storage)
                            all_handles += [{'workflow_id':event['childWorkflowExecutionStartedEventAttributes']['workflowExecution']['workflowId'],'run_id':event['childWorkflowExecutionStartedEventAttributes']['workflowExecution']['runId']} for event in history['events'] if 'childWorkflowExecutionStartedEventAttributes' in event]
                            all_handles += [{'workflow_id':handle['workflow_id'],'run_id':event['workflowExecutionContinuedAsNewEventAttributes']['newExecutionRunId']} for event in history['events'] if 'workflowExecutionContinuedAsNewEventAttributes' in event]
                        assert len(all_handles)==9
                        offline=files/'offline.jsonl';offline_env=dict(fixture.env,V_TRACE_JOURNAL=str(offline))
                        for index,handle in enumerate(all_handles):
                            history=files/f'{index}.bin';assert invoke('trace-history',handle,history)=='OK'
                            assert invoke('trace-replay',history,handle['workflow_id'],fixture.queue,env=offline_env)=='OK'
                            report['histories']+=1
                        assert not offline.exists() or offline.stat().st_size==0,'offline replay exported traces'
                        report['cases'].append(f'{version}/{profile or "default"}/storage={storage}: concurrent local/remote Activities, child workflows, query/Update, SIGKILL/replacement, failure/cancellation and Continue-as-New retained trace ancestry and application headers; nine histories replayed without exporting spans')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true')
    args=parser.parse_args();main(args.binary,args.report,profiles=('',) if args.quick else ('','prefix','context'),storage_modes=(False,) if args.quick else (False,True))

"""Frozen actual Go SDK OTEL handoffs: independent header/span checks and V replay."""
RESULT_REPORT='tracing-go-results.json'
import argparse,base64,hashlib,json,os,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT
from propagation_wire import carriers

def main(binary,report_name):
    report={'passed':False,'histories':0,'cases':[]}
    try:
        directory=ROOT/'_tests/data/tracing-go';manifest=json.loads((directory/'manifest.json').read_text())
        assert manifest['passed'] and len(manifest['cases'])==4
        reference=manifest['reference'];assert (reference['sdk'],reference['opentelemetry_interceptor'],reference['opentelemetry'])==('v1.35.0','v0.6.0','v1.43.0')
        for name,expected in reference['sources'].items():assert hashlib.sha256((ROOT/'_tests/native/tracing_temporal_go'/name).read_bytes()).hexdigest()==expected,name
        for case in manifest['cases']:
            for name,expected in case['files'].items():assert hashlib.sha256((directory/name).read_bytes()).hexdigest()==expected,name
            prefix=case['server']+'-'+case['direction'];history=json.loads((directory/(prefix+'.json')).read_text())
            assert not any('workflowTaskFailedEventAttributes' in event for event in history['events'])
            go=json.loads((directory/(prefix+'-go-spans.json')).read_text());v=json.loads((directory/(prefix+'-v-spans.json')).read_text())
            assert go and v
            spans={span['span_id']:span for span in go}
            spans.update({span['context']['span_id']:{'name':span['name'],'trace_id':span['context']['trace_id'],'span_id':span['context']['span_id'],'parent_span_id':span['parent_span_id']} for span in v})
            assert all(span['trace_id']==case['trace_id'] for span in spans.values())
            found=[];markers=[]
            for payload in carriers(history):
                metadata={key:base64.b64decode(value) for key,value in payload['metadata'].items()};assert metadata['encoding']==b'json/plain'
                carrier=json.loads(base64.b64decode(payload['data']));assert carrier['traceparent'].split('-')[1]==case['trace_id'] and carrier['baggage']==case['baggage']
                span_id=carrier['traceparent'].split('-')[2];assert span_id in spans,(span_id,case['direction'])
                found.append(carrier);markers.append(metadata.get('temporal-v/propagation'))
            assert found==case['carriers'] and b'1' in markers and None in markers,'both actual SDK carriers must be present'
            workflow_spans=[span for span in spans.values() if span['name']=='RunWorkflow:v.trace.interop']
            assert workflow_spans and all(span['parent_span_id']==found[0]['traceparent'].split('-')[2] for span in workflow_spans)
            journal=ARTIFACTS/(report_name+'-'+prefix+'.jsonl');assert not journal.exists() or journal.stat().st_size==0
            environment={key:value for key,value in os.environ.items() if not key.startswith(('TEMPORAL_','V_TRACE_','V_SDK_EXAMPLE_'))}
            environment.update(V_TRACE_ID=case['trace_id'],V_TRACE_BAGGAGE=case['baggage'],V_TRACE_JOURNAL=str(journal))
            result=subprocess.run([str(binary),'trace-replay',str(directory/(prefix+'.bin')),case['handle']['workflow_id'],case['task_queue']],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
            (ARTIFACTS/f'logs/{report_name}-{prefix}.log').write_text(result.stdout+result.stderr)
            assert result.returncode==0 and result.stdout.strip()=='OK',result.stdout+result.stderr
            assert not journal.exists() or journal.stat().st_size==0,'offline replay exported spans'
            report['histories']+=1;report['cases'].append(prefix+': actual Go Temporal SDK/OpenTelemetry and V worker handoff retained state, trace ancestry and baggage; both SDKs replayed the generated history; independent frozen carrier/span inspection and callback-free V replay passed')
        report['reference']=reference;report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

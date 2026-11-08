"""Optional live Go/V worker handoffs using the actual Go SDK OpenTelemetry interceptor."""
import argparse,base64,hashlib,json,subprocess,uuid
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
from propagation_wire import carriers

def main(binary,reference,destination):
    destination.mkdir(parents=True,exist_ok=True)
    sources={name:hashlib.sha256((ROOT/'_tests/native/tracing_temporal_go'/name).read_bytes()).hexdigest() for name in ('main.go','go.mod','go.sum')}
    manifest={'passed':False,'reference':{'sdk':'v1.35.0','opentelemetry_interceptor':'v0.6.0','opentelemetry':'v1.43.0','go_toolchain':subprocess.check_output(['go','version'],text=True).strip(),'sources':sources,'binary_sha256':hashlib.sha256(reference.read_bytes()).hexdigest()},'cases':[]}
    for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
        for direction in ('go-to-v','v-to-go'):
            with TemporalFixture('trace-go-'+version+'-'+direction,server_binary=server) as fixture:
                trace_id=uuid.uuid4().hex;files=ARTIFACTS/'tracing-go-live'/fixture.queue;files.mkdir(parents=True)
                go_journal=files/'go.jsonl';v_journal=files/'v.jsonl'
                fixture.env.update(V_TRACE_ID=trace_id,V_TRACE_BAGGAGE='tenant=interop',V_TRACE_JOURNAL=str(v_journal),V_TRACE_GO_JOURNAL=str(go_journal))
                def invoke(executable,role,*args):
                    result=subprocess.run([str(executable),role,*map(lambda value:json.dumps(value) if isinstance(value,dict) else str(value),args)],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    with (files/'calls.log').open('a') as output:output.write(role+'\n'+result.stdout+result.stderr)
                    assert result.returncode==0,(role,result.stdout,result.stderr)
                    return result.stdout.strip()
                first,second=(reference,binary) if direction=='go-to-v' else (binary,reference)
                worker=fixture.launch(first,'before',('worker' if first==reference else 'trace-worker',))
                handle=json.loads(invoke(first,'start' if first==reference else 'trace-interop-start'))
                assert invoke(first,'ready' if first==reference else 'trace-ready',handle)=='OK'
                fixture.wait_for(lambda:invoke(first,'state' if first==reference else 'trace-state',handle)=='1')
                worker.terminate();assert worker.wait(timeout=20)==0
                assert invoke(reference,'reset-sticky',handle)=='OK'
                fixture.launch(second,'after',('worker' if second==reference else 'trace-worker',))
                fixture.wait_for(lambda:invoke(second,'state' if second==reference else 'trace-state',handle)=='1')
                assert invoke(second,'finish' if second==reference else 'trace-finish',handle)=='OK'
                if second==reference:assert invoke(second,'result',handle)=='OK'
                else:
                    result=json.loads(invoke(second,'trace-result',handle));assert result['status']=='completed' and result['data']=='"interop"',result
                history=fixture.history(handle)
                assert not any('workflowTaskFailedEventAttributes' in event for event in history['events']),history
                carrier_values=[]
                for payload in carriers(history):
                    assert base64.b64decode(payload['metadata']['encoding'])==b'json/plain'
                    carrier=json.loads(base64.b64decode(payload['data']));assert carrier['traceparent'].split('-')[1]==trace_id and carrier.get('baggage')=='tenant=interop',carrier
                    carrier_values.append(carrier)
                assert len(carrier_values)>=3,carrier_values
                prefix=version+'-'+direction;json_path=destination/(prefix+'.json');binary_path=destination/(prefix+'.bin')
                json_path.write_text(json.dumps(history,indent=2)+'\n')
                assert invoke(binary,'trace-history',handle,binary_path)=='OK'
                assert invoke(reference,'replay',json_path)=='OK'
                before=v_journal.read_bytes() if v_journal.exists() else b''
                assert invoke(binary,'trace-replay',binary_path,handle['workflow_id'],fixture.queue)=='OK'
                assert v_journal.read_bytes()==before,'V offline replay exported spans'
                go_spans=[json.loads(line) for line in go_journal.read_text().splitlines()]
                v_spans=[json.loads(line) for line in v_journal.read_text().splitlines()]
                assert go_spans and v_spans
                assert all(span['trace_id']==trace_id for span in go_spans),go_spans
                assert all(span['context']['trace_id']==trace_id for span in v_spans),v_spans
                go_path=destination/(prefix+'-go-spans.json');go_path.write_text(json.dumps(go_spans,indent=2)+'\n')
                v_path=destination/(prefix+'-v-spans.json');v_path.write_text(json.dumps(v_spans,indent=2)+'\n')
                manifest['cases'].append({'server':version,'direction':direction,'handle':handle,'task_queue':fixture.queue,'trace_id':trace_id,'baggage':'tenant=interop','carriers':carrier_values,'files':{path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in (json_path,binary_path,go_path,v_path)}})
    manifest['passed']=True;(destination/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps({'passed':True,'histories':len(manifest['cases']),'destination':str(destination)},indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--reference',type=Path,required=True);parser.add_argument('--destination',type=Path,default=ROOT/'_tests/data/tracing-go')
    args=parser.parse_args();main(args.binary,args.reference,args.destination)

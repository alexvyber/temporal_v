"""Typed query/Update/remote and local Activity callbacks through interceptors and replay."""
RESULT_REPORT='typed-callbacks-results.json'
import argparse,json,subprocess,base64,tempfile
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
BINARY=ARTIFACTS/'tools/v-mixed-values'
def invoke(f,mode,*args,success=True):
 p=subprocess.run([str(BINARY),mode,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=25)
 (ARTIFACTS/f'logs/{f.name}-{mode}.log').write_text(p.stdout+p.stderr)
 if not success:return p
 assert p.returncode==0,p.stderr
 return 'OK' if p.stdout.strip()=='OK' else json.loads(p.stdout)
def payload_value(payload,codec,kind):
 encoding=base64.b64decode(payload['metadata']['encoding']).decode()
 assert encoding=={'binary':'binary/plain','protobuf':'binary/protobuf','null':'binary/null','binary-null':'binary/plain'}[kind],encoding
 data=base64.b64decode(payload.get('data',''))
 if codec:assert data[:3]==bytes([0,255,90]);data=data[3:]
 if kind=='binary':assert data==bytes([0,255,0,128])
 if kind=='binary-null':assert data==b'null'
 if kind=='null':assert data==b''
 if kind=='protobuf':assert base64.b64decode(payload['metadata']['messageType'])==b'temporal.api.common.v1.WorkflowExecution' and b'typed-result' in data
 return data
def exercise():
    replays=[]
    for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
     for codec in (False,True):
      with tempfile.TemporaryDirectory(prefix='mixed-tokens-',dir=ARTIFACTS) as directory, TemporalFixture(f'typed-callback-{label}-{codec}',server_binary=server) as f:
       f.env.update(V_SDK_MIXED_CODEC='true' if codec else 'false',V_SDK_MIXED_TOKEN_DIR=directory,TEMPORAL_MAX_CACHED_WORKFLOWS='2')
       w=f.launch(BINARY,'before',('worker',))
       direct=[(kind,invoke(f,'start',f.queue+'-'+kind,'callback.'+kind)) for kind in ('binary','protobuf','null','binary-null')]
       deferred=[(kind,invoke(f,'start',f.queue+'-local-'+kind,'local.'+kind)) for kind in ('binary','protobuf','null','binary-null')]
       for kind,h in direct:f.wait_for(lambda:invoke(f,'query',json.dumps(h),success=False).returncode==0)
       for kind,h in deferred:f.wait_for(lambda:invoke(f,'query',json.dumps(h),success=False).returncode==0)
       w.kill();assert w.wait(timeout=10)==-9
       w=f.launch(BINARY,'after',('worker',))
       for index,(kind,h) in enumerate(direct+deferred):
        f.wait_for(lambda:invoke(f,'query',json.dumps(h),success=False).returncode==0)
        query=invoke(f,'query-wire',json.dumps(h))
        update=invoke(f,'update-wire',json.dumps(h))
        assert update==query and invoke(f,'update-wire',json.dumps(h))==query,(query,update)
        invoke(f,'finish',json.dumps(h));outcome=invoke(f,'result-wire',json.dumps(h));assert outcome['status']=='completed'
        history=f.history(h);f.assert_clean_history(h)
        completed=[event['workflowExecutionCompletedEventAttributes'] for event in history['events'] if 'workflowExecutionCompletedEventAttributes' in event]
        data=payload_value(completed[0]['result']['payloads'][0],codec,kind)
        assert outcome['raw_hex']==data.hex() and outcome['raw_kind']=={'binary':'binary','protobuf':'protobuf','null':'null_value','binary-null':'binary'}[kind],outcome
        assert outcome['data_hex']==(b'null' if kind=='null' else data).hex(),outcome
        assert query=={'raw_kind':outcome['raw_kind'],'raw_hex':outcome['raw_hex']}
        if index<len(direct):
         activity=[event['activityTaskCompletedEventAttributes'] for event in history['events'] if 'activityTaskCompletedEventAttributes' in event]
         assert payload_value(activity[0]['result']['payloads'][0],codec,kind)==data
        path=ARTIFACTS/f'binary-histories/{f.name}-{index}.bin';f.call('history',h,file=path);replays.append((path,h['workflow_id'],f.queue,f.env.copy()))
       w.terminate();assert w.wait(timeout=20)==0
      print(label,codec,'LIVE_OK',flush=True)
    for history,id,queue,environment in replays:
     p=subprocess.run([str(BINARY),'replay',str(history),id,queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=40)
     assert p.returncode==0,p.stderr
    assert len(replays)==32
    return len(replays)
def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary
    report={'passed':False,'cases':[]}
    try:
        report['histories']=exercise()
        report['cases'].append('Both server versions with/without codecs: binary/protobuf/null/null-byte remote and local Activity callbacks, two Activity/query interceptors, typed query/Update results and Update dedupe, independent wire histories, cache pressure, SIGKILL/replacement and 32 offline replays')
        report['passed']=True
    finally:
        (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--binary',type=Path,default=BINARY)
    parser.add_argument('--report',choices=(RESULT_REPORT,'typed-callbacks-sanitized-results.json'),default=RESULT_REPORT)
    arguments=parser.parse_args()
    main(arguments.binary,arguments.report)

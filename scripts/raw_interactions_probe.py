"""Signals/queries use named precedence and owned raw dynamic fallback."""
RESULT_REPORT='raw-interactions-results.json'
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import time
from integration import ARTIFACTS,ROOT,TemporalFixture,counts


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0,'transient_query_errors':[]}
    payload=b'\xff\0\x80'+b'x'*320000
    expected={'encoding':'application/x-temporal-v-dynamic','prefix':'ff0080','length':str(len(payload)),'sha256':hashlib.sha256(payload).hexdigest()}
    try:
        profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['','prefix','context'] for stored in [False,True]]
        if quick:profiles=profiles[:1]
        for version,server,codec,stored in profiles:
            with TemporalFixture('raw-interactions-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir()
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_RAW_INTERACTIONS_FIXED='false',V_RAW_DECL_FIXED='false')
                if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory))
                worker=fixture.launch(binary,'before',('worker',))
                handles=[fixture.start('dynamic','v.raw-interactions',{}),fixture.start('named','v.named-raw-interactions',{})]
                def state(handle,repairing_signal=False):
                    deadline=time.monotonic()+60
                    while True:
                        assert worker.poll() is None,'worker exited while waiting for query'
                        try:return json.loads(fixture.call('query',handle,name='state',rpc_ms=5000)['data'])
                        except AssertionError as failure:
                            try:detail=json.loads(str(failure).splitlines()[-1])
                            except (ValueError,IndexError):raise failure
                            retryable=str(detail.get('code'))=='4' or (str(detail.get('code'))=='1' and detail.get('message')=='h2 protocol error: http2 error')
                            # The server can reject queries until the replacement
                            # commits the task that the deliberately broken signal
                            # handler failed. Only tolerate that known failure while
                            # waiting for this repair, and retain the final history
                            # assertions below so unexpected task failures still fail.
                            if repairing_signal and str(detail.get('code'))=='9' and detail.get('message')=='Unable to query workflow due to Workflow Task in failed state.':
                                failures=[event['workflowTaskFailedEventAttributes'].get('failure',{}).get('message','') for event in fixture.history(handle)['events'] if 'workflowTaskFailedEventAttributes' in event]
                                assert failures and all(message=='dynamic signal panic' for message in failures),failures
                                retryable=True
                            if not retryable or time.monotonic()>=deadline:raise
                            report['transient_query_errors'].append(dict(detail,workflow_id=handle['workflow_id']))
                            time.sleep(0.05)
                def raw(handle,kind,name):
                    result=subprocess.run([str(binary),'interactions-opaque',json.dumps(handle),kind,name],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=25)
                    assert result.returncode==0,result.stdout+result.stderr
                    return result.stdout.strip()
                for index,handle in enumerate(handles):
                    fixture.wait_for(lambda:state(handle)==[])
                    fixture.env['V_SDK_EXAMPLE_CODEC']=''
                    try:metadata=json.loads(fixture.call('query',handle,name='__temporal_workflow_metadata')['data'])['definition']
                    finally:fixture.env['V_SDK_EXAMPLE_CODEC']=codec
                    assert [item.get('name','') for item in metadata.get('queryDefinitions',[])]==([''] if index==0 else ['','fixed','state']),metadata
                    assert [item.get('name','') for item in metadata.get('signalDefinitions',[])]==([''] if index==0 else ['','finish','fixed']),metadata
                    for name in ['opaque','different-name']:
                        response=json.loads(raw(handle,'query',name));assert response==dict(expected,name=name),response
                    if index==0:
                        response=json.loads(raw(handle,'query','fixed'));assert response==dict(expected,name='fixed'),response
                    else:
                        assert raw(handle,'named-reject','fixed')=='NAMED_RAW_REJECTED'
                        assert fixture.call('query',handle,name='fixed',input='"named"')['data']=='"named-query"'
                        fixture.call('signal',handle,name='fixed',input='"converted"')
                        fixture.wait_for(lambda:state(handle)==['named:"converted"'])
                    failure=fixture.call('query',handle,name='query-panic',success=False);assert 'dynamic query panic' in json.dumps(failure),failure
                    assert raw(handle,'signal','opaque')=='SENT'
                    wanted=1 if index==0 else 2
                    fixture.wait_for(lambda:len(state(handle))==wanted)
                    assert state(handle)[-1]=='opaque:application/x-temporal-v-dynamic:ff0080'
                assert worker.poll() is None
                bad_handles=[]
                for workflow_type,expected_error in [('v.bad-raw-query','invalid or duplicate named query'),('v.bad-raw-signal','invalid or duplicate named signal'),('v.ambiguous-raw-query','combined named and dynamic queries require query_names')]:
                    bad=fixture.start(workflow_type,workflow_type,{})
                    bad_handles.append(bad)
                    fixture.wait_for(lambda:counts(fixture.history(bad))['workflowtaskfailed']>=1)
                    bad_history=fixture.history(bad)
                    errors=[event['workflowTaskFailedEventAttributes'].get('failure',{}).get('message','') for event in bad_history['events'] if 'workflowTaskFailedEventAttributes' in event]
                    assert errors and all(expected_error in message for message in errors),errors
                    assert counts(bad_history)['workflowexecutioncompleted']==0
                    assert counts(bad_history)['workflowexecutionfailed']==0
                raw(handles[0],'signal','signal-panic')
                fixture.wait_for(lambda:counts(fixture.history(handles[0]))['workflowtaskfailed']>=1)
                # A dynamic signal failure affects its task, while another run continues.
                raw(handles[1],'signal','second')
                fixture.wait_for(lambda:len(state(handles[1]))==3)
                worker.kill();worker.wait(timeout=10)
                fixture.env['V_RAW_INTERACTIONS_FIXED']='true';fixture.env['V_RAW_DECL_FIXED']='true';worker=fixture.launch(binary,'after',('worker',))
                fixture.wait_for(lambda:len(state(handles[0],repairing_signal=True))==2)
                assert state(handles[0])[-1]=='signal-panic:application/x-temporal-v-dynamic:ff0080'
                assert state(handles[1])==['named:"converted"','opaque:application/x-temporal-v-dynamic:ff0080','second:application/x-temporal-v-dynamic:ff0080']
                for index,handle in enumerate(handles):
                    response=json.loads(raw(handle,'query','after-restart'));assert response==dict(expected,name='after-restart')
                    fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['status']=='completed'
                    history=fixture.history(handle)
                    failed=[event['workflowTaskFailedEventAttributes'].get('failure',{}).get('message','') for event in history['events'] if 'workflowTaskFailedEventAttributes' in event]
                    assert all(message=='dynamic signal panic' for message in failed),failed
                    assert bool(failed)==(index==0),failed
                    signal=next(event['workflowExecutionSignaledEventAttributes'] for event in history['events'] if event.get('workflowExecutionSignaledEventAttributes',{}).get('signalName')=='opaque')
                    payloads=signal['input']['payloads'];message_type=base64.b64decode(payloads[0].get('metadata',{}).get('messageType',''))
                    assert (message_type==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,message_type
                    file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
                    replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0,replay.stdout+replay.stderr
                    report['histories']+=1
                for offset,bad in enumerate(bad_handles,2):
                    result=fixture.call('result',bad)
                    assert result['status']=='completed' and result['data']=='"validated"',result
                    file=directory/(str(offset)+'.bin');fixture.call('history',bad,file=file)
                    replay=subprocess.run([str(binary),'replay',str(file),bad['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0,replay.stdout+replay.stderr
                    report['histories']+=1
                worker.terminate();assert worker.wait(timeout=20)==0
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: dynamic-only and named-priority raw signals/queries, opaque bytes and owned payload views, metadata, query panic isolation, signal panic task failure, other-run progress, three malformed declarations rejected without closing executions; SIGKILL/repair and five offline histories passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

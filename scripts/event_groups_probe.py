"""Experimental Event Groups persist correlation metadata without changing replay."""
RESULT_REPORT='event-groups-results.json'
import argparse
import base64
import json
from pathlib import Path
import subprocess
from fixture_payload_codec import decode_fixture_payload
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture,counts


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0}
    try:
        profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')]
                  for codec in ['', 'prefix', 'context'] for stored in [False,True]]
        if quick:profiles=profiles[:1]
        for version,server,codec,stored in profiles:
            with TemporalFixture('event-groups-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,
                                 server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir()
                (directory/'nexus').mkdir();fixture.env['V_PANIC_NEXUS_DIR']=str(directory/'nexus')
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_EVENT_GROUPS='true',V_GROUP_POISON='true',V_PANIC_FIXED='true',V_PANIC_FAIL_WORKFLOW='false')
                if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
                endpoint=fixture.queue.replace('_','-')+'-endpoint'
                created=subprocess.run([str(CLI),'operator','nexus','endpoint','create','--address',fixture.address,'--name',endpoint,'--target-namespace','default','--target-task-queue',fixture.queue],capture_output=True,text=True,timeout=20)
                assert created.returncode==0,created.stderr
                attribute=subprocess.run([str(CLI),'operator','search-attribute','create','--address',fixture.address,'--name','VEventGroupKeyword','--type','Keyword'],capture_output=True,text=True,timeout=20)
                assert attribute.returncode==0,attribute.stderr
                worker=fixture.launch(binary,'before',('worker',));handle=fixture.start('groups','v.event-groups',{'endpoint':endpoint})
                def state():return json.loads(fixture.call('query',handle,name='state',rpc_ms=20000)['data'])
                fixture.wait_for(lambda:state()['ready']=='true')
                fixture.call('signal',handle,name='group')
                fixture.wait_for(lambda:state()['signals']=='1')
                immediate=fixture.call('update',handle,name='immediate',update_id='client-group-immediate')
                assert immediate['status']=='completed' and immediate['data']=='"immediate-ok"',immediate
                pending=fixture.call('update',handle,name='deferred',update_id='client-group-deferred',wait_ms=100)
                assert pending['status']=='accepted',pending
                worker.kill();assert worker.wait(timeout=10)==-9
                worker=fixture.launch(binary,'after',('worker',))
                fixture.wait_for(lambda:state()['deferred']=='true')
                deferred=fixture.call('update',handle,name='deferred',update_id='client-group-deferred')
                assert deferred['status']=='completed' and deferred['data']=='"deferred-ok"',deferred
                fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['data']=='"groups-ok"'
                history=fixture.history(handle);assert counts(history)['workflowtaskfailed']==0
                (directory/'history.json').write_text(json.dumps(history,indent=2)+'\n')

                def marker_key(marker):
                    if 'label' in marker:return 'label:'+marker['label']['id']
                    if 'inboundEvent' in marker:return 'event:'+str(marker['inboundEvent']['inboundEventId'])
                    if 'inboundUpdate' in marker:return 'update:'+marker['inboundUpdate']['inboundUpdateId']
                    raise AssertionError(marker)
                def groups(event):return [marker_key(marker) for marker in event.get('eventGroupMarkers',[])]
                def label_text(payload,owner=None):
                    metadata,data=decode_fixture_payload(payload,codec)
                    assert metadata['encoding']==b'json/plain',metadata
                    assert 'temporal-v/value-context' not in metadata,'application converter touched a UI label'
                    if codec=='context':
                        scope=json.loads(metadata['temporal-v/codec-context']);assert scope['kind']=='workflow' and scope['workflow_id']==(owner or handle)['workflow_id'],scope
                    return json.loads(data)
                def summary(event):
                    payload=event.get('userMetadata',{}).get('summary')
                    if not payload:return ''
                    metadata,data=decode_fixture_payload(payload,codec)
                    return json.loads(data)
                timers={summary(event):event for event in history['events'] if 'timerStartedEventAttributes' in event}
                expected={'direct':['label:outer','label:inner'],'nested':['label:outer','label:inner'],
                          'caught-error':['label:outer','label:inner','label:error'],'nested-restored':['label:outer','label:inner'],
                          'outer-restored':['label:outer'],'outside':[],
                          'update-timer':['update:client-group-immediate','label:update'],
                          'after-local-update':['update:client-group-deferred','label:after-local']}
                signal=next(event for event in history['events'] if event.get('workflowExecutionSignaledEventAttributes',{}).get('signalName')=='group')
                expected['signal-timer']=['event:'+str(signal['eventId'])]
                expected['signal-explicit']=['event:'+str(signal['eventId']),'label:signal']
                for name,markers in expected.items():assert groups(timers[name])==markers,(name,groups(timers.get(name,{})),markers)
                direct=next(marker for marker in timers['direct']['eventGroupMarkers'] if marker.get('label',{}).get('id')=='outer')
                assert label_text(direct['label']['label'])=='LABEL_SENTINEL direct-override'
                for event in history['events']:
                    for marker in event.get('eventGroupMarkers',[]):
                        if 'label' in marker:assert label_text(marker['label']['label']).startswith('LABEL_SENTINEL ')
                # Core inherits scheduled annotations on cancellation, rather than
                # taking the finish signal's implicit group.
                by_timer={event['timerStartedEventAttributes']['timerId']:event for event in timers.values()}
                for event in history['events']:
                    if 'timerCanceledEventAttributes' in event:assert groups(event)==groups(by_timer[event['timerCanceledEventAttributes']['timerId']]),event
                completed=next(event for event in history['events'] if 'workflowExecutionCompletedEventAttributes' in event)
                finish=next(event for event in history['events'] if event.get('workflowExecutionSignaledEventAttributes',{}).get('signalName')=='finish')
                assert groups(completed)==['event:'+str(finish['eventId'])],completed
                scheduled=next(event for event in history['events'] if 'activityTaskScheduledEventAttributes' in event)
                assert groups(scheduled)==['label:remote'],scheduled
                first=scheduled['activityTaskScheduledEventAttributes']['input']['payloads'][0]
                reference=base64.b64decode(first.get('metadata',{}).get('messageType',''))
                assert (reference==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,first.get('metadata')
                assert any(groups(event)==['label:child'] for event in history['events'] if 'startChildWorkflowExecutionInitiatedEventAttributes' in event)
                assert any(groups(event)==['label:nexus'] for event in history['events'] if 'nexusOperationScheduledEventAttributes' in event)
                assert any(groups(event)==['label:memo'] for event in history['events'] if 'workflowPropertiesModifiedEventAttributes' in event)
                assert any(groups(event)==['label:search'] for event in history['events'] if 'upsertWorkflowSearchAttributesEventAttributes' in event)
                local_markers=[event for event in history['events'] if event.get('markerRecordedEventAttributes',{}).get('markerName')=='core_local_activity']
                assert any(groups(event)==['label:local'] for event in local_markers),local_markers
                all_timers=[event for event in history['events'] if 'timerStartedEventAttributes' in event]
                assert any(groups(event)==['label:local'] for event in all_timers),'local backoff lost its groups'
                assert any(groups(event)==['update:client-group-deferred','label:deferred-local'] for event in all_timers),'deferred backoff lost its groups'
                patch_markers=[event for event in history['events'] if 'markerRecordedEventAttributes' in event]
                assert any(groups(event)==['label:patch'] for event in patch_markers),'patch marker lost its groups'
                assert any(groups(event)==['label:retired'] for event in patch_markers),'deprecated patch lost its groups'
                assert any(groups(event)==['update:client-group-deferred','label:deferred-local'] for event in local_markers),local_markers
                handles=[handle,{'workflow_id':handle['workflow_id']+'-child'}]
                child_history=fixture.history(handles[-1]);assert all(not groups(event) for event in child_history['events'])
                receiver=fixture.start('receiver','v.event-group.receiver',{})
                external=fixture.start('external','v.event-groups',{'mode':'external','target':receiver['workflow_id']})
                assert fixture.call('result',external)['data']=='"external-ok"'
                assert fixture.call('result',receiver)['status']=='canceled'
                external_history=fixture.history(external)
                assert any(groups(event)==['label:external-signal'] for event in external_history['events'] if 'signalExternalWorkflowExecutionInitiatedEventAttributes' in event)
                assert any(groups(event)==['label:external-cancel'] for event in external_history['events'] if 'requestCancelExternalWorkflowExecutionInitiatedEventAttributes' in event)
                handles += [receiver,external]
                for mode,status,expected in [('fail','failed',['label:failure']),('cancel','canceled',['label:cancel-scope','label:cancel-direct'])]:
                    current=fixture.start(mode,'v.event-groups',{'mode':mode})
                    result=fixture.call('result',current);assert result['status']==status,result
                    terminal=fixture.history(current)['events'][-1];assert groups(terminal)==expected,terminal
                    handles.append(current)
                def finish_chain(current):
                    result=fixture.call('result',current);assert result['status']=='continued_as_new' and result['next_run_id'],result
                    following=dict(current,run_id=result['next_run_id'])
                    assert fixture.call('result',following)['data']=='"chain-ok"'
                    assert groups(fixture.history(current)['events'][-1])==['label:continue']
                    assert groups(fixture.history(following)['events'][-1])==['label:chain-complete']
                    return [current,following]
                handles += finish_chain(fixture.start('chain','v.event-groups',{'mode':'chain'}))
                panic=fixture.start('panic','v.event-groups',{'mode':'panic'})
                fixture.wait_for(lambda:counts(fixture.history(panic))['workflowtaskfailed']>0)
                panic_history=fixture.history(panic)
                assert 'Event Group scope panic' in json.dumps(panic_history)
                assert not counts(panic_history)['workflowexecutionfailed']
                worker.terminate();assert worker.wait(timeout=20)==0
                def replay_all(entries,enabled=True):
                    for index,current in enumerate(entries):
                        history=fixture.history(current)
                        for event in history['events']:
                            for marker in event.get('eventGroupMarkers',[]):
                                if 'label' in marker:assert label_text(marker['label']['label'],current).startswith('LABEL_SENTINEL ')
                        file=directory/(current['workflow_id']+'-'+str(index)+'.bin');fixture.call('history',current,file=file)
                        replay=subprocess.run([str(binary),'replay',str(file),current['workflow_id'],fixture.queue],cwd=ROOT,env=dict(fixture.env,V_GROUP_REPLAY='true',V_GROUP_PANIC_FIXED='true',V_EVENT_GROUPS=str(enabled).lower()),capture_output=True,text=True,timeout=45)
                        assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
                        report['histories']+=1
                replay_all(handles)
                fixture.env['V_EVENT_GROUPS']='false'
                disabled=fixture.launch(binary,'disabled',('worker',));plain=fixture.start('disabled','v.event-groups',{'mode':'disabled'})
                assert fixture.call('result',plain)['data']=='"disabled-ok"'
                assert all(not event.get('eventGroupMarkers') for event in fixture.history(plain)['events'])
                configuration=fixture.start('configuration','v.event-groups',{'mode':'chain'})
                fixture.wait_for(lambda:counts(fixture.history(configuration))['workflowtaskfailed']>0)
                broken=fixture.history(configuration)
                assert not counts(broken)['workflowexecutionfailed'] and all(not groups(event) for event in broken['events'])
                disabled.kill();assert disabled.wait(timeout=10)==-9
                fixture.env.update(V_EVENT_GROUPS='true',V_GROUP_PANIC_FIXED='true')
                repaired=fixture.launch(binary,'repaired',('worker',))
                assert fixture.call('result',panic,wait_ms=20000)['data']=='"panic-repaired"'
                assert groups(fixture.history(panic)['events'][-1])==['label:panic']
                repaired_chain=finish_chain(configuration)
                repaired.terminate();assert repaired.wait(timeout=20)==0
                replay_all([panic]+repaired_chain)
                replay_all([plain],enabled=False)
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: nested/direct/duplicate/error restoration, implicit signal/Update groups, durable local retry and captured continuation, activities/child/Nexus/memo/patch metadata, codec-only UI labels, inherited cancellation, continue-as-new/external/failure/cancellation, panic and missing-flag task repair, disabled guard, SIGKILL/replacement and poison-validator offline replay passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

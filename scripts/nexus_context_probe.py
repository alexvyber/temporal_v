"""Nexus handlers reuse their worker client for durable start and cancellation."""
RESULT_REPORT='nexus-context-results.json'
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS, ROOT, CLI, TemporalFixture, counts


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0}
    try:
        profiles=[(version,server,codec,stored,shared) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')]
                  for codec in ['', 'prefix', 'context'] for stored in [False,True] for shared in [False,True]]
        if quick:profiles=profiles[:2]
        for version,server,codec,stored,shared in profiles:
            with TemporalFixture('nexus-context-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+str(shared)+'-'+binary.name,
                                 server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir();calls=directory/'calls';calls.mkdir();hooks=directory/'hooks';hooks.write_text('')
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_NEXUS_CONTEXT_SHARED=str(shared).lower(),V_NEXUS_CLIENT_HOOKS=str(hooks),V_NEXUS_CONTEXT_CALLS=str(calls))
                if stored:fixture.env['V_SDK_EXAMPLE_STORAGE_DIR']=str(directory/'storage')
                endpoint=fixture.queue.replace('_','-')+'-endpoint'
                created=subprocess.run([str(CLI),'operator','nexus','endpoint','create','--address',fixture.address,'--name',endpoint,'--target-namespace','default','--target-task-queue',fixture.queue],capture_output=True,text=True,timeout=20)
                assert created.returncode==0,created.stderr
                worker=fixture.workflows('before');nexus=fixture.launch(binary,'before',())
                name='checkpoint 世界 '+('x'*300000)
                handles=[fixture.start('success','v.nexus',{'endpoint':endpoint,'operation':'context-client','name':name,'delay_ms':5000}),
                         fixture.start('cancel','v.nexus',{'endpoint':endpoint,'operation':'context-client','name':'cancel','delay_ms':15000})]
                targets=[]
                def target(handle):
                    started=next(event['nexusOperationStartedEventAttributes'] for event in fixture.history(handle)['events'] if 'nexusOperationStartedEventAttributes' in event)
                    return {'workflow_id':'nexus-'+started['requestId']}
                for handle in handles:
                    fixture.wait_for(lambda:counts(fixture.history(handle))['nexusoperationstarted']==1)
                    targets.append(target(handle))
                assert len(list(calls.glob('start-*')))==2
                assert len(hooks.read_text().splitlines())==int(shared)
                scheduled=next(event['nexusOperationScheduledEventAttributes'] for event in fixture.history(handles[0])['events'] if 'nexusOperationScheduledEventAttributes' in event)
                payload=scheduled['input'];message_type=base64.b64decode(payload.get('metadata',{}).get('messageType',''))
                assert (message_type==b'temporal.api.sdk.v1.ExternalStorageReference')==stored,payload.get('metadata')
                worker.kill();nexus.kill();assert worker.wait(timeout=10)==-9 and nexus.wait(timeout=10)==-9
                worker=fixture.workflows('after');nexus=fixture.launch(binary,'after',())
                fixture.call('signal',handles[1],name='cancel')
                assert fixture.call('result',handles[1])['data']=='"Nexus cancelled"'
                fixture.wait_for(lambda:(calls/'cancel').exists())
                assert fixture.call('result',targets[1])['status']=='canceled'
                success=fixture.call('result',handles[0]);assert success['status']=='completed' and json.loads(success['data'])=='Hello from async Nexus, '+name+'!'
                healthy=fixture.start('healthy','v.nexus',{'endpoint':endpoint,'operation':'context-client','name':'healthy','delay_ms':20})
                assert json.loads(fixture.call('result',healthy)['data'])=='Hello from async Nexus, healthy!'
                handles.append(healthy);targets.append(target(healthy))
                assert len(hooks.read_text().splitlines())==2*int(shared),'Nexus context client reran plugin hooks'
                for process in [worker,nexus]:process.terminate();assert process.wait(timeout=20)==0
                before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in calls.iterdir()}
                for index,handle in enumerate(handles+targets):
                    assert counts(fixture.history(handle))['workflowtaskfailed']==0
                    file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
                    replay=subprocess.run([str(ARTIFACTS/'tools/v-replay'),'--history',str(file),'--workflow-id',handle['workflow_id'],'--task-queue',fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0,replay.stdout+replay.stderr
                    report['histories']+=1
                assert {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in calls.iterdir()}==before,'offline replay invoked Nexus clients'
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}/shared={shared}: start/cancel handler clients reused the worker connection, isolated closed handles, inherited plugin/configuration once per worker, retained codec/storage scope and large results; SIGKILL/replacement, target cancellation, new progress, clean drain and six offline histories passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-nexus-worker');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

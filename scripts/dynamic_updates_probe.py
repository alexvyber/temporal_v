"""Named-priority and raw dynamic Updates, including unknown payload encodings."""
RESULT_REPORT = 'dynamic-updates-results.json'
import argparse
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, TemporalFixture, counts


def main(binary, report_name, quick=False):
    report = {'passed': False, 'cases': [], 'histories': 0}
    try:
        profiles = [('', False)] if quick else [(codec, storage) for codec in ['', 'prefix', 'context'] for storage in [False, True]]
        for version, server in [('1-32', 'temporal'), ('1-33', 'temporal-protocol')]:
            for codec, storage in profiles:
                with TemporalFixture('dynamic-update-'+version+'-'+(codec or 'default')+'-'+str(storage)+'-'+binary.name, server_binary=ARTIFACTS/'tools'/server) as fixture:
                    files=fixture.histories/fixture.queue; files.mkdir()
                    fixture.env['V_SDK_EXAMPLE_CODEC']=codec
                    if storage: fixture.env.update(V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage'), V_PANIC_STORAGE_DRIVER='true')
                    worker=fixture.launch(binary,'before',('worker',))
                    handles=[fixture.start('named','v.dynamic-updates',{}),fixture.start('only','v.dynamic-only',{})]
                    def query(handle): return json.loads(fixture.call('query',handle,name='state')['data'])
                    def update(handle,name,identifier):
                        outcome=fixture.call('update',handle,name=name,update_id=identifier,input='7')
                        if outcome['status']=='accepted': outcome=fixture.call('update-result',handle,update_id=identifier)
                        return outcome
                    def opaque(handle):
                        result=subprocess.run([str(binary),'dynamic-opaque',json.dumps(handle)],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=30)
                        assert result.returncode==0,result.stdout+result.stderr
                        value=json.loads(result.stdout)
                        assert value['encoding']=='application/x-temporal-v-dynamic' and value['bytes']=='ff0080',value
                    for index,handle in enumerate(handles):
                        fixture.wait_for(lambda: query(handle)==[])
                        saved=fixture.env.pop('V_SDK_EXAMPLE_CODEC')
                        try: metadata=json.loads(fixture.call('query',handle,name='__temporal_workflow_metadata')['data'])
                        finally: fixture.env['V_SDK_EXAMPLE_CODEC']=saved
                        definitions=metadata['definition']['updateDefinitions']
                        assert {entry['name'] for entry in definitions}==({'','fixed'} if index==0 else {''}),metadata
                        fixed=update(handle,'fixed','fixed')
                        assert fixed['status']=='completed'
                        assert fixed['data']=='"named"' if index==0 else json.loads(fixed['data'])['handler']=='dynamic'
                        ordinary=update(handle,'unregistered-name','ordinary')
                        assert ordinary['status']=='completed' and json.loads(ordinary['data'])['name']=='unregistered-name',ordinary
                        opaque(handle)
                        if index == 0:
                            rejected=subprocess.run([str(binary),'dynamic-opaque-fixed',json.dumps(handle)],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=30)
                            assert rejected.returncode==0 and rejected.stdout.strip()=='NAMED_RAW_REJECTED',rejected.stdout+rejected.stderr
                        for name,message in [('reject','dynamic validation rejected'),('validator-panic','dynamic validator panic'),('handler-error','dynamic handler error')]:
                            failure=update(handle,name,name)
                            assert failure['status']=='failed' and failure['message']==message,failure
                        held=fixture.call('update',handle,name='deferred',update_id='held',input='7')
                        assert held['status']=='accepted',held
                        assert query(handle)==['fixed','unregistered-name','opaque','handler-error','deferred']
                    worker.kill(); worker.wait(timeout=10)
                    fixture.env['V_DYNAMIC_VALIDATOR_REPLAY_POISON']='true'
                    worker=fixture.launch(binary,'after',('worker',))
                    for handle in handles:
                        fixture.wait_for(lambda: len(query(handle))==5)
                        duplicate=update(handle,'unregistered-name','ordinary')
                        assert duplicate['status']=='completed' and len(query(handle))==5
                        opaque(handle); assert len(query(handle))==5
                        fixture.call('signal',handle,name='release')
                        released=fixture.call('update-result',handle,update_id='held')
                        assert released['status']=='completed' and released['data']=='"released"',released
                        fixture.call('signal',handle,name='finish')
                        assert fixture.call('result',handle)['status']=='completed'
                        assert counts(fixture.history(handle))['workflowtaskfailed']==0
                    for index,handle in enumerate(handles):
                        file=files/(str(index)+'.bin'); fixture.call('history',handle,file=file)
                        replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                        assert replay.returncode==0,replay.stdout+replay.stderr
                        report['histories']+=1
                    worker.terminate(); assert worker.wait(timeout=20)==0
                    report['cases'].append(f'{version}/{codec or "default"}/storage={storage}: dynamic-only and named-priority dispatch, owned raw opaque bytes, metadata, validator rejection/panic, handler failure, deferred completion, dedupe after SIGKILL and two offline histories passed')
        report['passed']=True
    finally: (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic'); parser.add_argument('--report',default=RESULT_REPORT); parser.add_argument('--quick',action='store_true')
    args=parser.parse_args(); main(args.binary,args.report,args.quick)

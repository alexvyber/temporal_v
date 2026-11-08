"""Recorded side effects: changed-only mutable markers, retries and callback-free replay."""
RESULT_REPORT='side-effect-results.json'
import argparse,base64,collections,hashlib,json,os,signal,subprocess,tempfile
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
from typed_details_wire import unwrap,unpack_payloads
BINARY=ARTIFACTS/'tools/v-side-effects'

def invoke(f,role,*args):
    result=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,(role,result.stderr)
    return 'OK' if result.stdout.strip()=='OK' else json.loads(result.stdout)

def verify_history(f,handle,state,profile,failing,mixed=False):
    history=f.history(handle)
    markers=[event['markerRecordedEventAttributes'] for event in history['events'] if 'markerRecordedEventAttributes' in event]
    names=collections.Counter(marker['markerName'] for marker in markers)
    expected={'SideEffect':1} if failing else {'SideEffect':4 if mixed else 3,'MutableSideEffect':2}
    if mixed and not failing:
        expected.update({'core_patch':1,'core_local_activity':1})
        assert [marker['markerName'] for marker in markers]==['SideEffect','MutableSideEffect','core_patch','core_local_activity','SideEffect','SideEffect','MutableSideEffect','SideEffect']
    assert names==expected,names
    markers=[marker for marker in markers if marker['markerName'] in ('SideEffect','MutableSideEffect')]
    expected_tokens=[state] if failing else state['tokens']
    observed=[];hints=[]
    for marker in markers:
        details=marker['details']
        assert 'side-effect-id' in details and 'data' in details,details
        identity=unpack_payloads(details['side-effect-id']['payloads']);assert len(identity)==1
        metadata,encoded=unwrap(identity[0],profile,handle,'workflow')
        if marker['markerName']=='SideEffect':
            sequence=json.loads(encoded);assert sequence==len(observed)+1
            values=unpack_payloads(details['data']['payloads']);assert len(values)==1
            metadata,data=unwrap(values[0],profile,handle,'workflow')
            assert metadata['encoding']==b'binary/plain' and len(data)==32
            observed.append(data.hex())
        else:
            assert json.loads(encoded).startswith('configuration_')
            counter=unpack_payloads(details['mutable-side-effect-call-counter']['payloads']);assert len(counter)==1
            _,encoded=unwrap(counter[0],profile,handle,'workflow');hints.append(json.loads(encoded))
    assert observed==expected_tokens,(observed,expected_tokens)
    if not failing:assert hints==[1,4],hints
    failures=[event['workflowTaskFailedEventAttributes'] for event in history['events'] if 'workflowTaskFailedEventAttributes' in event]
    if failing:
        assert len(failures)==1,failures
        assert 'fixture refused side effect first attempt' in json.dumps(failures),failures
    else:assert not failures,failures
    return history

def main(binary=BINARY,report_name=RESULT_REPORT,codecs=('', 'prefix', 'context','pack','split-pack','pack-split-pack'),modes=(False,True)):
    global BINARY
    BINARY=binary;report={'passed':False,'histories':0,'markers':0,'cases':[],'storage_fixtures':[]}
    try:
        # A captured old-Core history commits the LA marker in WFT 1 and the
        # following SideEffect in WFT 2. It failed deterministically before the fix.
        fixture=ROOT/'_tests/data/side-effect-local-activity-boundary'
        captured=json.loads((fixture/'manifest.json').read_text())
        for name,key in [('history.bin','history_sha256'),('history.json','json_sha256')]:
            assert hashlib.sha256((fixture/name).read_bytes()).hexdigest()==captured[key]
        events=json.loads((fixture/'history.json').read_text())['events']
        local=next(event['markerRecordedEventAttributes'] for event in events if event.get('markerRecordedEventAttributes',{}).get('markerName')=='core_local_activity')
        effects=[event['markerRecordedEventAttributes'] for event in events if event.get('markerRecordedEventAttributes',{}).get('markerName')=='SideEffect']
        assert local['workflowTaskCompletedEventId']!=effects[1]['workflowTaskCompletedEventId']
        with tempfile.TemporaryDirectory(prefix='side-effect-boundary-',dir=ARTIFACTS) as scratch:
            environment=dict(os.environ,V_SDK_EXAMPLE_CODEC='prefix',V_SDK_EXAMPLE_STORAGE_DIR='',V_EFFECT_COUNTS='',V_EFFECT_FORBID='1',V_EFFECT_JOURNAL=str(Path(scratch)/'forbidden-callbacks'),V_EFFECT_CONFIG=str(Path(scratch)/'must-not-be-read'))
            regression_images={}
            for replay_binary in [BINARY,ARTIFACTS/'tools/v-upgrade-baseline-side-effects']:
                regression_images[str(replay_binary)]=hashlib.sha256(replay_binary.read_bytes()).hexdigest()
                replay=subprocess.run([str(replay_binary),'replay',str(fixture/'history.bin'),captured['workflow_id'],captured['task_queue']],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
                (ARTIFACTS/'logs'/('side-effect-boundary-'+replay_binary.name+'-'+report_name+'.log')).write_text(replay.stdout+replay.stderr)
                assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
                assert not list(Path(scratch).iterdir()),'Replay invoked a nondurable callback'
        report['captured_boundary_regression']={'passed':True,'history_sha256':captured['history_sha256'],'producer':captured['producer'],'replays':2,'images':regression_images}
        report['histories']+=1
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for codec in codecs:
                profile=codec or 'default'
                for storage in (False,True):
                    for mixed in modes:
                        with TemporalFixture(f'side-effect-{version}-{profile}-{storage}-mixed-{mixed}',server_binary=server) as f:
                            files=ARTIFACTS/'side-effect-fixtures'/f.queue;files.mkdir(parents=True)
                            config=files/'config.txt';config.write_text('one');journal=files/'executions.log'
                            f.env.update(V_SDK_EXAMPLE_CODEC=codec if codec in ('prefix','context') else '',V_EFFECT_COUNTS=codec if codec in ('pack','split-pack','pack-split-pack') else '',V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage') if storage else '',V_EFFECT_CONFIG=str(config),V_EFFECT_JOURNAL=str(journal),V_EFFECT_FORBID='0')
                            worker=f.launch(BINARY,'before',('worker',))
                            handles=invoke(f,'start-mixed' if mixed else 'start')
                            for handle in handles[:2]:f.wait_for(lambda:invoke(f,'state',json.dumps(handle))['phase']==1)
                            failed_result=invoke(f,'result',json.dumps(handles[2]));assert failed_result['status']=='completed'
                            failed_value=json.loads(failed_result['data']);assert len(failed_value)==64
                            config.write_text('two')
                            for handle in handles[:2]:assert invoke(f,'change',json.dumps(handle))=='OK'
                            snapshots=[]
                            for handle in handles[:2]:
                                f.wait_for(lambda:invoke(f,'state',json.dumps(handle))['phase']==2)
                                state=invoke(f,'state',json.dumps(handle));assert state['configuration']=='two' and len(state['tokens'])==(4 if mixed else 3),state
                                snapshots.append(state)
                            expected=collections.Counter({'random':9 if mixed else 7,'config':10,'equal':8,'failure-op':2})
                            if mixed:expected['local']=2
                            assert collections.Counter(journal.read_text().splitlines())==expected,journal.read_text()
                            before=journal.read_bytes();f.env['V_EFFECT_FORBID']='1'
                            worker.kill();worker.wait(timeout=15)
                            worker=f.launch(BINARY,'after',('worker',))
                            for handle,state in zip(handles[:2],snapshots):
                                assert invoke(f,'state',json.dumps(handle))==state
                                assert invoke(f,'finish',json.dumps(handle))=='OK'
                                result=invoke(f,'result',json.dumps(handle));assert result['status']=='completed' and json.loads(result['data'])==state,result
                            assert journal.read_bytes()==before
                            assert not Path(str(journal)+'.equal-replay').exists()
                            histories=[]
                            for index,handle in enumerate(handles):
                                history=verify_history(f,handle,failed_value if index==2 else snapshots[index],profile,index==2,mixed)
                                report['markers']+=sum('markerRecordedEventAttributes' in event for event in history['events'])
                                path=f.histories/(handle['run_id']+'.bin');invoke(f,'history',json.dumps(handle),path)
                                histories.append((path,handle['workflow_id'],f.queue,dict(f.env)))
                            worker.send_signal(signal.SIGTERM);worker.wait(timeout=30);assert worker.returncode==0
                        stored={str(p.relative_to(files/'storage')):hashlib.sha256(p.read_bytes()).hexdigest() for p in (files/'storage').glob('*') if p.is_file() and p.name!='retrievals.log'} if storage else {}
                        for path,identity,queue,environment in histories:
                            replay=subprocess.run([str(BINARY),'replay',str(path),identity,queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
                            assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stderr
                            report['histories']+=1
                        assert journal.read_bytes()==before and not Path(str(journal)+'.equal-replay').exists()
                        if storage:
                            after={str(p.relative_to(files/'storage')):hashlib.sha256(p.read_bytes()).hexdigest() for p in (files/'storage').glob('*') if p.is_file() and p.name!='retrievals.log'}
                            assert stored==after,'replay uploaded or changed marker payloads'
                            report['storage_fixtures'].append({'directory':str(files/'storage'),'files':after})
                        report['cases'].append(f'{version}/{profile}/storage={storage}/mixed={mixed}: binary recorded values and mutable first/unchanged/changed values; independent marker IDs/data/call hints; swallowed callback error still failed its task; SIGKILL/replacement and offline replay never invoked computation/equality or uploaded payloads')
                        print(report['cases'][-1],flush=True)
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    parser.add_argument('--codecs',nargs='+',default=['','prefix','context','pack','split-pack','pack-split-pack'])
    parser.add_argument('--mode',choices=['both','plain','mixed'],default='both')
    args=parser.parse_args();main(args.binary,args.report,args.codecs,(False,True) if args.mode=='both' else (args.mode=='mixed',))

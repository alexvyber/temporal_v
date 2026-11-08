"""Parallel offline corpus replay, isolated failures, ordered outcomes and fail-fast drain."""
RESULT_REPORT='replay-batch-results.json'
import argparse,hashlib,json,signal,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
BINARY=ARTIFACTS/'tools/v-replay-batch'

def invoke(f,role,*args):
    result=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=90)
    (ARTIFACTS/f'logs/{f.name}-{role}-{args[-1] if role=="batch" else "live"}.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,(role,result.stderr)
    return 'OK' if result.stdout.strip()=='OK' else json.loads(result.stdout)

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary;report={'passed':False,'live_histories':0,'successful_replays':0,'failed_replays':0,'skipped':0,'cases':[],'storage_fixtures':[]}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for codec in ('','prefix','context'):
                for storage in (False,True):
                    with TemporalFixture(f'replay-batch-{version}-{codec or "default"}-{storage}',server_binary=server) as f:
                        directory=ARTIFACTS/'replay-batch-storage'/f.queue
                        if storage:directory.mkdir(parents=True)
                        f.env.update(V_SDK_EXAMPLE_CODEC=codec,V_SDK_EXAMPLE_STORAGE_DIR=str(directory) if storage else '')
                        worker=f.launch(BINARY,'before',('worker',))
                        handles=invoke(f,'start')
                        for handle in handles:assert invoke(f,'check',json.dumps(handle))=='OK'
                        for handle in handles[:3]:assert invoke(f,'finish',json.dumps(handle))=='OK'
                        worker.kill();worker.wait(timeout=15)
                        worker=f.launch(BINARY,'after',('worker',))
                        for handle in handles[3:]:assert invoke(f,'finish',json.dumps(handle))=='OK'
                        entries=[]
                        for handle in handles:
                            f.assert_clean_history(handle)
                            path=f.histories/(handle['run_id']+'.bin');invoke(f,'history',json.dumps(handle),path)
                            entries.append({'path':str(path),'workflow_id':handle['workflow_id'],'queue':f.queue})
                            report['live_histories']+=1
                        manifest=f.histories/'manifest.json';manifest.write_text(json.dumps(entries))
                        worker.send_signal(signal.SIGTERM);worker.wait(timeout=30);assert worker.returncode==0
                    # The fixture is stopped: every following operation must be offline.
                    before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('*') if p.is_file() and p.name!='retrievals.log'} if storage else {}
                    assert invoke(f,'batch',manifest,'validation')=='OK'
                    for mode in ('sequential','parallel','mixed','fail-fast-serial','fail-fast-parallel','mismatch'):
                        result=invoke(f,'batch',manifest,mode)
                        assert result['active']==0,result
                        outcomes=result['outcomes']
                        expected_ids=[entry['workflow_id'] for entry in entries]
                        if mode=='mixed':expected_ids.insert(1,'invalid-history')
                        if mode.startswith('fail-fast'):expected_ids.insert(0,'invalid-history')
                        assert [item['workflow_id'] for item in outcomes]==expected_ids,outcomes
                        assert [item['index'] for item in outcomes]==list(range(len(outcomes))),outcomes
                        kinds=[item['kind'] for item in outcomes]
                        if mode in ('sequential','parallel'):assert kinds==['succeeded']*6,kinds
                        elif mode=='mixed':assert kinds==['succeeded','failed']+['succeeded']*5,kinds
                        elif mode=='fail-fast-serial':assert kinds==['failed']+['skipped']*6,kinds
                        elif mode=='fail-fast-parallel':assert kinds==['failed','succeeded']+['skipped']*5,kinds
                        else:assert kinds==['failed']*6,kinds
                        for item in outcomes:
                            if item['kind']=='succeeded':assert item['result']['run_id'] and item['result']['activations']>0,item
                            if item['kind']=='failed':assert item['message'],item
                        report['successful_replays']+=kinds.count('succeeded');report['failed_replays']+=kinds.count('failed');report['skipped']+=kinds.count('skipped')
                    if storage:
                        after={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.glob('*') if p.is_file() and p.name!='retrievals.log'}
                        assert before==after,'offline batch replay uploaded or modified payloads'
                        report['storage_fixtures'].append({'directory':str(directory),'files':after})
                    report['cases'].append(f'{version}/{codec or "default"}/storage={storage}: live replacement and six wire histories; offline serial/parallel replay, callback concurrency bound, ordered mixed failures, fail-fast drain/skip, incompatible code rejection and unchanged external storage')
                    print(report['cases'][-1],flush=True)
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

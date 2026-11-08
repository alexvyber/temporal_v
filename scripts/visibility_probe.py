"""Visibility pagination and per-execution memo conversion against Temporal."""
RESULT_REPORT='visibility-results.json'
import argparse,json,subprocess,tempfile,shutil,hashlib
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
BINARY=ARTIFACTS/'tools/v-visibility'

def invoke(f,role,*args):
    result=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,result.stderr
    return 'OK' if result.stdout.strip()=='OK' else json.loads(result.stdout)

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary;report={'passed':False,'cases':[],'histories':0,'storage_fixtures':[]}
    try:
        with TemporalFixture('visibility-pagination-validation') as f:
            result=subprocess.run([str(BINARY),'validation'],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=45)
            assert result.returncode==0 and result.stdout.strip()=='VISIBILITY_VALIDATION_OK',result.stderr
        for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for codec in ('','prefix','context'):
                for storage in (False,True):
                    with tempfile.TemporaryDirectory(prefix='visibility-',dir=ARTIFACTS) as directory:
                        with TemporalFixture(f'visibility-{label}-{codec or "default"}-{storage}',server_binary=server) as f:
                            f.env.update(V_SDK_EXAMPLE_CODEC=codec,V_SDK_EXAMPLE_STORAGE_DIR=directory if storage else '')
                            worker=f.launch(BINARY,'before',('worker',))
                            handles=invoke(f,'start')
                            identities={item['workflow_id'] for item in handles}
                            f.wait_for(lambda:invoke(f,'count')==6)
                            for handle in handles:assert invoke(f,'check',json.dumps(handle))=='OK'
                            for handle in handles[:3]:assert invoke(f,'finish',json.dumps(handle))=='OK'
                            f.wait_for(lambda:invoke(f,'count',"'Completed'")==3 and invoke(f,'count',"'Running'")==3)
                            assert set(invoke(f,'list'))==identities
                            retrievals=Path(directory)/'retrievals.log'
                            before=retrievals.read_text() if storage else ''
                            assert set(invoke(f,'raw-list'))==identities
                            if storage:assert retrievals.read_text()==before,'raw listing retrieved memo payloads'
                            worker.kill();assert worker.wait(timeout=10)==-9
                            worker=f.launch(BINARY,'after',('worker',))
                            assert set(invoke(f,'list'))==identities
                            for handle in handles[3:]:assert invoke(f,'finish',json.dumps(handle))=='OK'
                            f.wait_for(lambda:invoke(f,'count',"'Completed'")==6)
                            paths=[]
                            for handle in handles:
                                f.assert_clean_history(handle)
                                path=ARTIFACTS/f'binary-histories/{f.queue}-{handle["run_id"]}.bin'
                                invoke(f,'history',json.dumps(handle),path);paths.append((path,handle))
                            worker.terminate();assert worker.wait(timeout=20)==0
                            environment=f.env.copy();queue=f.queue
                        stores=Path(directory)/'stores.log';before=stores.read_text() if storage else ''
                        for path,handle in paths:
                            replay=subprocess.run([str(BINARY),'replay',str(path),handle['workflow_id'],queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
                            assert replay.returncode==0,replay.stderr
                            report['histories']+=1
                        if storage:
                            assert stores.read_text()==before,'replay uploaded visibility payloads'
                            saved=ARTIFACTS/'visibility-storage'/queue;shutil.copytree(directory,saved)
                            report['storage_fixtures'].append({'directory':str(saved),'codec':codec,'files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in saved.iterdir() if p.is_file()}})
                    print(label,codec or 'default',storage,'VISIBILITY_LIVE_OK',flush=True)
        assert report['histories']==72
        report['cases'].append('Current/archived/scan page routing, namespace/options, lazy iteration, empty intermediate pages, token cycles, retryable errors, unknown fields and EOF validated with injected responses')
        report['cases'].append('Both servers and three codec policies: six running/completed executions paginated with typed JSON/binary/protobuf/null memos; per-workflow external claims with _type keys enforced retrieval scope, raw listing avoided retrieval; counts, SIGKILL/replacement and 72 offline histories passed without replay uploads')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:value for key,value in report.items() if key!='storage_fixtures'},indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

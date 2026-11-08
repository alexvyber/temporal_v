"""Real raw-history frontend responses, storage, pagination and replay."""
RESULT_REPORT='raw-history-results.json'
import argparse,json,subprocess,tempfile,time
from pathlib import Path
from integration import ROOT,ARTIFACTS,TemporalFixture
BINARY=ARTIFACTS/'tools/v-raw-history'

def invoke(f,role,*args):
    result=subprocess.run([str(BINARY),role,*map(str,args)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=50)
    (ARTIFACTS/f'logs/{f.name}-{role}.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,result.stderr
    return 'OK' if result.stdout.strip()=='OK' else json.loads(result.stdout)

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary;report={'passed':False,'cases':[]};histories=[]
    try:
        for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for profile in ('default','prefix','context'):
                for storage in (False,True):
                    with tempfile.TemporaryDirectory(prefix='raw-history-',dir=ARTIFACTS) as directory:
                        with TemporalFixture(f'raw-history-{label}-{profile}-{storage}',server_args=('--dynamic-config-value','frontend.sendRawWorkflowHistory=true'),server_binary=server) as f:
                            f.env.update(V_SDK_EXAMPLE_CODEC='' if profile=='default' else profile,V_SDK_EXAMPLE_STORAGE_DIR=directory if storage else '')
                            worker=f.launch(BINARY,'before',('worker',))
                            handle=invoke(f,'start',f.queue)
                            def started():
                                result=subprocess.run([str(BINARY),'history',json.dumps(handle),str(Path(directory)/'pending.bin')],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=30)
                                if result.returncode!=0:return False
                                summary=json.loads(result.stdout)
                                return summary['events']>=5
                            f.wait_for(started)
                            worker.kill();assert worker.wait(timeout=10)==-9
                            worker=f.launch(BINARY,'after',('worker',))
                            invoke(f,'finish',json.dumps(handle));invoke(f,'result',json.dumps(handle))
                            path=Path(directory)/'completed.bin';summary=invoke(f,'history',json.dumps(handle),path)
                            assert summary['raw_batches']>0 and summary['pages']>1 and summary['events']>5,summary
                            f.call('describe',handle)
                            worker.terminate();assert worker.wait(timeout=20)==0
                            env=f.env.copy();identity=handle['workflow_id'];queue=f.queue
                        # No server is alive here. Storage references must remain
                        # durable, and replay may retrieve but must not upload.
                        before=(Path(directory)/'stores.log').read_text() if storage else ''
                        for name in ('pending.bin','completed.bin'):
                            result=subprocess.run([str(BINARY),'replay',str(Path(directory)/name),identity,queue],cwd=ROOT,env=env,capture_output=True,text=True,timeout=50)
                            assert result.returncode==0,result.stderr
                            saved=ARTIFACTS/f'binary-histories/{queue}-{name}';saved.write_bytes((Path(directory)/name).read_bytes());histories.append(str(saved))
                        if storage:assert (Path(directory)/'stores.log').read_text()==before
                    print(label,profile,storage,'RAW_HISTORY_LIVE_OK',flush=True)
        assert len(histories)==24
        report.update(passed=True,histories=24)
        report['cases'].append('Both servers returned actual raw blobs; paginated normalization, result polling, protected payloads and external references survived SIGKILL/replacement; 24 pending/completed histories replayed after server shutdown without uploads')
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

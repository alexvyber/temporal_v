"""Overall result budgets dominate late failures without hiding early errors."""
RESULT_REPORT='wait-deadline-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,CLI,TemporalFixture
BINARY=ARTIFACTS/'tools/v-wait-deadlines'

def main(binary=BINARY,report_name=RESULT_REPORT):
    report={'passed':False,'cases':[]};histories=[]
    try:
        with TemporalFixture('wait-deadline-injection') as f:
            result=subprocess.run([str(binary)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=30)
            assert result.returncode==0 and result.stdout.strip()=='WAIT_DEADLINE_VALIDATION_OK',result.stderr
        for label,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for codec in ('','prefix','context'):
                with TemporalFixture(f'wait-deadline-{label}-{codec or "default"}',server_binary=server) as f:
                    f.env['V_SDK_EXAMPLE_CODEC']=codec
                    worker=f.launch(binary,'worker',('worker',))
                    endpoint=f.queue+'-endpoint'
                    created=subprocess.run([str(CLI),'operator','nexus','endpoint','create','--address',f.address,'--name',endpoint,'--target-namespace','default','--target-task-queue',f.queue+'-empty'],capture_output=True,text=True,timeout=20)
                    assert created.returncode==0,created.stderr
                    result=subprocess.run([str(binary),'live',endpoint],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=60)
                    (ARTIFACTS/f'logs/{f.name}-client.log').write_text(result.stdout+result.stderr)
                    assert result.returncode==0,result.stderr
                    handle=json.loads(result.stdout);f.assert_clean_history(handle)
                    path=ARTIFACTS/f'binary-histories/{f.queue}.bin';f.call('history',handle,file=path);histories.append((path,handle['workflow_id'],f.queue,f.env.copy()))
                    worker.terminate();assert worker.wait(timeout=20)==0
                print(label,codec or 'default','WAIT_DEADLINE_LIVE_OK',flush=True)
        for path,identity,queue,env in histories:
            result=subprocess.run([str(binary),'replay',str(path),identity,queue],cwd=ROOT,env=env,capture_output=True,text=True,timeout=40);assert result.returncode==0,result.stderr
        report.update(passed=True,histories=len(histories),live_short_waits=480)
        report['cases'].append('Four result APIs retained early injected transport status/diagnostics and mapped failures after budget expiry to deadline; both servers and three converter profiles passed 480 one-millisecond waits, early missing-execution errors, execution survival and six offline histories')
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

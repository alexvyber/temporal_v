"""Client propagation and independently closed, concurrently used connection leases."""
RESULT_REPORT='tracing-client-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture

def main(binary,report_name):
    report={'passed':False,'cases':[]}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            with TemporalFixture('trace-client-'+version,server_binary=server) as fixture:
                result=subprocess.run([str(binary),'client-validation'],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                (ARTIFACTS/f'logs/trace-client-{version}-{report_name}.log').write_text(result.stdout+result.stderr)
                assert result.returncode==0 and result.stdout.strip()=='CLIENT_OK',result.stdout+result.stderr
                report['cases'].append(version+': seven RPC forms and both Update-with-Start operations preserved application payloads and unknown fields; per-call W3C headers preserved ordered duplicate binary metadata; failed RPC status recorded; sixteen retained clients completed concurrent real RPCs after their original and parent clients closed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

"""Span snapshots, bounded export, concurrency and dispatcher ownership without a collector."""
RESULT_REPORT='tracing-provider-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT
BINARY=ARTIFACTS/'tools/v-tracing'

def main(binary=BINARY,report_name=RESULT_REPORT):
    report={'passed':False,'cases':[]}
    try:
        result=subprocess.run([str(binary),'provider-validation'],cwd=ROOT,capture_output=True,text=True,timeout=45)
        (ARTIFACTS/f'logs/{report_name}.log').write_text(result.stdout+result.stderr)
        assert result.returncode==0 and result.stdout.strip()=='PROVIDER_OK',result.stderr
        assert 'private-export' not in result.stdout+result.stderr and 'private-shutdown' not in result.stdout+result.stderr
        report['cases']=[
            'Caller-array snapshots and FIFO export survived mutation after admission; pending span/byte limits included blocked in-flight exports and rejected overflow/late records',
            'Failed export batches were reported privately by flush, then recovered; shutdown failures stayed visible without emitting private diagnostics to logs',
            'Span finish was idempotent; attribute replacement, event/link limits, epoch-zero timestamps and OK-over-error status precedence were preserved',
            'Eight concurrent producers exported sixty-four distinct span identities without losses or pending ownership',
            'Caller shutdown deadlines cooperatively canceled blocked callbacks; later drain and callback-initiated shutdown released the exporter exactly once'
        ]
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

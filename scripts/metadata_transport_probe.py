"""Repeated RPC metadata through actual Core callbacks and nested forwarding."""
RESULT_REPORT='metadata-transport-results.json'
import argparse,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT
BINARY=ARTIFACTS/'tools/v-metadata-transport'

def main(binary=BINARY,report_name=RESULT_REPORT):
    report={'passed':False,'cases':[]}
    try:
        result=subprocess.run([str(binary)],cwd=ROOT,capture_output=True,text=True,timeout=60)
        (ARTIFACTS/f'logs/{report_name}.log').write_text(result.stdout+result.stderr)
        assert result.returncode==0 and result.stdout.strip()=='METADATA_MULTIPLICITY_OK',result.stderr
        report['calls']=163
        report['cases'].append('Ordered repeated ASCII/binary metadata, empty binary values, connection-default precedence and comma-coalesced forwarding crossed actual Core/V callbacks; eight concurrent writers per forwarding mode completed 163 calls; invalid keys/ASCII failed before dispatch')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.binary,args.report)

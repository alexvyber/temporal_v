"""Validate W3C propagation against frozen Go OpenTelemetry outputs and strict edges."""
RESULT_REPORT='trace-context-results.json'
import argparse,hashlib,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT
BINARY=ARTIFACTS/'tools/v-tracing'

def main(binary=BINARY,report_name=RESULT_REPORT):
    report={'passed':False,'cases':[]}
    try:
        corpus=json.loads((ROOT/'_tests/data/trace-context.json').read_text())
        assert corpus['reference']['version']=='v1.43.0'
        for name,expected in corpus['reference']['sources'].items():
            assert hashlib.sha256((ROOT/'_tests/native/tracing_go'/name).read_bytes()).hexdigest()==expected,name
        validated=subprocess.run([str(binary),'validation'],cwd=ROOT,capture_output=True,text=True,timeout=45)
        (ARTIFACTS/f'logs/{report_name}-validation.log').write_text(validated.stdout+validated.stderr)
        assert validated.returncode==0 and validated.stdout.strip()=='OK',validated.stderr
        inputs=ARTIFACTS/(report_name+'-inputs.json');inputs.write_text(json.dumps([{'parent':value['parent'],'state':value['state']} for value in corpus['vectors']]))
        actual=subprocess.run([str(binary),'contexts',str(inputs)],cwd=ROOT,capture_output=True,text=True,timeout=90)
        assert actual.returncode==0,actual.stderr
        outputs=actual.stdout.splitlines();assert len(outputs)==len(corpus['vectors'])
        def semantic(value):
            if value=='0':return None
            assert value.startswith('1|'),value
            return json.loads(value[2:])
        differences=[{'input':vector,'v':output} for vector,output in zip(corpus['vectors'],outputs) if semantic(vector['output'])!=semantic(output)]
        report['comparison']={'vectors':len(outputs),'differences':differences[:20],'difference_count':len(differences),'corpus_sha256':hashlib.sha256((ROOT/'_tests/data/trace-context.json').read_bytes()).hexdigest(),'reference':corpus['reference']}
        assert not differences,differences[:2]
        report['cases'].append(f'{len(outputs)} pinned Go OpenTelemetry propagation cases matched V: version/flag/ID boundaries, future versions, malformed UTF-8/control/hex fields, tracestate normalization/limits and invalid-state isolation')
        report['cases'].append('Strict version-zero trailing fields, duplicate mixed-case parents and wrong Temporal carrier encodings were rejected; default JSON carrier round trips and random root/child identity, sampling and remote flags passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

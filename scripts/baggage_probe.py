"""Independent Go OpenTelemetry baggage corpus, partial parsing and immutable snapshots."""
RESULT_REPORT='baggage-results.json'
import argparse,hashlib,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT

def main(binary,report_name):
    report={'passed':False,'cases':[]}
    try:
        corpus=json.loads((ROOT/'_tests/data/baggage.json').read_text())
        assert corpus['reference']['module']=='go.opentelemetry.io/otel' and corpus['reference']['version']=='v1.43.0'
        for name,expected in corpus['reference']['sources'].items():assert hashlib.sha256((ROOT/'_tests/native/baggage_go'/name).read_bytes()).hexdigest()==expected,name
        validation=subprocess.run([str(binary),'baggage-validation'],cwd=ROOT,capture_output=True,text=True,timeout=30)
        assert validation.returncode==0 and validation.stdout.strip()=='BAGGAGE_OK',validation.stderr+validation.stdout
        inputs=ARTIFACTS/(report_name+'-inputs.json');inputs.write_text(json.dumps([value['input'] for value in corpus['vectors']]))
        result=subprocess.run([str(binary),'baggage-contexts',str(inputs)],cwd=ROOT,capture_output=True,text=True,timeout=120);assert result.returncode==0,result.stderr
        outputs=result.stdout.splitlines();assert len(outputs)==len(corpus['vectors'])
        differences=[{'input':vector['input'],'go':vector['output'],'v':json.loads(output)} for vector,output in zip(corpus['vectors'],outputs) if vector['output']!=json.loads(output)]
        report['comparison']={'vectors':len(outputs),'difference_count':len(differences),'differences':differences[:20],'corpus_sha256':hashlib.sha256((ROOT/'_tests/data/baggage.json').read_bytes()).hexdigest(),'reference':corpus['reference']}
        assert not differences,differences[:2]
        report['cases']=['Pinned Go OpenTelemetry baggage parsing matched percent escaping, UTF-8 repair, token/whitespace/property rules, duplicate resolution, partial invalid-member isolation and member/byte limits','Immutable snapshots preserved properties across caller mutation and independent updates/deletes; invalid constructors, oversized baggage and case-insensitive extraction passed']
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

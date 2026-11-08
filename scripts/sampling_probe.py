"""Pinned Go sampling decisions plus bounded span lifecycle and custom sampler ownership."""
RESULT_REPORT='sampling-results.json'
import argparse,hashlib,json,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT

def main(binary,report_name):
    report={'passed':False,'cases':[]}
    try:
        corpus=json.loads((ROOT/'_tests/data/sampling.json').read_text());assert corpus['reference']['version']=='v1.43.0'
        for name,expected in corpus['reference']['sources'].items():assert hashlib.sha256((ROOT/'_tests/native/sampling_go'/name).read_bytes()).hexdigest()==expected,name
        validation=subprocess.run([str(binary),'sampling-validation'],cwd=ROOT,capture_output=True,text=True,timeout=45)
        (ARTIFACTS/f'logs/{report_name}-validation.log').write_text(validation.stdout+validation.stderr)
        assert validation.returncode==0 and validation.stdout.strip()=='SAMPLING_OK',validation.stdout+validation.stderr
        inputs=ARTIFACTS/(report_name+'-inputs.json');inputs.write_text(json.dumps([value['input'] for value in corpus['vectors']]))
        result=subprocess.run([str(binary),'sampling-contexts',str(inputs)],cwd=ROOT,capture_output=True,text=True,timeout=120);assert result.returncode==0,result.stderr
        outputs=result.stdout.splitlines();assert len(outputs)==len(corpus['vectors'])
        differences=[{'input':vector['input'],'go':vector['output'],'v':json.loads(output)} for vector,output in zip(corpus['vectors'],outputs) if vector['output']!=json.loads(output)]
        report['comparison']={'vectors':len(outputs),'difference_count':len(differences),'differences':differences[:20],'corpus_sha256':hashlib.sha256((ROOT/'_tests/data/sampling.json').read_bytes()).hexdigest(),'reference':corpus['reference']}
        assert not differences,differences[:2]
        report['cases']=['Pinned Go ratio/parent-based/always-on/off decisions matched exact 63-bit boundaries, fractions, local/remote parent flags and tracestate','Custom sampler input/result arrays were immutable snapshots; sampler attributes overrode caller attributes; recording-only/drop/explicit overrides had distinct recording/export behavior; invalid ratios/states failed and span duration included the sampling callback']
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

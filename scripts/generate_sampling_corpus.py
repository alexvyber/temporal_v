"""Pinned Go OpenTelemetry sampling decisions at exact ratio boundaries and parent policies."""
import hashlib,json,random,subprocess,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def main():
    cases=[];seen=set()
    def add(method,fraction,identity,parent=False,remote=False,flags=0,state=''):
        value={'method':method,'fraction':str(fraction),'trace_id':identity,'parent':parent,'remote':remote,'flags':flags,'state':state};key=json.dumps(value,sort_keys=True)
        if key not in seen:seen.add(key);cases.append(value)
    fractions=['0','0.000001','0.01','0.1','0.25','0.5','0.75','0.9','0.999999','1']
    for fraction in fractions:
        bound=int(float(fraction)*(1<<63))
        prefixes={0,1,(1<<64)-1,(1<<63)-1,1<<63}
        for delta in (-2,-1,0,1,2):
            for low in (0,1):prefixes.add(max(0,min((1<<64)-1,((bound+delta)<<1)|low)))
        for prefix in sorted(prefixes):
            identity='1234567890abcdef'+f'{prefix:016x}'
            for method in ('ratio','parent','on','off'):
                add(method,fraction,identity)
                for remote in (False,True):
                    for flags in (0,1,2,3,254,255):add(method,fraction,identity,True,remote,flags,'vendor=value')
    rng=random.Random(20261107)
    for _ in range(12000):
        add(rng.choice(['ratio','parent','on','off']),rng.choice(fractions),f'{rng.getrandbits(128):032x}',bool(rng.getrandbits(1)),bool(rng.getrandbits(1)),rng.randrange(256),'vendor=value')
    oracle=ROOT/'_tests/native/sampling_go'
    with tempfile.TemporaryDirectory(prefix='v-sampling-reference-') as folder:
        inputs=Path(folder)/'cases.json';inputs.write_text(json.dumps(cases))
        result=subprocess.run(['go','run','.',str(inputs)],cwd=oracle,capture_output=True,text=True,timeout=180);assert result.returncode==0,result.stderr
    outputs=result.stdout.splitlines();assert len(outputs)==len(cases)
    corpus={'reference':{'module':'go.opentelemetry.io/otel/sdk','version':'v1.43.0','go_toolchain':subprocess.check_output(['go','version'],text=True).strip(),'sources':{name:hashlib.sha256((oracle/name).read_bytes()).hexdigest() for name in ('main.go','go.mod','go.sum')}},'vectors':[{'input':value,'output':json.loads(output)} for value,output in zip(cases,outputs)]}
    (ROOT/'_tests/data/sampling.json').write_text(json.dumps(corpus,indent=2)+'\n');print('Recorded',len(cases),'Go sampling decisions')
if __name__=='__main__':main()

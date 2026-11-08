"""Regenerate bounded W3C propagation expectations with pinned Go OpenTelemetry."""
import hashlib,json,random,subprocess,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def main():
    randomizer=random.Random(20261106)
    trace='4bf92f3577b34da6a3ce929d0e0e4736';span='00f067aa0ba902b7'
    base=f'00-{trace}-{span}-01'
    cases=[];seen=set()
    def add(parent,state=''):
        key=(parent,state)
        if key not in seen:seen.add(key);cases.append({'parent':parent,'state':state})
    for version in range(256):
        for flags in (0,1,2,3,4,15,128,254,255):add(f'{version:02x}-{trace}-{span}-{flags:02x}')
    for value in ('','0','000','FF','aG','\x00',' 00','００'):
        add(f'{value}-{trace}-{span}-01')
    for value in ('', '0'*32,trace.upper(),trace+'0',trace[:-1], 'z'+trace[1:]):add(f'00-{value}-{span}-01')
    for value in ('', '0'*16,span.upper(),span+'0',span[:-1], 'z'+span[1:]):add(f'00-{trace}-{value}-01')
    for index in range(len(base)):
        for value in ('A','g',' ',':','\x00'):
            add(base[:index]+value+base[index+1:])
        add(base[:index]+base[index+1:])
    for version in ('00','01','fe'):
        for suffix in ('-future','--future','-future-more','-\x00'):
            add(f'{version}-{trace}-{span}-01'+suffix)
    states=['','vendor=value','vendor= leading value ','vendor=value,other=second',',vendor=value,,',' ','vendor=','vendor =value','vendor=bad=value','vendor=tab\tvalue','vendor=é','vendor=x,vendor=y','1tenant@vendor=value','tenant@1vendor=value']
    for size in (0,1,13,14,15,240,241,242,255,256,257):
        states.extend(['a'*size+'=value','a'+'b'*size+'@vendor=value','tenant@'+'a'*size+'=value','vendor='+'x'*size])
    for size in (31,32,33):states.append(','.join('v'+str(index)+'=x' for index in range(size)))
    for state in states:
        for parent in (base,base[:-2]+'00',f'01-{trace}-{span}-ff','invalid'):add(parent,state)
    for index in range(12000):
        trace_id=f'{randomizer.getrandbits(128):032x}';span_id=f'{randomizer.getrandbits(64):016x}'
        version=randomizer.choice(['00','00','01','10','fe'])
        flags=randomizer.randrange(4) if version=='00' else randomizer.randrange(256)
        parent=f'{version}-{trace_id}-{span_id}-{flags:02x}'
        if index%3==0:
            offset=randomizer.randrange(len(parent));parent=parent[:offset]+randomizer.choice('GZ: /\x00')+parent[offset+1:]
        elif index%7==0:parent=parent[:-randomizer.randrange(1,12)]
        elif version!='00' and index%5==0:parent+='-future-'+str(index)
        add(parent,randomizer.choice(states))
    oracle=ROOT/'_tests/native/tracing_go'
    with tempfile.TemporaryDirectory(prefix='temporal-v-trace-reference-') as folder:
        path=Path(folder)/'cases.json';path.write_text(json.dumps(cases,ensure_ascii=False))
        result=subprocess.run(['go','run','.',str(path)],cwd=oracle,capture_output=True,text=True,timeout=180)
        assert result.returncode==0,result.stderr
    outputs=result.stdout.splitlines();assert len(outputs)==len(cases)
    corpus={'reference':{'module':'go.opentelemetry.io/otel','version':'v1.43.0','go_toolchain':subprocess.check_output(['go','version'],text=True).strip(),'specification':'https://www.w3.org/TR/2024/CRD-trace-context-2-20240328/','sources':{name:hashlib.sha256((oracle/name).read_bytes()).hexdigest() for name in ('go.mod','go.sum','main.go')}},'vectors':[{**value,'output':output} for value,output in zip(cases,outputs)]}
    (ROOT/'_tests/data/trace-context.json').write_text(json.dumps(corpus,indent=2,ensure_ascii=False)+'\n')
    print('Recorded',len(cases),'Go OpenTelemetry propagation cases')
if __name__=='__main__':main()

"""Freeze Go OpenTelemetry baggage parsing/encoding behavior, including partial results."""
import hashlib,json,random,subprocess,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def main():
    cases=[];seen=set()
    def add(value):
        if value not in seen:seen.add(value);cases.append(value)
    for value in ['',',','key=value','key=',' key = value ','key=a=b','key=+','key=%2b','key=%00','key=%FF','key=%C0%AF','key=%ED%A0%80','key=%F4%90%80%80','key=a;prop','key=a;prop=','key=a;;','key=a; ;x','key=a;prop=%FF','key=a;prop=%','good=value,bad,other=valid','key=one,key=two','é=x','key=世界','key=%E4%B8%96%E7%95%8C','key=x\n','\u2003key = value\u2003','key=x;prop=x\n']:
        add(value)
    for byte in range(256):
        value=chr(byte)
        for template in ['{}=value','key={}value','key=val{}ue','key=value;{}prop','key=value;prop={}value','good=value,bad{}key=invalid,other=valid']:
            add(template.format(value))
        add(f'key=%{byte:02x}');add(f'key=value;prop=%{byte:02x}')
    for length in (0,1,62,63,64,65,66):
        base=','.join(f'k{index}=x' for index in range(length));add(base);add(base+',k0=again');add(base+',bad,more=x')
    for length in (4094,4095,4096,4097,8180,8188,8189,8190,8191,8192,8193,8194):
        add('k='+'x'*length);add('good=one,k='+'x'*length+',after=x');add('k='+'x'*length+',k=small')
    rng=random.Random(20261107)
    alphabet='abcXYZ019!#$%&\'*+-.^_`|~=;,+ /\\\t\r\n世界%00%ff'
    for index in range(10000):
        value=''.join(rng.choice(alphabet) for _ in range(rng.randrange(0,80)))
        if index%3==0:value='good=first,'+value+',other=last'
        if index%5==0:value='key=value;'+value
        add(value)
    oracle=ROOT/'_tests/native/baggage_go'
    with tempfile.TemporaryDirectory(prefix='v-baggage-oracle-') as folder:
        inputs=Path(folder)/'cases.json';inputs.write_text(json.dumps(cases,ensure_ascii=False))
        result=subprocess.run(['go','run','.',str(inputs)],cwd=oracle,capture_output=True,text=True,timeout=180);assert result.returncode==0,result.stderr
    outputs=result.stdout.splitlines();assert len(outputs)==len(cases)
    corpus={'reference':{'module':'go.opentelemetry.io/otel','version':'v1.43.0','go_toolchain':subprocess.check_output(['go','version'],text=True).strip(),'sources':{name:hashlib.sha256((oracle/name).read_bytes()).hexdigest() for name in ('go.mod','go.sum','main.go')}},'vectors':[{'input':value,'output':json.loads(output)} for value,output in zip(cases,outputs)]}
    (ROOT/'_tests/data/baggage.json').write_text(json.dumps(corpus,indent=2,ensure_ascii=False)+'\n')
    print('Recorded',len(cases),'Go OpenTelemetry baggage cases')
if __name__=='__main__':main()

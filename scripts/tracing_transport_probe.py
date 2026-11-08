"""Exercise the native OTLP span export boundary with independent wire inspection."""
RESULT_REPORT='tracing-transport-results.json'
import argparse,copy,json,subprocess,struct,tempfile,threading,time,os
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from integration import ARTIFACTS,ROOT
from otlp_receiver import OtlpGrpcReceiver,fields
BINARY=ARTIFACTS/'tools/v-tracing'

def fieldmap(data):
    result={}
    for number,wire,value in fields(data):result.setdefault(number,[]).append(value)
    return result

def any_value(data):
    value=fieldmap(data)
    if 1 in value:return value[1][0].decode()
    if 2 in value:return bool(value[2][0])
    if 3 in value:
        integer=value[3][0];return integer if integer<2**63 else integer-2**64
    if 4 in value:return struct.unpack('<d',value[4][0])[0]
    if 5 in value:return [any_value(item) for item in fieldmap(value[5][0]).get(1,[])]
    raise AssertionError(value)

def attributes(values):
    return {fieldmap(value)[1][0].decode():any_value(fieldmap(value)[2][0]) for value in values}

def inspect_binary(data):
    resources=fieldmap(data)[1];assert len(resources)==1
    resource=fieldmap(resources[0]);resource_attributes=attributes(fieldmap(resource[1][0]).get(1,[]))
    scopes=resource[2];assert len(scopes)==1
    scope=fieldmap(scopes[0]);identity=fieldmap(scope[1][0]);span=fieldmap(scope[2][0])
    return {'resource':resource_attributes,'scope_name':identity[1][0].decode(),'scope_version':identity.get(2,[b''])[0].decode(),'span':span,'attributes':attributes(span.get(9,[]))}

class HttpReceiver:
    def __init__(self, protocol, hold=False, tls_context=None):
        self.protocol=protocol;self.received=[];self.hold=hold;self.started=threading.Event();self.release=threading.Event()
        owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                size=int(self.headers.get('Content-Length','0'));data=self.rfile.read(size)
                owner.received.append({'path':self.path,'headers':dict(self.headers),'data':data});owner.started.set()
                if owner.hold:owner.release.wait(timeout=5)
                response=b'{}' if owner.protocol=='http/json' else b''
                try:
                    self.send_response(200);self.send_header('Content-Type','application/json' if owner.protocol=='http/json' else 'application/x-protobuf');self.send_header('Content-Length',str(len(response)));self.end_headers();self.wfile.write(response)
                except (BrokenPipeError,ConnectionResetError):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);
        if tls_context is not None:self.server.socket=tls_context.wrap_socket(self.server.socket,server_side=True)
        self.server.daemon_threads=True;self.port=self.server.server_address[1]
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
    def __enter__(self):self.thread.start();return self
    def __exit__(self,*_):self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(timeout=3)

def payload():
    attribute=lambda kind,value:{'kind':kind,'value':value}
    return {'scope_name':'temporal-v','scope_version':'0.0.1','context':{'trace_id':'1234567890abcdef1234567890abcdef','span_id':'1234567890abcdef','trace_flags':3,'trace_state':'vendor=value','remote':False},'parent_span_id':'abcdef1234567890','parent_remote':True,'name':'V span 世界','kind':2,'start_unix_nanos':'1794000000000000001','end_unix_nanos':'1794000000001234567','attributes':{'string':attribute('string','世界'),'boolean':attribute('boolean',True),'maximum':attribute('integer',str(2**63-1)),'minimum':attribute('integer',str(-2**63)),'double':attribute('double','1.25'),'strings':attribute('string_array',['a','世界']),'booleans':attribute('boolean_array',[True,False]),'integers':attribute('integer_array',[str(-2**63),str(2**63-1)]),'doubles':attribute('double_array',['-1.5','2.25'])},'events':[{'name':'event','unix_nanos':'1794000000000000002','attributes':{'exception.type':attribute('string','FixtureError')},'dropped_attributes':2}],'links':[{'context':{'trace_id':'abcdef1234567890abcdef1234567890','span_id':'abcdef1234567890','trace_flags':1,'trace_state':'other=value','remote':True},'attributes':{'linked':attribute('boolean',True)},'dropped_attributes':1}],'dropped_attributes':3,'dropped_events':4,'dropped_links':5,'status':2,'status_message':'fixture failure'}

def config(protocol,port):
    return {'endpoint':f'http://127.0.0.1:{port}'+('' if protocol=='grpc' else '/v1/traces'),'protocol':protocol,'timeout_millis':1000,'service_name':'v-sdk-tracing-fixture','headers':{'test-export-tag':'wire-proof'},'resource':{'environment':{'kind':'string','value':'fixture'}}}

def invoke(binary,directory,options,spans,budget=1000,role='native-export'):
    options_path=directory/'options.json';options_path.write_text(json.dumps(options));spans_path=directory/'spans.json';spans_path.write_text(json.dumps(spans))
    environment={key:value for key,value in os.environ.items() if not key.startswith('OTEL_')}
    environment.update(NO_PROXY='127.0.0.1,localhost',no_proxy='127.0.0.1,localhost')
    start=time.monotonic();result=subprocess.run([str(binary),role,str(options_path),str(spans_path),str(budget)],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=10)
    return result,time.monotonic()-start

def main(binary=BINARY,report_name=RESULT_REPORT,role='native-export'):
    report={'passed':False,'cases':[]}
    try:
        with tempfile.TemporaryDirectory(prefix='temporal-v-tracing-native-') as folder:
            directory=Path(folder);expected=payload()
            for protocol in ('grpc','http/protobuf','http/json'):
                receiver=OtlpGrpcReceiver(lambda body:[inspect_binary(body)['span'][5][0].decode()]) if protocol=='grpc' else HttpReceiver(protocol)
                with receiver:
                    result,elapsed=invoke(binary,directory,config(protocol,receiver.port),[expected],role=role);assert result.returncode==0 and result.stdout.strip()=='OK',(result.stderr,getattr(receiver,'errors',[]))
                    assert len(receiver.received)==1,receiver.received
                    if protocol=='http/json':
                        request=receiver.received[0];assert request['path']=='/v1/traces' and request['headers']['test-export-tag']=='wire-proof'
                        decoded=json.loads(request['data']);resource=decoded['resourceSpans'][0];scope=resource['scopeSpans'][0];span=scope['spans'][0]
                        assert span['traceId']==expected['context']['trace_id'] and span['spanId']==expected['context']['span_id']
                        assert span['startTimeUnixNano']==expected['start_unix_nanos'] and span['endTimeUnixNano']==expected['end_unix_nanos']
                        assert scope['scope']['name']=='temporal-v' and scope['scope']['version']=='0.0.1',scope['scope']
                        values={value['key']:value['value'] for value in span['attributes']};assert values['maximum']['intValue']==str(2**63-1) and values['minimum']['intValue']==str(-2**63)
                        assert values['integers']['arrayValue']['values']==[{'intValue':str(-2**63)},{'intValue':str(2**63-1)}]
                    else:
                        body=receiver.received[0][1] if protocol=='grpc' else receiver.received[0]['data'];decoded=inspect_binary(body);span=decoded['span'];values=decoded['attributes']
                        assert decoded['scope_name']=='temporal-v' and decoded['scope_version']=='0.0.1',decoded
                        assert decoded['resource']=={'service.name':'v-sdk-tracing-fixture','environment':'fixture'}
                        assert span[1][0].hex()==expected['context']['trace_id'] and span[2][0].hex()==expected['context']['span_id'] and span[4][0].hex()==expected['parent_span_id']
                        assert int.from_bytes(span[7][0],'little')==int(expected['start_unix_nanos']) and int.from_bytes(span[8][0],'little')==int(expected['end_unix_nanos'])
                        assert values=={'string':'世界','boolean':True,'maximum':2**63-1,'minimum':-2**63,'double':1.25,'strings':['a','世界'],'booleans':[True,False],'integers':[-2**63,2**63-1],'doubles':[-1.5,2.25]},values
                        assert (span[10][0],span[12][0],span[14][0])==(3,4,5)
                        event=fieldmap(span[11][0]);link=fieldmap(span[13][0]);status=fieldmap(span[15][0])
                        assert event[2][0]==b'event' and event[4][0]==2 and link[5][0]==1 and status[3][0]==2
                    report['cases'].append(protocol+': actual OTLP requests preserved IDs, timestamps, scope/resource and typed i64/array values')
            with HttpReceiver('http/protobuf',hold=True) as receiver:
                result,elapsed=invoke(binary,directory,config('http/protobuf',receiver.port),[expected],100)
                assert result.returncode!=0 and elapsed<2 and receiver.started.is_set(),(elapsed,result.stderr)
                report['cases'].append('A silent HTTP receiver exceeded the 100ms overall budget and returned promptly without blocking runtime shutdown')
            with HttpReceiver('http/protobuf') as receiver:
                options=config('http/protobuf',receiver.port)
                for mutation in ('invalid-id','invalid-kind','invalid-end','invalid-attribute','empty-batch'):
                    value=copy.deepcopy(expected)
                    if mutation=='invalid-id':value['context']['trace_id']='0'*32
                    if mutation=='invalid-kind':value['kind']=255
                    if mutation=='invalid-end':value['end_unix_nanos']='1'
                    if mutation=='invalid-attribute':value['attributes']['bad']={'kind':'integer','value':'9223372036854775808'}
                    result,_=invoke(binary,directory,options,[] if mutation=='empty-batch' else [value]);assert result.returncode!=0,(mutation,result.stdout)
                assert not receiver.received
                result,_=invoke(binary,directory,options,[expected],role='native-close');assert result.returncode==0 and 'closed' in result.stdout
                report['cases'].append('Malformed IDs/kinds/timestamps/integer ranges/empty batches were rejected before network I/O; idempotent close and export-after-close passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    parser.add_argument('--role',choices=['native-export','typed-export'],default='native-export')
    args=parser.parse_args();main(args.binary,args.report,args.role)

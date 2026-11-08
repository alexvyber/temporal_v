"""Authenticated OTLP transports with disposable CA, strict host verification and mTLS."""
RESULT_REPORT='tracing-tls-results.json'
import argparse,json,ssl,tempfile
from pathlib import Path
from integration import ARTIFACTS
from tls_probe import certificates
from otlp_receiver import OtlpGrpcReceiver
from tracing_transport_probe import HttpReceiver,config,invoke as native_invoke,payload,inspect_binary

def invoke(*args,**kwargs):
    result,elapsed=native_invoke(*args,**kwargs)
    assert 'BEGIN PRIVATE KEY' not in result.stdout+result.stderr and 'BEGIN CERTIFICATE' not in result.stdout+result.stderr
    return result,elapsed

def main(binary,report_name):
    report={'passed':False,'cases':[]}
    try:
        with tempfile.TemporaryDirectory(prefix='v-trace-tls-') as folder:
            directory=Path(folder);certificates(directory)
            ca=(directory/'ca.pem').read_text();cert=(directory/'client.pem').read_text();key=(directory/'client.key').read_text()
            for protocol in ('grpc','http/protobuf','http/json'):
                for mutual in (False,True):
                    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(directory/'server.pem',directory/'server.key')
                    context.set_alpn_protocols(['h2'] if protocol=='grpc' else ['http/1.1'])
                    if mutual:context.verify_mode=ssl.CERT_REQUIRED;context.load_verify_locations(cafile=str(directory/'ca.pem'))
                    receiver=OtlpGrpcReceiver(lambda body:[inspect_binary(body)['span'][5][0].decode()],tls_context=context) if protocol=='grpc' else HttpReceiver(protocol,tls_context=context)
                    with receiver:
                        options=config(protocol,receiver.port);options.update(endpoint=options['endpoint'].replace('http://127.0.0.1','https://localhost'),root_ca_cert=ca)
                        if mutual:options.update(client_cert=cert,client_key=key)
                        for role in ('native-export','typed-export'):
                            before=len(receiver.received);result,elapsed=invoke(binary,directory,options,[payload()],role=role)
                            assert result.returncode==0 and len(receiver.received)==before+1,(protocol,mutual,role,result.stderr)
                        before=len(receiver.received)
                        bad=dict(options,endpoint=options['endpoint'].replace('localhost','127.0.0.1'))
                        result,_=invoke(binary,directory,bad,[payload()]);assert result.returncode!=0,'hostname mismatch accepted'
                        bad=dict(options,root_ca_cert='');result,_=invoke(binary,directory,bad,[payload()]);assert result.returncode!=0,'untrusted private CA accepted'
                        if mutual:
                            bad=dict(options,client_cert='',client_key='');result,_=invoke(binary,directory,bad,[payload()]);assert result.returncode!=0,'mTLS accepted no identity'
                        assert len(receiver.received)==before,'rejected TLS client sent application payload'
                        report['cases'].append(f'{protocol}/mTLS={mutual}: native and typed V spans authenticated the private CA; hostname and trust failures were rejected before application payload; client identity required for mTLS')
                for invalid in [dict(root_ca_cert=ca),dict(client_cert=cert),dict(client_key=key)]:
                    options=config(protocol,1);options.update(invalid)
                    result,_=invoke(binary,directory,options,[payload()]);assert result.returncode!=0
            report['cases'].append('TLS material on plaintext endpoints and incomplete client identities were rejected; certificate/private-key contents stayed out of diagnostics')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

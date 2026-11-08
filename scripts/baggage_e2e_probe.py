"""Cross-SDK-boundary baggage scope, codec/storage separation and restart replay."""
RESULT_REPORT='baggage-e2e-results.json'
import argparse,json,subprocess,uuid
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
from tracing_workflow_probe import main as workflow_matrix
from tracing_nexus_probe import main as nexus_matrix

ROOT_BAGGAGE='tenant=public,unicode=%E4%B8%96%E7%95%8C;flag;meta=a%20b'
SIGNAL_BAGGAGE='scope=signal,tenant=signal'
UPDATE_BAGGAGE='scope=update,tenant=update'

def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0}
    try:
        profiles=('',) if quick else ('','prefix','context');storage_modes=(False,) if quick else (False,True)
        workflow_report=report_name.removesuffix('-results.json')+'-workflow-results.json'
        workflow_matrix(binary,workflow_report,profiles,storage_modes,baggage=ROOT_BAGGAGE)
        nexus_report=report_name.removesuffix('-results.json')+'-nexus-results.json'
        nexus_matrix(binary,nexus_report,profiles,storage_modes,baggage=ROOT_BAGGAGE)
        report['workflow']=json.loads((ARTIFACTS/workflow_report).read_text());report['nexus']=json.loads((ARTIFACTS/nexus_report).read_text())
        assert report['workflow']['passed'] and report['nexus']['passed']
        report['histories']=report['workflow']['histories']+report['nexus']['histories']
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            with TemporalFixture('baggage-scope-'+version,server_binary=server) as fixture:
                files=ARTIFACTS/'baggage-scope-fixtures'/fixture.queue;files.mkdir(parents=True);journal=files/'live.jsonl'
                fixture.env.update(V_TRACE_ID=uuid.uuid4().hex,V_TRACE_JOURNAL=str(journal),V_TRACE_BAGGAGE=ROOT_BAGGAGE,V_TRACE_BAGGAGE_SIGNAL=SIGNAL_BAGGAGE,V_TRACE_BAGGAGE_UPDATE=UPDATE_BAGGAGE,V_SDK_EXAMPLE_CODEC='context',V_SDK_EXAMPLE_STORAGE_DIR=str(files/'storage'))
                def invoke(role,*args,baggage=None):
                    env=fixture.env if baggage is None else dict(fixture.env,V_TRACE_BAGGAGE_CLIENT=baggage)
                    result=subprocess.run([str(binary),role,*map(lambda value:json.dumps(value) if isinstance(value,dict) else str(value),args)],cwd=ROOT,env=env,capture_output=True,text=True,timeout=45)
                    with (files/'calls.log').open('a') as output:output.write(role+'\n'+result.stdout+result.stderr)
                    assert result.returncode==0,(role,result.stdout,result.stderr)
                    return result.stdout.strip()
                worker=fixture.launch(binary,'before',('trace-worker',));handles=json.loads(invoke('trace-start'))
                handle=handles[0];fixture.wait_for(lambda:invoke('trace-state',handle)=='3')
                assert invoke('trace-baggage-signal',handle,baggage=SIGNAL_BAGGAGE)=='OK'
                fixture.wait_for(lambda:invoke('trace-state',handle)=='4')
                assert invoke('trace-update',handle,baggage=UPDATE_BAGGAGE)=='OK'
                fixture.wait_for(lambda:invoke('trace-state',handle)=='5')
                worker.kill();worker.wait(timeout=10);fixture.launch(binary,'after',('trace-worker',))
                fixture.wait_for(lambda:invoke('trace-state',handle)=='5')
                assert invoke('trace-finish',handle)=='OK'
                assert json.loads(invoke('trace-result',handle))['status']=='completed'
                history=files/'scope.bin';assert invoke('trace-history',handle,history)=='OK'
                assert invoke('trace-replay',history,handle['workflow_id'],fixture.queue)=='OK'
                report['histories']+=1
                # Baggage is propagation data, never an automatically exported attribute.
                spans=[json.loads(line) for line in journal.read_text().splitlines()]
                assert all('tenant' not in span['attributes'] and 'unicode' not in span['attributes'] for span in spans)
                report['cases'].append(version+': signal and Update baggage overrides reached local Activities and readonly query/validator interceptors; root baggage returned on later events and after SIGKILL/replay; application codecs/storage retained scope')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true')
    args=parser.parse_args();main(args.binary,args.report,args.quick)

"""Query-only activations retain live observability while history replay stays quiet."""
RESULT_REPORT='query-observation-results.json'
import argparse
import json
from pathlib import Path
import subprocess
import urllib.request
import uuid
from integration import ARTIFACTS,ROOT,TemporalFixture
from smoke import free_port


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0,'query_modes':[]}
    try:
        profiles=[(version,server,codec) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')] for codec in ['','prefix','context']]
        if quick:profiles=profiles[:1]
        for version,server,codec in profiles:
            with TemporalFixture('query-observation-'+version+'-'+(codec or 'default')+'-'+binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir();journal=directory/'traces.jsonl';journal.write_text('')
                port=free_port();fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_TRACE_ID=uuid.uuid4().hex,V_TRACE_JOURNAL=str(journal),V_QUERY_METRICS_ADDRESS='127.0.0.1:'+str(port))
                worker=fixture.launch(binary,'before',('query-observe-worker',))
                def metrics():
                    try:
                        with urllib.request.urlopen('http://127.0.0.1:'+str(port)+'/metrics',timeout=2) as response:text=response.read().decode()
                    except (OSError,TimeoutError):return None
                    result={'start':0,'query':0}
                    for line in text.splitlines():
                        if not line.startswith('v_query_observation_total'):continue
                        for phase in result:
                            if 'phase="'+phase+'"' in line:result[phase]+=float(line.rsplit(' ',1)[1])
                    return result
                def traces():return [json.loads(line) for line in journal.read_text().splitlines() if line]
                fixture.wait_for(lambda:metrics() is not None)
                handle=fixture.start('live','v.query-observation',{})
                fixture.wait_for(lambda:metrics()['start']==1)
                modes=[]
                def query():
                    value=json.loads(fixture.call('query',handle,name='state',rpc_ms=20000)['data']);assert value['data']=='"state"',value
                    modes.append(value);return value
                query();query()
                assert any(value['activation'] and not value['history'] for value in modes),modes
                expected=sum(not value['history'] for value in modes)
                fixture.wait_for(lambda:metrics()['query']==expected)
                fixture.wait_for(lambda:sum(span['name']=='QueryWorkflow:state' for span in traces())==expected)
                worker.kill();worker.wait(timeout=10);worker=fixture.launch(binary,'after',('query-observe-worker',))
                fixture.wait_for(lambda:metrics() is not None)
                after=[query(),query(),query()]
                assert any(value['activation'] and not value['history'] for value in after),after
                live_after=sum(not value['history'] for value in after)
                fixture.wait_for(lambda:metrics()['query']==live_after)
                assert metrics()['start']==0,'history replay exported the start metric'
                fixture.wait_for(lambda:sum(span['name']=='QueryWorkflow:state' for span in traces())==expected+live_after)
                fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['status']=='completed'
                file=directory/'history.bin';fixture.call('history',handle,file=file)
                worker.terminate();assert worker.wait(timeout=20)==0
                starts=sum('QUERY_OBSERVATION_START' in (ARTIFACTS/'logs'/('query-observation-'+version+'-'+(codec or 'default')+'-'+binary.name+'-'+label+'.log')).read_text() for label in ['before','after'])
                assert starts==1,'replay duplicated a workflow log'
                offline=directory/'offline.jsonl';offline.write_text('')
                replay=subprocess.run([str(binary),'query-observe-replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=dict(fixture.env,V_TRACE_JOURNAL=str(offline)),capture_output=True,text=True,timeout=45)
                assert replay.returncode==0 and replay.stdout.strip()=='QUERY_REPLAY_OK',replay.stdout+replay.stderr
                assert offline.stat().st_size==0 and 'QUERY_OBSERVATION_START' not in replay.stdout+replay.stderr
                report['query_modes'].append({'server':version,'codec':codec or 'default','queries':modes})
                report['histories']+=1
                report['cases'].append(f'{version}/{codec or "default"}: Core replay flag stayed true on cached query-only activations while history flag was false; query metrics/spans stayed live, historical start metric/log suppressed after SIGKILL, completion and offline replay exported no replay observations')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

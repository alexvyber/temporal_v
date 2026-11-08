"""Activity clients share connections/configuration with independent handle ownership."""
RESULT_REPORT='activity-client-results.json'
import argparse
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS,ROOT,TemporalFixture,counts


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0}
    try:
        profiles=[(version,server,codec,stored,shared)
                  for version,server in [('1-32','temporal'),('1-33','temporal-protocol')]
                  for codec in ['', 'prefix', 'context'] for stored in [False,True] for shared in [False,True]]
        if quick:profiles=[profiles[0],profiles[1]]
        for version,server,codec,stored,shared in profiles:
            with TemporalFixture('activity-client-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+str(shared)+'-'+binary.name,
                                 server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir();journal=directory/'traces.jsonl'
                fixture.env.update(V_SDK_EXAMPLE_CODEC=codec,V_ACTIVITY_CLIENT_SHARED=str(shared).lower(),V_TRACE_JOURNAL=str(journal))
                if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'))
                worker=fixture.launch(binary,'before',('activity-client-worker',))
                handles=[fixture.start('outer-'+str(index),'v.activity-client',{}) for index in range(2)]
                def state(handle):return json.loads(fixture.call('query',handle,name='state',rpc_ms=20000)['data'])
                for handle in handles:fixture.wait_for(lambda:state(handle)['completed']==2 and state(handle)['called']==2)
                expected=[state(handle) for handle in handles]
                children=[child for value in expected for child in value['children']]
                assert len(children)==4 and len({child['workflow_id'] for child in children})==4,children
                worker.terminate();assert worker.wait(timeout=30)==0
                log=ARTIFACTS/'logs'/('activity-client-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+str(shared)+'-'+binary.name+'-before.log')
                assert 'ACTIVITY_CLIENT_DRAIN_OK' in log.read_text(),log.read_text()
                spans=[json.loads(line) for line in journal.read_text().splitlines()]
                by_id={span['context']['span_id']:span for span in spans}
                starts=[span for span in spans if span['name']=='StartWorkflowExecution']
                assert len(starts)==4,[(span['name'],span.get('parent_span_id')) for span in spans]
                for span in starts:
                    assert by_id[span['parent_span_id']]['name']=='RunActivity:v.activity-client',span
                assert len([span for span in spans if span['name']=='RunActivity:v.activity-client'])==4
                replacement=fixture.launch(binary,'replacement',('worker',))
                for handle,value in zip(handles,expected):assert state(handle)==value
                replacement.kill();assert replacement.wait(timeout=10)==-9
                replacement=fixture.launch(binary,'after-kill',('worker',))
                for handle,value in zip(handles,expected):
                    assert state(handle)==value
                    fixture.call('signal',handle,name='finish')
                    assert fixture.call('result',handle)['data']=='"client-workflow-ok"'
                for index,handle in enumerate(handles+children):
                    assert counts(fixture.history(handle))['workflowtaskfailed']==0
                    file=directory/(str(index)+'.bin');fixture.call('history',handle,file=file)
                    replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                    assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
                    report['histories']+=1
                replacement.terminate();assert replacement.wait(timeout=20)==0
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}/shared={shared}: concurrent local/remote Activities started workflows, read scoped results and signaled callers; shared plugin/interceptor snapshot ran once, original client close and sibling close preserved worker/retained handles, retained clients worked after worker drain; trace ancestry, SIGKILL replacement and six offline histories passed')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

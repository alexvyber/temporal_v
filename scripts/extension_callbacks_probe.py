"""Custom RPC/telemetry callback panics preserve dispatcher ownership and drain."""
RESULT_REPORT='extension-callbacks-results.json'
import argparse
import json
from pathlib import Path
import subprocess
from integration import ARTIFACTS,ROOT,TemporalFixture


def main(grpc_binary,metric_binary,trace_binary,report_name):
    report={'passed':False,'cases':[],'metrics':[]}
    def invoke(binary,role,environment=None):
        result=subprocess.run([str(binary),role],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=90)
        (ARTIFACTS/'logs'/(report_name+'-'+binary.name+'-'+role+'.log')).write_text(result.stdout+result.stderr)
        assert result.returncode==0,result.stderr[-4000:]
        assert 'private-' not in result.stdout+result.stderr,'callback panic or private diagnostics escaped into output'
        return result.stdout.strip()
    try:
        assert invoke(trace_binary,'exporter-panic-validation')=='TRACE_PANIC_OK'
        report['cases'].append('Trace export panic failed/removed its batch; a subsequent export succeeded; shutdown panic returned an error while closing the provider; both callback defers and retained export/shutdown contexts drained')
        for version,server in [('1-32','temporal'),('1-33','temporal-protocol')]:
            with TemporalFixture('extension-panics-'+version+'-'+grpc_binary.name,server_binary=ARTIFACTS/'tools'/server) as fixture:
                assert invoke(grpc_binary,'panic-validation',fixture.env)=='GRPC_PANIC_OK'
                report['cases'].append(f'{version}: eight threads completed eighty panicking and eighty successful custom RPCs; five panicking connection handshakes failed with redacted errors; callback defers balanced and native accepted/responded/pending ownership drained')
                for role in ['panic-export','panic-once','panic-shutdown']:
                    stats=json.loads(invoke(metric_binary,role,fixture.env));assert stats['finished'] and stats['core_done'] and stats['pending']==0,stats
                    assert stats['export_failures']>0,stats
                    if role=='panic-export':assert stats['exported']==0 and stats['failed']>0,stats
                    else:assert stats['exported']>0,stats
                    assert stats['live_definitions']==0 and stats['live_labels']==0,stats
                    report['metrics'].append({'server':version,'mode':role,'stats':stats})
                report['cases'].append(f'{version}: persistent/one-shot metric export panics and shutdown panic preserved error counters, later export progress, user defers, closed contexts, released native points/definitions/labels and clean dispatcher completion')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--grpc-binary',type=Path,default=ARTIFACTS/'tools/v-grpc');parser.add_argument('--metric-binary',type=Path,default=ARTIFACTS/'tools/v-metric-export');parser.add_argument('--trace-binary',type=Path,default=ARTIFACTS/'tools/v-tracing');parser.add_argument('--report',default=RESULT_REPORT);args=parser.parse_args();main(args.grpc_binary,args.metric_binary,args.trace_binary,args.report)

"""Changed SideEffect code must reject replay and honor workflow failure policy."""
RESULT_REPORT='side-effect-errors-results.json'
import argparse,hashlib,json,signal,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT,TemporalFixture
import side_effect_probe as effects
BINARY=ARTIFACTS/'tools/v-side-effects'

def main(binary=BINARY,report_name=RESULT_REPORT):
    global BINARY
    BINARY=binary
    effects.BINARY=BINARY
    report={'passed':False,'histories':0,'rejected_replays':0,'cases':[]}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            for policy in ('task','other','all','selected'):
                with TemporalFixture('side-effect-errors-'+version+'-'+policy,server_binary=server) as f:
                    directory=ARTIFACTS/'side-effect-errors-fixtures'/f.queue;directory.mkdir(parents=True)
                    config=directory/'configuration';config.write_text('one');journal=directory/'executions'
                    codec={'task':'','other':'prefix','all':'prefix','selected':'context'}[policy]
                    f.env.update(V_EFFECT_CONFIG=str(config),V_EFFECT_JOURNAL=str(journal),V_EFFECT_FORBID='0',V_EFFECT_MUTATION='',V_EFFECT_POLICY=policy,V_SDK_EXAMPLE_CODEC=codec,V_SDK_EXAMPLE_STORAGE_DIR='')
                    worker=f.launch(BINARY,'before',('worker',));handle=effects.invoke(f,'start-one');encoded=json.dumps(handle)
                    f.wait_for(lambda:effects.invoke(f,'state',encoded)['phase']==1)
                    before=journal.read_bytes();worker.kill();assert worker.wait(timeout=15)==-9
                    f.env.update(V_EFFECT_FORBID='1',V_EFFECT_MUTATION='rename')
                    worker=f.launch(BINARY,'changed',('worker',))
                    effects.invoke(f,'wake',encoded)
                    if policy in ('all','selected'):
                        result=effects.invoke(f,'result',encoded)
                        assert result['status']=='failed' and 'nondetermin' in result['message'].lower(),result
                    else:
                        def rejected():
                            failures=[e['workflowTaskFailedEventAttributes'] for e in f.history(handle)['events'] if 'workflowTaskFailedEventAttributes' in e]
                            return any(e.get('cause')=='WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR' for e in failures)
                        f.wait_for(rejected)
                        assert not any('workflowExecutionFailedEventAttributes' in e for e in f.history(handle)['events'])
                        worker.kill();assert worker.wait(timeout=15)==-9
                        f.env['V_EFFECT_MUTATION']=''
                        worker=f.launch(BINARY,'repaired',('worker',))
                        effects.invoke(f,'cancel',encoded)
                        assert effects.invoke(f,'result',encoded)['status']=='canceled'
                    worker.send_signal(signal.SIGTERM);assert worker.wait(timeout=30)==0
                    history=f.histories/(handle['run_id']+'.bin');effects.invoke(f,'history',encoded,history)
                    environment=dict(f.env)
                    assert journal.read_bytes()==before and not Path(str(journal)+'.equal-replay').exists()
                for mutation in ('extra','remove','rename'):
                    environment['V_EFFECT_MUTATION']=mutation
                    replay=subprocess.run([str(BINARY),'replay',str(history),handle['workflow_id'],f.queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
                    (ARTIFACTS/f'logs/{f.name}-{mutation}-replay.log').write_text(replay.stdout+replay.stderr)
                    assert replay.returncode!=0 and ('nondetermin' in replay.stderr.lower() or 'marker' in replay.stderr.lower()),(mutation,replay.stdout,replay.stderr)
                    report['rejected_replays']+=1
                assert journal.read_bytes()==before and not Path(str(journal)+'.equal-replay').exists()
                report['histories']+=1
                report['cases'].append(f'{version}/{policy}/{codec or "default"}: renamed mutable ID honored workflow-task versus whole-workflow failure policy; task failures recovered with original code and cancellation; extra/removed/renamed calls rejected offline without executing callbacks')
                print(report['cases'][-1],flush=True)
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

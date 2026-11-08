"""Replay actual Go SDK side-effect marker histories in V and replace a Go worker."""
RESULT_REPORT='side-effect-go-results.json'
import hashlib,json,signal,subprocess,shutil
from integration import ARTIFACTS,ROOT,TemporalFixture
import side_effect_probe as effects
GO=ARTIFACTS/'tools/side-effect-go-oracle'
CORPUS=ROOT/'_tests/data/side-effect-go'
V=ARTIFACTS/'tools/v-side-effects'

def main():
    effects.BINARY=V
    CORPUS.mkdir(parents=True,exist_ok=True)
    report={'passed':False,'histories':0,'cases':[],'oracle':{'sdk':'v1.35.0','files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'_tests/native/side_effect_go').glob('*') if p.is_file()}}}
    try:
        for version,server in [('1-32',ARTIFACTS/'tools/temporal'),('1-33',ARTIFACTS/'tools/temporal-protocol')]:
            with TemporalFixture('side-effect-go-'+version,server_binary=server) as f:
                directory=ARTIFACTS/'side-effect-go-fixtures'/f.queue;directory.mkdir(parents=True)
                config=directory/'configuration';config.write_text('one')
                journal=directory/'executions';journal.write_text('')
                f.env.update(V_EFFECT_CONFIG=str(config),V_EFFECT_JOURNAL=str(journal),V_EFFECT_FORBID='1',V_SDK_EXAMPLE_CODEC='',V_SDK_EXAMPLE_STORAGE_DIR='')
                instance=f.launch(GO,'go-worker',('worker',))
                started=subprocess.run([str(GO),'start'],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=30)
                assert started.returncode==0,started.stderr
                handle=json.loads(started.stdout)
                f.wait_for(lambda:effects.invoke(f,'state',json.dumps(handle))['phase']==1)
                config.write_text('two');effects.invoke(f,'change',json.dumps(handle))
                f.wait_for(lambda:effects.invoke(f,'state',json.dumps(handle))['phase']==2)
                state=effects.invoke(f,'state',json.dumps(handle));assert state['configuration']=='two' and len(state['tokens'])==3,state
                instance.kill();assert instance.wait(timeout=15)==-9
                reset=subprocess.run([str(GO),'reset-sticky',json.dumps(handle)],cwd=ROOT,env=f.env,capture_output=True,text=True,timeout=30)
                assert reset.returncode==0,reset.stderr
                instance=f.launch(V,'v-worker',('worker',))
                assert effects.invoke(f,'state',json.dumps(handle))==state
                effects.invoke(f,'finish',json.dumps(handle))
                result=effects.invoke(f,'result',json.dumps(handle));assert result['status']=='completed' and json.loads(result['data'])==state,result
                effects.verify_history(f,handle,state,'default',False)
                json_history=f.histories/(handle['run_id']+'.json');json_history.write_text(json.dumps(f.history(handle)))
                binary_history=f.histories/(handle['run_id']+'.bin');effects.invoke(f,'history',json.dumps(handle),binary_history)
                instance.send_signal(signal.SIGTERM);assert instance.wait(timeout=30)==0
                environment=dict(f.env)
            replay=subprocess.run([str(V),'replay',str(binary_history),handle['workflow_id'],f.queue],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
            (ARTIFACTS/f'logs/side-effect-go-{version}-v-replay.log').write_text(replay.stdout+replay.stderr)
            assert replay.returncode==0,replay.stderr
            reference=subprocess.run([str(GO),'replay',str(json_history)],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
            (ARTIFACTS/f'logs/side-effect-go-{version}-go-replay.log').write_text(reference.stdout+reference.stderr)
            assert reference.returncode==0,reference.stderr
            assert journal.read_bytes()==b'' and not journal.with_suffix('.equal-replay').exists()
            assert reference.returncode==0
            record={'version':version,'handle':handle,'task_queue':f.queue,'state':state,'files':{}}
            for source,suffix in ((json_history,'json'),(binary_history,'bin')):
                target=CORPUS/(version+'.'+suffix);shutil.copy2(source,target);record['files'][target.name]=hashlib.sha256(target.read_bytes()).hexdigest()
            report.setdefault('records',[]).append(record)
            report['histories']+=1
            report['cases'].append(version+': Go SDK binary SideEffect and protobuf-JSON MutableSideEffect wrappers/unchanged call counters replayed by V; Go-to-V replacement preserved queries/results without invoking V callbacks; both Go and V replayed completed history offline')
            print(report['cases'][-1],flush=True)
        report['passed']=True
        (CORPUS/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    finally:(ARTIFACTS/RESULT_REPORT).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()

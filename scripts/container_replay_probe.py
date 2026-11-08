"""Source-bound upgrade/rollback corpus using real archived runtime containers.

Separate from check.sh: requires already validated images and a Docker daemon.
"""
import argparse,hashlib,json,os,subprocess,time,uuid
from pathlib import Path


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def counts(history):
    result={}
    for event in history['events']:
        kind=event['eventType'].removeprefix('EVENT_TYPE_').replace('_','').lower()
        result[kind]=result.get(kind,0)+1
    return result

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--architecture',choices=('amd64','arm64'),required=True)
    for name in ('old-manifest','old-check','new-check','new-container-proof','client','temporal-cli','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--old-runtime',required=True);parser.add_argument('--new-runtime',required=True);parser.add_argument('--validation',required=True)
    args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True);(output/'logs').mkdir(exist_ok=True)
    report={'passed':False,'architecture':args.architecture,'cases':[],'histories':[],'cleanup_failures':[]}
    def docker(*command,check=True):
        try:result=subprocess.run(['docker',*map(str,command)],capture_output=True,text=True,timeout=90 if check else 15)
        except subprocess.TimeoutExpired:
            if check:raise
            report['cleanup_failures'].append(list(map(str,command)))
            return 'DOCKER_CLEANUP_TIMEOUT'
        if check:assert result.returncode==0,result.stderr
        return result.stdout.strip()
    def image(tag):return json.loads(docker('image','inspect',tag))[0]
    try:
        manifest=json.loads(args.old_manifest.read_text());old_check=json.loads(args.old_check.read_text());new_check=json.loads(args.new_check.read_text());container_proof=json.loads(args.new_container_proof.read_text())
        old=image(args.old_runtime);new=image(args.new_runtime);validation=image(args.validation)
        assert old_check['passed'] and new_check['passed'] and container_proof['passed']
        assert old_check['source']['sha256']==manifest['source']
        assert old['Id']==manifest['images'][args.architecture]['retained_runtime_image']
        assert new['Id']==container_proof['images']['runtime'] and validation['Id']==container_proof['images']['validation']
        assert old['Architecture']==new['Architecture']==validation['Architecture']==args.architecture
        assert old['Config']['User']==new['Config']['User']=='65532:65532'
        report['sources']={'old':old_check['source']['sha256'],'new':new_check['source']['sha256']}
        report['images']={'old':old['Id'],'new':new['Id'],'validation':validation['Id']}
        report['inputs']={str(path.resolve()):digest(path) for path in (args.old_manifest,args.old_check,args.new_check,args.new_container_proof,args.client,args.temporal_cli,Path(__file__))}
        for direction,before,after in [('upgrade',args.old_runtime,args.new_runtime),('rollback',args.new_runtime,args.old_runtime)]:
            for codec in (False,True):
                prefix='v-corpus-'+uuid.uuid4().hex[:14];containers=[];network=False;handles={};address='';environment={key:value for key,value in os.environ.items() if not key.startswith(('TEMPORAL_', 'V_SDK_EXAMPLE_'))};environment['V_SDK_EXAMPLE_CODEC']='prefix' if codec else ''
                try:
                    docker('network','create',prefix);network=True
                    server=prefix+'-server';containers.append(server)
                    docker('run','-d','--name',server,'--network',prefix,'--network-alias','temporal','-p','127.0.0.1::7233','--entrypoint','/build/artifacts/tools/temporal',args.validation,'server','start-dev','--ip','0.0.0.0','--port','7233','--headless')
                    address='127.0.0.1:'+docker('port',server,'7233/tcp').rsplit(':',1)[1]
                    deadline=time.monotonic()+45
                    while True:
                        ready=subprocess.run([str(args.temporal_cli),'operator','cluster','health','--address',address],capture_output=True,text=True,timeout=5)
                        if ready.returncode==0:break
                        assert time.monotonic()<deadline,ready.stderr;time.sleep(.1)
                    def client(command,handle=None,**options):
                        call=[str(args.client),'--address','http://'+address,'--task-queue',prefix,command]
                        values={key:value for key,value in (handle or {}).items() if key in ('workflow_id','run_id')};values.update(options)
                        for key,value in values.items():call+=['--'+key.replace('_','-'),str(value)]
                        result=subprocess.run(call,env=environment,capture_output=True,text=True,timeout=60);assert result.returncode==0,result.stderr
                        return json.loads(result.stdout)
                    def history(handle):
                        result=subprocess.run([str(args.temporal_cli),'workflow','show','--address',address,'--workflow-id',handle['workflow_id'],'--run-id',handle['run_id'],'--output','json'],capture_output=True,text=True,timeout=15);assert result.returncode==0,result.stderr
                        return json.loads(result.stdout)
                    def worker(runtime,kind,label):
                        name=prefix+'-'+label;containers.append(name)
                        docker('run','-d','--name',name,'--network',prefix,'--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=32m','--cap-drop','ALL','--security-opt','no-new-privileges','-e','TEMPORAL_ADDRESS=http://temporal:7233','-e','TEMPORAL_TASK_QUEUE='+prefix,'-e','V_SDK_EXAMPLE_CODEC=prefix' if codec else 'V_SDK_EXAMPLE_CODEC=','--entrypoint','/opt/v-sdk/bin/'+kind,runtime)
                        return name
                    activity_before=worker(before,'v-worker','activity-before');workflow_before=worker(before,'v-workflow-worker','workflow-before')
                    handles['counter']=client('start',workflow_id=prefix+'-counter',type='v.counter',input='{"label":"container-corpus"}')
                    update=client('update',handles['counter'],name='add',update_id='once',input='{"amount":5}');assert update['status']=='completed',update
                    handles['restart']=client('start',workflow_id=prefix+'-restart',type='v.restart',input='{"name":"container-corpus","delay_seconds":10}')
                    handles['parallel']=client('start',workflow_id=prefix+'-parallel',type='v.parallel',input='{"name":"container-corpus","delay_ms":100,"wait_for_cancel":true}')
                    handles['local']=client('start',workflow_id=prefix+'-local',type='v.default-retry',input='{"local":true,"fail_until":2}')
                    assert client('result',handles['local'])['status']=='completed'
                    deadline=time.monotonic()+40
                    while counts(history(handles['restart'])).get('timerstarted',0)!=1:
                        assert time.monotonic()<deadline;time.sleep(.1)
                    for name in (workflow_before,activity_before):
                        docker('kill','--signal','KILL',name);assert docker('inspect','--format','{{.State.ExitCode}}',name)=='137'
                    activity_after=worker(after,'v-worker','activity-after');workflow_after=worker(after,'v-workflow-worker','workflow-after')
                    assert json.loads(client('query',handles['counter'],name='state')['data'])['total']==5
                    assert json.loads(client('update',handles['counter'],name='add',update_id='once',input='{"amount":5}')['data'])['total']==5
                    client('signal',handles['counter'],name='add',input='{"amount":7}')
                    assert json.loads(client('query',handles['counter'],name='state')['data'])['total']==12
                    client('signal',handles['counter'],name='finish');assert client('result',handles['counter'])['status']=='completed'
                    assert client('result',handles['restart'])['status']=='completed'
                    client('cancel',handles['parallel']);assert client('result',handles['parallel'])['status']=='canceled'
                    before_logs=docker('logs',activity_before);after_logs=docker('logs',activity_after)
                    assert handles['restart']['run_id'] in before_logs and handles['restart']['run_id'] in after_logs,'both Activity worker versions must execute the restart workflow'
                    for label,handle in handles.items():
                        document=history(handle);events=counts(document);assert events.get('workflowtaskfailed',0)==0,events
                        filename=f'{direction}-{codec}-{label}.bin';path=output/filename;client('history',handle,file=path)
                        (output/(filename+'.json')).write_text(json.dumps(document,indent=2)+'\n')
                        entry={'direction':direction,'codec':codec,'workflow_id':handle['workflow_id'],'run_id':handle['run_id'],'queue':prefix,'history':filename,'sha256':digest(path),'events':events,'replays':{}}
                        for version,runtime in [('old',args.old_runtime),('new',args.new_runtime)]:
                            call=['docker','run','--rm','--network','none','--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=32m','--cap-drop','ALL','--security-opt','no-new-privileges','--mount',f'type=bind,src={path},dst=/history.bin,readonly','-e','V_SDK_EXAMPLE_CODEC=prefix' if codec else 'V_SDK_EXAMPLE_CODEC=','--entrypoint','/opt/v-sdk/bin/v-replay',runtime,'--history','/history.bin','--workflow-id',handle['workflow_id'],'--task-queue',prefix]
                            replay=subprocess.run(call,capture_output=True,text=True,timeout=60);(output/f'logs/{filename}-{version}-replay.log').write_text(replay.stdout+replay.stderr);assert replay.returncode==0,replay.stderr
                            entry['replays'][version]=True
                        report['histories'].append(entry)
                    for name in (workflow_after,activity_after):
                        docker('stop','--time','20',name);assert docker('inspect','--format','{{.State.ExitCode}}',name)=='0',docker('logs',name)
                    report['cases'].append(f'{direction} codec={codec}: both workflow/Activity workers replaced; Update dedupe, signals, timer, cancellation and local retry passed; four histories replayed with both runtimes')
                finally:
                    for name in reversed(containers):
                        (output/f'logs/{name}.log').write_text(docker('logs',name,check=False));docker('rm','-f',name,check=False)
                    if network:docker('network','rm',prefix,check=False)
            print(args.architecture,direction,'CONTAINER_REPLAY_OK',flush=True)
        assert len(report['histories'])==16 and not report['cleanup_failures'],report['cleanup_failures']
        report['passed']=True
    finally:(output/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'architecture':args.architecture,'histories':len(report['histories']),'replays':2*len(report['histories'])},indent=2))

if __name__=='__main__':main()

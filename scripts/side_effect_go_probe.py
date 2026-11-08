"""Verify frozen real Go SDK marker histories using V replay, without a Go runtime."""
RESULT_REPORT='side-effect-go-replay-results.json'
import argparse,base64,hashlib,json,os,subprocess,tempfile
from pathlib import Path
from types import SimpleNamespace
from integration import ARTIFACTS,ROOT
import side_effect_probe as effects
BINARY=ARTIFACTS/'tools/v-side-effects'

def main(binary=BINARY,report_name=RESULT_REPORT):
    report={'passed':False,'histories':0,'cases':[]}
    try:
        corpus=ROOT/'_tests/data/side-effect-go';manifest=json.loads((corpus/'manifest.json').read_text())
        assert manifest['passed'] and manifest['oracle']['sdk']=='v1.35.0' and len(manifest['records'])==2
        for name,expected in manifest['oracle']['files'].items():
            assert hashlib.sha256((ROOT/'_tests/native/side_effect_go'/name).read_bytes()).hexdigest()==expected,name
        for record in manifest['records']:
            for name,expected in record['files'].items():
                assert Path(name).name==name and hashlib.sha256((corpus/name).read_bytes()).hexdigest()==expected,name
            history=json.loads((corpus/(record['version']+'.json')).read_text())
            fake=SimpleNamespace(history=lambda handle:history)
            effects.verify_history(fake,record['handle'],record['state'],'default',False)
            completed=[event['workflowExecutionCompletedEventAttributes'] for event in history['events'] if 'workflowExecutionCompletedEventAttributes' in event]
            assert len(completed)==1
            assert json.loads(base64.b64decode(completed[0]['result']['payloads'][0]['data']))==record['state']
            with tempfile.TemporaryDirectory(prefix='temporal-v-go-markers-') as directory:
                journal=Path(directory)/'executions';journal.write_bytes(b'')
                environment={key:value for key,value in os.environ.items() if not key.startswith(('TEMPORAL_','V_EFFECT_','V_SDK_EXAMPLE_'))}
                environment.update(V_EFFECT_FORBID='1',V_EFFECT_JOURNAL=str(journal),V_EFFECT_CONFIG=str(Path(directory)/'must-not-read'))
                replay=subprocess.run([str(binary),'replay',str(corpus/(record['version']+'.bin')),record['handle']['workflow_id'],record['task_queue']],cwd=ROOT,env=environment,capture_output=True,text=True,timeout=45)
                (ARTIFACTS/f'logs/{report_name}-{record["version"]}.log').write_text(replay.stdout+replay.stderr)
                assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stderr
                assert journal.read_bytes()==b'' and not Path(str(journal)+'.equal-replay').exists()
            report['histories']+=1
            report['cases'].append(record['version']+': frozen Go SDK 1.35.0 histories passed source/file hashes, independent binary-value and mutable counter inspection, and callback-free V replay including Go protobuf-JSON wrappers')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=BINARY);parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args();main(args.binary,args.report)

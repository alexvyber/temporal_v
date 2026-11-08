"""Completion observers see encoded, stored, owned snapshots before submission."""
RESULT_REPORT='worker-completion-results.json'
import argparse,hashlib,json,os,tempfile
from pathlib import Path
from integration import ARTIFACTS
import worker_hooks_probe,panic_probe

def main(binary,report_name,quick=False):
 report={'passed':False,'histories':0,'subreports':{},'cases':[]}
 directory=Path(tempfile.mkdtemp(prefix='completion-audit-'+report_name.removesuffix('.json')+'-',dir=ARTIFACTS));report['audit_directory']=str(directory)
 previous=os.environ.get('V_WORKER_COMPLETION_DIR');os.environ['V_WORKER_COMPLETION_DIR']=str(directory)
 try:
  name=report_name.removesuffix('.json')+'-lifecycle.json'
  worker_hooks_probe.main(binary,name,quick)
  value=json.loads((ARTIFACTS/name).read_text());assert value['passed'];report['histories']+=value['histories'];report['subreports'][name]=hashlib.sha256((ARTIFACTS/name).read_bytes()).hexdigest()
  if not quick:
   name=report_name.removesuffix('.json')+'-panics.json'
   panic_probe.main(binary,name)
   value=json.loads((ARTIFACTS/name).read_text());assert value['passed'];report['histories']+=value['histories'];report['subreports'][name]=hashlib.sha256((ARTIFACTS/name).read_bytes()).hexdigest()
  records={};hashes={}
  for path in directory.glob('*.json'):
   value=json.loads(path.read_text());key=(value['run_id'],value['before']);records.setdefault(key,{})[value['label']]=value;hashes[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
  assert records,'completion observers did not run'
  for key,observed in records.items():
   assert set(observed)=={'mutate','panic','after'},(key,observed)
   assert observed['panic']['before']==observed['after']['before']==observed['mutate']['before']
   assert observed['after']['before']==observed['after']['after']
   if observed['mutate']['mutated']:assert observed['mutate']['before']!=observed['mutate']['after']
  values=[value['mutate'] for value in records.values()]
  assert any(value['mutated'] for value in values),'fixture did not mutate shared nested bytes'
  if not quick:
   assert any(value['failed'] for value in values),'missing failed workflow-task completions'
   assert any(value['external'] for value in values),'observer ran before external storage'
   assert any(value['encoded_prefix'] for value in values),'observer ran before codecs'
   assert any(value['contextual'] for value in values),'missing scoped codec metadata'
  report['observed_completions']=len(records);report['mutated_snapshots']=sum(value['mutated'] for value in values);report['audit_files']=hashes
  report['cases'].append('Every completion reached mutate, panic and subsequent observer with the same original wire hash; nested mutation changed only its owned snapshot and panic did not suppress later hooks/Core submission')
  if not quick:report['cases'].append('Observed successful/failed task completions after codec framing, contextual metadata and external storage; live failure/cancellation, handler drain, SIGKILL replacement and offline replay passed')
  report['passed']=True
 finally:
  if previous is None:os.environ.pop('V_WORKER_COMPLETION_DIR',None)
  else:os.environ['V_WORKER_COMPLETION_DIR']=previous
  (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({key:value for key,value in report.items() if key!='audit_files'},indent=2))

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)

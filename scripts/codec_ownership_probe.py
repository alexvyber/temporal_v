"""Mutating codec callbacks cannot change retained payloads or cached outputs."""
RESULT_REPORT='codec-ownership-results.json'
import argparse,hashlib,json,os,subprocess
from pathlib import Path
from integration import ARTIFACTS,ROOT
import raw_headers_probe,workflow_headers_probe,event_groups_probe

def main(binary,unit_binary,report_name,quick=False):
 report={'passed':False,'histories':0,'cases':[],'reports':{}}
 previous=os.environ.get('V_CODEC_OWNERSHIP')
 try:
  unit=subprocess.run([str(unit_binary)],cwd=ROOT,capture_output=True,text=True,timeout=90)
  (ARTIFACTS/('logs/'+report_name+'-unit.log')).write_text(unit.stdout+unit.stderr)
  assert unit.returncode==0 and unit.stdout.strip()=='CODEC_OWNERSHIP_OK rounds=1800 threads=8',unit.stdout+unit.stderr
  report['unit']={'passed':True,'rounds':1800,'threads':8,'binary_sha256':hashlib.sha256(unit_binary.read_bytes()).hexdigest()}
  os.environ['V_CODEC_OWNERSHIP']='true'
  for module,key,subset in [(raw_headers_probe,'activities',quick),(workflow_headers_probe,'interactions',quick),(event_groups_probe,'event-groups',True)]:
   child_name=report_name.removesuffix('-results.json')+'-'+key+'-results.json'
   module.main(binary,child_name,subset)
   p=ARTIFACTS/child_name;child=json.loads(p.read_text());assert child['passed']
   report['reports'][child_name]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'histories':child['histories']}
   report['histories']+=child['histories']
  report['cases'].append('Shared-buffer eight-thread tests covered data, binary metadata, nested external details, unknown fields, retained payload-converter/codec outputs, consecutive stages and sibling isolation')
  report['cases'].append('Deliberately mutating reversible codec passed remote/local/dynamic Activities, workflow/event/query/validator headers, UI Event Groups and summaries, explicit scoped errors, storage, worker loss and replay; UI scope case uses the expanded first server profile')
  report['passed']=True
 finally:
  if previous is None:os.environ.pop('V_CODEC_OWNERSHIP',None)
  else:os.environ['V_CODEC_OWNERSHIP']=previous
  (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--unit-binary',type=Path,default=ARTIFACTS/'tools/v-codec-ownership');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.unit_binary,args.report,args.quick)

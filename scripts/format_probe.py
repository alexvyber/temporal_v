"""Verify every owned V source using the pinned v fmt, without rewriting files."""
RESULT_REPORT='format-results.json'
from pathlib import Path
import hashlib,json,subprocess
from integration import ARTIFACTS,ROOT

def main():
 report={'passed':False,'cases':[]}
 try:
  files=sorted(path for path in ROOT.rglob('*.v') if not any(part in ('.git','__pycache__') for part in path.relative_to(ROOT).parts))
  before={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
  command=[str(ARTIFACTS/'tools/v/v'),'-d','use_bundled_libgc','fmt','-verify','-no-migrate-json2',*map(str,files)]
  value=subprocess.run(command,cwd=ROOT,capture_output=True,text=True,timeout=180)
  (ARTIFACTS/'logs/format-check.log').write_text(value.stdout+value.stderr)
  assert value.returncode==0,value.stdout[-2000:]+value.stderr[-2000:]
  assert before=={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files},'format verification modified source'
  report.update(passed=True,files=len(files),cases=['Every owned V file, including generated schemas and SDK traversal, passed pinned v fmt verification','Verification preserved every source byte'])
 finally:(ARTIFACTS/RESULT_REPORT).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':main()

"""Verify every owned V source using the pinned v fmt, without rewriting files."""
RESULT_REPORT='format-results.json'
from pathlib import Path
import hashlib,io,json,unittest
from integration import ARTIFACTS,ROOT
from format_v import format_files
from prepare_formatter import verify
from formatter_test import FormatterTests

def main():
 report={'passed':False,'cases':[]}
 try:
  files=sorted(path for path in ROOT.rglob('*.v') if not any(part in ('.git','__pycache__') for part in path.relative_to(ROOT).parts))
  before={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
  receipt=verify(ARTIFACTS)
  report['formatter']={key:receipt[key] for key in ('version','lock_sha256','binaries')}
  value=format_files(files,check=True,artifacts=ARTIFACTS,cwd=ROOT,capture_output=True,text=True,timeout=180)
  (ARTIFACTS/'logs/format-check.log').write_text(value.stdout+value.stderr)
  assert value.returncode==0,value.stdout[-2000:]+value.stderr[-2000:]
  output=io.StringIO()
  tests=unittest.TextTestRunner(stream=output,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(FormatterTests))
  with (ARTIFACTS/'logs/format-check.log').open('a') as log:log.write(output.getvalue())
  assert tests.wasSuccessful(),output.getvalue()
  report['generator_regressions']=tests.testsRun
  assert before=={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files},'format verification modified source'
  report.update(passed=True,files=len(files),cases=['Every owned V file, including generated schemas and SDK traversal, passed source-pinned V 0.5.2 formatter verification','Verification preserved every source byte','Real protobuf, RPC, activation, and storage generators reproduce canonical bytes; drift and formatter tampering are rejected'])
 finally:(ARTIFACTS/RESULT_REPORT).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':main()

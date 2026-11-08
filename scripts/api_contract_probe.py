"""Check the frozen API and adversarially verify signature change detection."""
RESULT_REPORT='api-contract-results.json'
from pathlib import Path
import json
import api_contract
from integration import ARTIFACTS

def main():
 report={'passed':False,'cases':[]}
 try:
  value=api_contract.verify()
  declarations=value['declarations'];assert len(declarations)>20
  assert 'text_ref("'+value['module_version']+'")' in (api_contract.ROOT/'native/core_shim.c').read_text()
  for filename in ['sdk/transport.v','corebridge/transport.v']:
   assert "client_version" in (api_contract.ROOT/filename).read_text() and "'"+value['module_version']+"'" in (api_contract.ROOT/filename).read_text()
  for name,old,new in [('sdk/workflow.v','fn (WorkflowContextView, []PayloadValue) !Workflow','fn () Workflow'),('sdk/workflow.v','view WorkflowContextView','view string')]:
   source=(api_contract.ROOT/name).read_text();assert old in source
   assert api_contract.declarations(source.replace(old,new,1))!=declarations[name]
  sample='pub fn sample(value string) !string { return value }\n'
  assert api_contract.declarations(sample)==api_contract.declarations(sample.replace('return value','return value + "changed"'))
  assert api_contract.canonical('pub fn sample(value string) !string')==api_contract.canonical('pub fn sample(\n value string\n) !string')
  assert api_contract.canonical("pub const label = 'two  spaces'")!=api_contract.canonical("pub const label = 'two spaces'")
  assert api_contract.declarations('pub type Choice = string\n | int\n')!=api_contract.declarations('pub type Choice = string\n | bool\n')
  opaque='pub struct Context {\nmut:\n private int\npub:\n visible string\n}\n'
  assert api_contract.declarations(opaque)==api_contract.declarations(opaque.replace('private int','private bool'))
  assert api_contract.declarations(opaque)!=api_contract.declarations(opaque.replace('visible string','visible bool'))
  report.update(passed=True,module_version=value['module_version'],declaration_count=sum(map(len,declarations.values())),cases=['Current exported SDK declarations match frozen major-1 contract','Old factory and query signatures are rejected; sum alternatives are covered','Implementation-only function body changes preserve the API contract'])
 finally:(ARTIFACTS/RESULT_REPORT).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':main()

"""Validate the historical replay-baseline-v1 compiler, not the consumer compiler."""
RESULT_REPORT='v-runtime-results.json'
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from integration import ARTIFACTS,ROOT


def main():
    report={'passed':False,'cases':[]}
    try:
        tool=ROOT/'scripts/prepare_v_runtime.py'
        source=(ARTIFACTS/'tools/v').resolve()
        manifest=json.loads((ROOT/'locks/v-runtime.json').read_text())
        for flag in (['--check'],[]):
            result=subprocess.run([sys.executable,str(tool),str(source),*flag],capture_output=True,text=True,timeout=30)
            assert result.returncode==0,result.stderr
        compiler_hash=hashlib.sha256((source/'v').read_bytes()).hexdigest()
        with tempfile.TemporaryDirectory(prefix='v-panic-runtime-') as temporary:
            target=Path(temporary);runtime=target/manifest['file'];runtime.parent.mkdir(parents=True)
            runtime.write_text('unknown runtime\n')
            for flag in (['--check'],[]):
                result=subprocess.run([sys.executable,str(tool),str(target),*flag],capture_output=True,text=True,timeout=30)
                assert result.returncode!=0
                assert runtime.read_text()=='unknown runtime\n'
            optimized=subprocess.run([sys.executable,'-O',str(tool),str(target),'--check'],capture_output=True,text=True,timeout=30)
            assert optimized.returncode!=0 and runtime.read_text()=='unknown runtime\n',optimized.stderr
            runtime.write_bytes((source/manifest['file']).read_bytes())
            (target/'v').write_text('#!/bin/sh\nexit 93\n');(target/'v').chmod(0o755)
            (target/'.temporal-v-runtime.json').write_text('{}')
            result=subprocess.run([sys.executable,str(tool),str(target),'--check'],capture_output=True,text=True,timeout=30)
            assert result.returncode!=0 and 'receipt changed' in result.stderr,result.stderr
            (target/'.temporal-v-runtime.json').unlink()
            reverse=subprocess.run(['patch','--batch','-R','-p1','-i',str(ROOT/manifest['patch'])],cwd=target,capture_output=True,text=True,timeout=30)
            assert reverse.returncode==0,reverse.stderr
            assert hashlib.sha256(runtime.read_bytes()).hexdigest()==manifest['original_sha256']
            result=subprocess.run([sys.executable,str(tool),str(target)],capture_output=True,text=True,timeout=30)
            assert result.returncode!=0 and '93' in result.stderr,result.stderr
            assert hashlib.sha256(runtime.read_bytes()).hexdigest()==manifest['patched_sha256']
            assert not (target/'.temporal-v-runtime.json').exists(),'failed compiler rebuild must not get a receipt'
        assert hashlib.sha256((source/'v').read_bytes()).hexdigest()==compiler_hash
        report.update(passed=True,runtime_sha256=manifest['patched_sha256'],compiler_sha256=compiler_hash,cases=[
            'Pinned runtime patch and compiler receipt passed; repeated preparation left compiler bytes unchanged',
            'Unknown runtime sources and mismatched compiler receipts were rejected without compiler execution',
            'Exact upstream runtime reconstructed by reverse patch; forward patch restored the pinned extension',
            'Deliberately failed compiler rebuild produced no success receipt'])
    finally:(ARTIFACTS/RESULT_REPORT).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()

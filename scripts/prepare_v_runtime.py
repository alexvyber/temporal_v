"""Apply and verify the pinned V panic-stack runtime extension before compiling."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT=Path(__file__).resolve().parent.parent

def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def require(condition,message):
    if not condition:raise RuntimeError(message)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('compiler_source',type=Path)
    parser.add_argument('--check',action='store_true')
    args=parser.parse_args()
    source=args.compiler_source.resolve()
    manifest=json.loads((ROOT/'locks/v-runtime.json').read_text())
    runtime=source/manifest['file']
    patch=ROOT/manifest['patch']
    require(digest(patch)==manifest['patch_sha256'],'V runtime patch changed without updating its lock')
    receipt=source/'.temporal-v-runtime.json'
    if args.check:
        require(digest(runtime)==manifest['patched_sha256'],'V panic runtime does not match the pinned extension; run bootstrap')
        record=json.loads(receipt.read_text())
        require(record=={'runtime_sha256':manifest['patched_sha256'],'compiler_sha256':digest(source/'v')},'V compiler/runtime receipt changed; run bootstrap')
        return
    actual=digest(runtime)
    require(actual in (manifest['original_sha256'],manifest['patched_sha256']),'Refusing to patch an unknown V runtime source')
    if actual==manifest['original_sha256']:
        subprocess.run(['patch','--batch','--forward','-p1','-i',str(patch)],cwd=source,check=True)
    require(digest(runtime)==manifest['patched_sha256'],'V runtime patch result differs from the lock')
    expected={'runtime_sha256':manifest['patched_sha256'],'compiler_sha256':digest(source/'v') if (source/'v').is_file() else ''}
    if receipt.is_file() and json.loads(receipt.read_text())==expected:return
    environment=dict(os.environ,V_MACOS_V3_NO_FALLBACK='1',V_C_ERROR_BUG_REPORT_DISABLED='1')
    command=[str(source/'v'),'-d','use_bundled_libgc','self'] if (source/'v').is_file() else ['make','local=1']
    subprocess.run(command,cwd=source,env=environment,check=True)
    receipt.write_text(json.dumps({'runtime_sha256':manifest['patched_sha256'],'compiler_sha256':digest(source/'v')},indent=2)+'\n')

if __name__=='__main__':main()

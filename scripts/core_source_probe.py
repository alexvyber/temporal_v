"""Build source guard accepts the exact pin/patch tree and rejects stale native inputs."""
RESULT_REPORT='core-source-results.json'
import hashlib,json,subprocess,sys,tempfile
from pathlib import Path
from integration import ARTIFACTS,ROOT

def main():
    report={'passed':False,'cases':[]}
    try:
        manifest=json.loads((ROOT/'locks/core-source.json').read_text());source=ARTIFACTS/'tools/sdk-rust'
        def inspect(directory):return subprocess.run([sys.executable,str(ROOT/'scripts/verify_core_source.py'),str(directory)],cwd=ROOT,capture_output=True,text=True,timeout=30)
        valid=inspect(source);assert valid.returncode==0,valid.stderr
        name='crates/common/src/telemetry/otel.rs';expected=manifest['files'][name]
        with tempfile.TemporaryDirectory(prefix='core-source-guard-',dir=ARTIFACTS) as folder:
            temporary=Path(folder)
            for filename in manifest['files']:
                target=temporary/filename;target.parent.mkdir(parents=True,exist_ok=True);target.symlink_to(source/filename)
            changed=temporary/name;changed.unlink();changed.write_bytes((source/name).read_bytes()+b'\n// stale compiler fixture\n')
            rejected=inspect(temporary);assert rejected.returncode!=0 and 'Core source mismatch: '+name in rejected.stderr,rejected.stderr
            changed.unlink();changed.symlink_to(source/name)
            cargo_name='crates/sdk-core-c-bridge/Cargo.toml';cargo=temporary/cargo_name;cargo.unlink()
            stale=(source/cargo_name).read_text().replace('"http-json"','"http-json", "reqwest-client"')
            assert stale!=(source/cargo_name).read_text();cargo.write_text(stale)
            rejected=inspect(temporary);assert rejected.returncode!=0 and 'Core source mismatch: '+cargo_name in rejected.stderr,rejected.stderr
            cargo.unlink();cargo.symlink_to(source/cargo_name)
            changed.unlink();missing=inspect(temporary);assert missing.returncode!=0 and 'missing Core source: '+name in missing.stderr,missing.stderr
        assert hashlib.sha256((source/name).read_bytes()).hexdigest()==expected
        report['cases']=[f"{len(manifest['files'])} exact Core source files and {len(manifest['patches'])} maintained patch hashes verified; build.sh enforces the source guard before and after native compilation",'A stale HTTP metrics implementation and missing native source were rejected without touching the original tree; header-compatible stale Cargo feature selections cannot silently pass the source guard']
        report['reference']=manifest['reference'];report['passed']=True
    finally:(ARTIFACTS/RESULT_REPORT).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()

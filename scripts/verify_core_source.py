"""Reject stale or locally divergent Core sources before compiling the SDK ABI."""
import hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def verify(source):
    manifest=json.loads((ROOT/'locks/core-source.json').read_text());errors=[]
    for item in manifest['patches']:
        path=ROOT/item['path']
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=item['sha256']:errors.append('patch mismatch: '+item['path'])
    for name,expected in manifest['files'].items():
        relative=Path(name)
        if relative.is_absolute() or '..' in relative.parts:raise ValueError('invalid Core manifest path')
        path=source/relative
        if not path.is_file():errors.append('missing Core source: '+name)
        elif hashlib.sha256(path.read_bytes()).hexdigest()!=expected:errors.append('Core source mismatch: '+name)
    if errors:raise RuntimeError('\n'.join(errors[:20])+f'\nCore source verification rejected {len(errors)} mismatches; use a fresh pinned extraction with the maintained patches.')
    return manifest

if __name__=='__main__':
    try:
        manifest=verify(Path(sys.argv[1]));print(f"Verified {len(manifest['files'])} Core source files and {len(manifest['patches'])} maintained patches")
    except (OSError,ValueError,RuntimeError,IndexError) as error:
        print(str(error),file=sys.stderr);sys.exit(1)

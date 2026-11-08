"""Install and verify the source-pinned V formatter independently of the SDK compiler."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def location(artifacts=None):
    artifacts = Path(artifacts or os.environ.get('SPIKE_ARTIFACTS', ROOT.parent / 'artifacts/v-sdk-spike')).resolve()
    return artifacts / 'tools/v-formatter'


def verify(artifacts=None):
    source = location(artifacts)
    receipt_path = source / '.formatter-receipt.json'
    if not receipt_path.is_file():
        raise RuntimeError('Pinned V formatter is not prepared; run python3 scripts/prepare_formatter.py')
    receipt = json.loads(receipt_path.read_text())
    if receipt['lock_sha256'] != digest(ROOT / 'locks/v-formatter.json'):
        raise RuntimeError('V formatter lock differs from its installation; remove tools/v-formatter from the artifacts directory and rerun prepare_formatter.py')
    if receipt['platform'] != platform.system() or receipt['architecture'] != platform.machine():
        raise RuntimeError('V formatter installation belongs to another platform')
    for name, expected in {**receipt['sources'], **receipt['binaries']}.items():
        if digest(source / name) != expected:
            raise RuntimeError('Pinned V formatter input/image changed: ' + name)
    return receipt


def extract(archive, destination):
    files = {}
    with tarfile.open(archive) as bundle:
        for member in bundle:
            name = PurePosixPath(member.name)
            if name.is_absolute() or '..' in name.parts:
                raise RuntimeError('Unsafe formatter archive member: ' + member.name)
            relative = Path(*name.parts[1:])
            if not name.parts[1:]:
                continue
            target = destination / relative
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                content = bundle.extractfile(member).read()
                target.write_bytes(content)
                target.chmod(member.mode & 0o777)
                files[str(relative)] = hashlib.sha256(content).hexdigest()
            else:
                raise RuntimeError('Unsupported formatter archive member: ' + member.name)
    return files


def prepare():
    source = location()
    if source.exists():
        return verify()
    lock_path = ROOT / 'locks/v-formatter.json'
    lock = json.loads(lock_path.read_text())
    source.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='v-formatter-', dir=source.parent) as temporary:
        staging = Path(temporary) / 'source'
        staging.mkdir()
        inputs = {}
        for kind, destination in [('source', staging), ('bootstrap', staging / 'vc')]:
            archive = Path(temporary) / (kind + '.tar.gz')
            subprocess.run(['curl', '--fail', '--silent', '--show-error', '--location', lock[kind]['url'], '-o', str(archive)], check=True)
            if digest(archive) != lock[kind]['sha256']:
                raise RuntimeError('V formatter ' + kind + ' archive checksum mismatch')
            for name, expected in extract(archive, destination).items():
                inputs[str((destination / name).relative_to(staging))] = expected
        environment = dict(os.environ, VFLAGS='', VMODULES=str(Path(temporary) / 'modules'))
        compiler = staging / 'v'
        environment['VEXE'] = str(compiler)
        formatter = staging / 'cmd/tools/vfmt'
        subprocess.run(['cc', '-std=c99', '-w', '-o', str(compiler), str(staging / 'vc/v.c'), '-lm', '-lpthread'], env=environment, check=True)
        subprocess.run([str(compiler), '-gc', 'none', '-cc', 'cc', '-nocache', '-o', str(formatter), str(staging / 'cmd/tools/vfmt.v')], cwd=staging, env=environment, check=True)
        receipt = {
            'version': lock['version'],
            'platform': platform.system(),
            'architecture': platform.machine(),
            'lock_sha256': digest(lock_path),
            'sources': inputs,
            'binaries': {name: digest(staging / name) for name in ['v', 'cmd/tools/vfmt']},
        }
        (staging / '.formatter-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        staging.rename(source)
    return verify()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    receipt = verify() if args.check else prepare()
    print('Verified V formatter ' + receipt['version'])

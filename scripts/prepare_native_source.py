"""Prepare the checksum-pinned, patched Rust Core without installing a V compiler."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from prepare_formatter import ROOT, digest, extract
from verify_core_source import verify


def prepare(artifacts):
    source = artifacts / 'tools/sdk-rust'
    if source.exists():
        verify(source)
        return source
    source.parent.mkdir(parents=True, exist_ok=True)
    lock = json.loads((ROOT / 'locks/core-archive.json').read_text())
    with tempfile.TemporaryDirectory(prefix='core-source-', dir=source.parent) as temporary:
        temporary = Path(temporary)
        archive = temporary / 'source.tar.gz'
        subprocess.run(['curl', '--fail', '--silent', '--show-error', '--location', '--retry', '3',
                        lock['url'], '-o', str(archive)], check=True)
        if digest(archive) != lock['sha256']:
            raise RuntimeError('Rust Core archive checksum mismatch')
        staging = temporary / 'source'
        extract(archive, staging)
        manifest = json.loads((ROOT / 'locks/core-source.json').read_text())
        for item in manifest['patches']:
            patch = ROOT / item['path']
            if digest(patch) != item['sha256']:
                raise RuntimeError('Rust Core patch checksum mismatch: ' + item['path'])
            subprocess.run(['patch', '--batch', '--forward', '-p1', '-i', str(patch)], cwd=staging, check=True)
        shutil.copy2(ROOT / 'locks/core.Cargo.lock', staging / 'Cargo.lock')
        verify(staging)
        staging.rename(source)
    return source


if __name__ == '__main__':
    artifacts = Path(os.environ.get('SPIKE_ARTIFACTS', ROOT.parent / 'artifacts/v-sdk-spike')).resolve()
    print(prepare(artifacts))

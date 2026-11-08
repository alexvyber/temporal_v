"""Install the pinned time-skipping test server, verifying archive and binary."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tarfile
import tempfile


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def main():
    root = Path(__file__).resolve().parent.parent
    lock = json.loads((root / 'locks/test-server.json').read_text())
    artifacts = Path(os.environ.get('SPIKE_ARTIFACTS', root.parent / 'artifacts/v-sdk-spike'))
    target = {('Darwin', 'arm64'): 'macOS_arm64', ('Linux', 'x86_64'): 'linux_amd64', ('Linux', 'aarch64'): 'linux_arm64'}[(platform.system(), platform.machine())]
    expected = lock['archives'][target]
    directory = artifacts / 'tools' / ('test-server-' + lock['version'])
    executable = directory / 'temporal-test-server'
    if executable.is_file():
        if digest(executable) != expected['executable_sha256']:
            raise RuntimeError('installed test-server executable checksum mismatch')
        executable.chmod(0o755)
        return
    directory.mkdir(parents=True, exist_ok=True)
    archive = artifacts / 'tools' / f"test-server-{lock['version']}-{target}.tar.gz"
    if not archive.is_file():
        with tempfile.NamedTemporaryFile(dir=archive.parent, prefix='test-server-', delete=False) as temporary:
            downloaded = Path(temporary.name)
        try:
            subprocess.run(['curl', '--fail', '--silent', '--show-error', '--location', '--max-time', '180', expected['url'], '-o', str(downloaded)], check=True)
            if digest(downloaded) != expected['sha256']:
                raise RuntimeError('test-server archive checksum mismatch')
            downloaded.rename(archive)
        finally:
            downloaded.unlink(missing_ok=True)
    if digest(archive) != expected['sha256']:
        raise RuntimeError('cached test-server archive checksum mismatch')
    with tarfile.open(archive, 'r:gz') as package:
        member = package.getmember(f"temporal-test-server_{lock['version']}_{target}/temporal-test-server")
        if not member.isfile():
            raise RuntimeError('test-server archive member is not a regular file')
        data = package.extractfile(member).read()
    if hashlib.sha256(data).hexdigest() != expected['executable_sha256']:
        raise RuntimeError('test-server binary checksum mismatch')
    executable.write_bytes(data)
    executable.chmod(0o755)


if __name__ == '__main__':
    main()

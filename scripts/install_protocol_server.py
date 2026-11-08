"""Install the pinned prerelease used for protocol fields absent in the stable server."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import tarfile
import tempfile
from urllib.request import urlopen


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def main():
    root = Path(__file__).resolve().parent.parent
    artifacts = Path(os.environ.get('SPIKE_ARTIFACTS', root.parent / 'artifacts/v-sdk-spike')).resolve()
    lock = json.loads((root / 'locks/protocol-server.json').read_text())
    architecture = {'arm64': 'arm64', 'aarch64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine())
    target = platform.system().lower() + '_' + str(architecture)
    asset = lock['assets'][target]
    tools = artifacts / 'tools'
    tools.mkdir(parents=True, exist_ok=True)
    binary = tools / 'temporal-protocol'
    receipt = tools / 'temporal-protocol.json'
    unrecognized = binary.exists() and not receipt.is_file()
    if binary.exists() and receipt.is_file():
        installed = json.loads(receipt.read_text())
        if installed.get('kind') != 'temporal_protocol_test_server':
            raise RuntimeError('refusing to replace an unrecognized protocol server')
        if installed['archive_sha256'] == asset['sha256'] and installed['binary_sha256'] == digest(binary):
            print(binary)
            return
    archive = tools / f'protocol-server-{lock["cli_version"]}-{target}.tar.gz'
    if not archive.exists() or digest(archive) != asset['sha256']:
        with tempfile.NamedTemporaryFile(prefix='protocol-download-', dir=tools, delete=False) as output:
            download = Path(output.name)
            try:
                with urlopen(asset['url'], timeout=60) as source:
                    shutil.copyfileobj(source, output)
                output.flush()
                if digest(download) != asset['sha256']:
                    raise RuntimeError('protocol server archive checksum mismatch')
                download.replace(archive)
            finally:
                download.unlink(missing_ok=True)
    with tarfile.open(archive) as source:
        candidates = [item for item in source.getmembers() if Path(item.name).name == 'temporal' and item.isfile()]
        if len(candidates) != 1:
            raise RuntimeError('protocol server archive does not contain exactly one regular executable')
        with source.extractfile(candidates[0]) as content, tempfile.NamedTemporaryFile(prefix='protocol-executable-', dir=tools, delete=False) as output:
            staging = Path(output.name)
            try:
                shutil.copyfileobj(content, output)
                output.flush()
                staging.chmod(0o755)
                checksum = digest(staging)
                if unrecognized and digest(binary) != checksum:
                    raise RuntimeError('existing protocol server does not match the verified archive')
                staging.replace(binary)
            finally:
                staging.unlink(missing_ok=True)
    with tempfile.NamedTemporaryFile(prefix='protocol-receipt-', dir=tools, mode='w', delete=False) as output:
        staging = Path(output.name)
        try:
            output.write(json.dumps({'kind': 'temporal_protocol_test_server', 'cli_version': lock['cli_version'],
                                     'server_revision': lock['server_revision'], 'target': target,
                                     'archive_sha256': asset['sha256'], 'binary_sha256': checksum}, indent=2) + '\n')
            output.flush()
            staging.replace(receipt)
        finally:
            staging.unlink(missing_ok=True)
    print(binary)


if __name__ == '__main__': main()

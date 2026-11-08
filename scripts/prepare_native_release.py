"""Maintainer tool: place native builds in the installable V module tree."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent.parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(target, libraries, profile, source_manifest):
    source = ROOT / 'locks/core-source.json'
    if source_manifest and source_manifest.read_bytes() != source.read_bytes():
        raise RuntimeError('Native build used a different Rust Core source manifest')
    destination = ROOT / 'native/prebuilt' / target
    destination.mkdir(parents=True, exist_ok=True)
    extension = 'dylib' if target.startswith('darwin-') else 'so'
    for stem in ['libtemporalio_sdk_core_c_bridge', 'libtemporal_v_payloads']:
        library = destination / f'{stem}.{extension}'
        shutil.copy2(libraries / library.name, library)
        if extension == 'dylib':
            subprocess.run(['install_name_tool', '-id', '@rpath/' + library.name, str(library)], check=True)
            subprocess.run(['strip', '-Sx', str(library)], check=True)
            subprocess.run(['codesign', '--force', '--sign', '-', str(library)], check=True)
        else:
            # Linux exports are already stripped by Dockerfile.native.
            if not source_manifest:
                raise RuntimeError('Linux binaries require their exported source manifest')
    manifest = {
        'target': target,
        'profile': profile,
        'core_source_sha256': digest(source),
        'header_sha256': digest(ROOT / 'native/include/temporal-sdk-core-c-bridge.h'),
        'core_cargo_lock_sha256': digest(ROOT / 'locks/core.Cargo.lock'),
        'payload_cargo_lock_sha256': digest(ROOT / 'locks/payloads.Cargo.lock'),
        'files': {path.name: digest(path) for path in sorted(destination.glob('*.' + extension))},
    }
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', required=True, choices=['darwin-arm64', 'linux-amd64', 'linux-arm64'])
    parser.add_argument('--libraries', required=True, type=Path)
    parser.add_argument('--profile', required=True, choices=['debug', 'release'])
    parser.add_argument('--source-manifest', type=Path)
    args = parser.parse_args()
    prepare(args.target, args.libraries, args.profile, args.source_manifest)

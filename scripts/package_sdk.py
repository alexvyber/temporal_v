"""Build a self-contained local V module package from verified build inputs.

This creates a development artifact, not a signed/published production release.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', choices=('debug', 'release'), default=os.environ.get('SPIKE_BUILD_PROFILE', 'debug'))
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    artifacts = Path(os.environ.get('SPIKE_ARTIFACTS', root.parent / 'artifacts/v-sdk-spike')).resolve()
    target = platform.system().lower() + '-' + platform.machine()
    parent = artifacts / 'packages'
    parent.mkdir(exist_ok=True)
    destination = parent / target
    if destination.exists():
        manifest = destination / 'manifest.json'
        if not manifest.is_file() or json.loads(manifest.read_text()).get('kind') != 'temporal_v_development_package':
            raise RuntimeError('refusing to replace an unrecognized package directory')
    with tempfile.TemporaryDirectory(prefix='package-', dir=parent) as temporary:
        staging = Path(temporary) / 'artifact'
        module = staging / 'modules/temporal_v'
        module.mkdir(parents=True)
        for name in ('sdk', 'corebridge', 'temporal', 'native'):
            shutil.copytree(root / name, module / name)
        shutil.copy2(root / 'v.mod', module / 'v.mod')
        shutil.copytree(artifacts / 'vmodules/protobuf', staging / 'modules/protobuf', ignore=shutil.ignore_patterns('.git', '__pycache__'))
        extension = 'dylib' if platform.system() == 'Darwin' else 'so'
        library_dir = module / 'native/lib'
        library_dir.mkdir()
        for stem in ('libtemporalio_sdk_core_c_bridge', 'libtemporal_v_payloads'):
            library_name = stem + '.' + extension
            library = library_dir / library_name
            shutil.copy2(artifacts / 'rust-target' / arguments.profile / library_name, library)
            if platform.system() == 'Darwin':
                subprocess.run(['install_name_tool', '-id', '@rpath/' + library_name, str(library)], check=True)
                subprocess.run(['codesign', '--force', '--sign', '-', str(library)], check=True)
            elif platform.system() == 'Linux':
                subprocess.run(['patchelf', '--set-soname', library_name, str(library)], check=True)
            else:
                raise RuntimeError('package target must be macOS or Linux')
        (module / 'corebridge/library_flags.c.v').write_text(
            'module corebridge\n\n#flag -L @DIR/../native/lib\n'
            '#flag -ltemporalio_sdk_core_c_bridge\n'
            '#flag -ltemporal_v_payloads\n'
            '#flag -Wl,-rpath,@DIR/../native/lib\n')
        licenses = staging / 'LICENSES'
        licenses.mkdir()
        source = artifacts / 'tools/sdk-rust'
        for relative, name in (
            ('LICENSE.txt', 'temporal-rust-core.txt'),
            ('crates/protos/protos/api_upstream/LICENSE', 'temporal-api.txt'),
            ('crates/protos/protos/api_cloud_upstream/LICENSE', 'temporal-cloud-api.txt'),
        ):
            shutil.copy2(source / relative, licenses / name)
        shutil.copy2(root / 'README.md', staging / 'SOURCE_README.md')
        for relative in ('scripts/prepare_v_runtime.py','patches/v-panic-stack.patch','locks/v-runtime.json'):
            target_file=staging/'toolchain'/relative
            target_file.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(root/relative,target_file)
        (staging / 'README.md').write_text(
            '# Temporal V development package\n\n'
            'Set VMODULES to this package’s modules directory and import '
            '`temporal_v.sdk`. The package includes the pinned generated '
            'schemas, protobuf runtime, Core header and native library.\n\n'
            'Prepare the pinned V source with `python3 toolchain/scripts/prepare_v_runtime.py /path/to/v` '
            'before compiling consumers. This applies the maintained panic-stack runtime extension '
            'and rebuilds that compiler; an unpatched compiler cannot build this package.\n\n'
            'This is a local development artifact. Production parity, Linux '
            'validation and the complete release supply-chain audit remain required.\n')
        hashes = {str(path.relative_to(staging)): sha256(path) for path in sorted(staging.rglob('*')) if path.is_file()}
        manifest = {'kind': 'temporal_v_development_package', 'production_ready': False,
                    'target': target, 'profile': arguments.profile,
                    'core_revision': '67174ea96e8a06bc33137036e73cfaf508aa9dd7', 'sha256': hashes}
        (staging / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        (staging / 'SHA256SUMS').write_text(''.join(f'{digest}  {name}\n' for name, digest in hashes.items()))
        if destination.exists():
            shutil.rmtree(destination)
        staging.rename(destination)
    print(destination)


if __name__ == '__main__':
    main()

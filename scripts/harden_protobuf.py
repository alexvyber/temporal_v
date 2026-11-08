"""Apply the pinned decoder/generator fix only to a recognized dependency tree."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dependency', type=Path, required=True)
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    lock = json.loads((root / 'locks/protobuf-hardening.json').read_text())
    snapshot = {name: hashlib.sha256((args.dependency / name).read_bytes()).hexdigest()
                for name in lock['after']}
    if snapshot == lock['after']:
        print('Verified protobuf decoder/generator hardening')
        return
    if args.verify_only:
        raise RuntimeError('protobuf dependency is not patched; rebuild the SDK before testing')
    if snapshot != lock['before']:
        raise RuntimeError('protobuf dependency does not match either pinned patch state; refusing to modify it')
    command = ['patch', '-d', str(args.dependency), '-p1', '--batch', '--forward', '-i',
               str(root / 'patches/protobuf-hardening.patch')]
    subprocess.run(command + ['--dry-run'], check=True, stdout=subprocess.PIPE)
    subprocess.run(command, check=True, stdout=subprocess.PIPE)
    actual = {name: hashlib.sha256((args.dependency / name).read_bytes()).hexdigest()
              for name in lock['after']}
    if actual != lock['after']: raise RuntimeError('protobuf dependency differs from the pinned patched source')
    print('Applied verified protobuf decoder/generator hardening')


if __name__ == '__main__': main()

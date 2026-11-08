"""Generate actual Core schemas without invoking vpbgen's implicit formatter."""
import argparse
import os
import re
from pathlib import Path
import subprocess

from format_v import formatted_source
from prepare_formatter import ROOT


def generate(root, artifacts, check=False):
    protos = artifacts / 'tools/sdk-rust/crates/protos/protos'
    command = [str(artifacts / 'tools/vpbgen'), '-json', '-m', 'temporal']
    for directory in ['api_upstream', 'local', 'testsrv_upstream', 'api_cloud_upstream', '']:
        command += ['-I', str(protos / directory)]
    # stdout is raw V. vpbgen's -o path silently runs the build compiler's fmt.
    command.append(str(root / 'schemas/sdk.proto'))
    result = subprocess.run(command, check=True, capture_output=True, text=True,
                            env=dict(os.environ, VMODULES=str(artifacts / 'vmodules')))
    raw = result.stdout.replace('import protobuf\n', 'import alexvyber.temporal_v.protobuf\n').replace('import json2\n', 'import x.json2\n')
    for name in re.findall(r'^pub fn (\w+)\.decode\(', raw, re.M):
        raw += f'\npub fn (message {name}) decode_owned(data []u8) !{name} {{ return {name}.decode(data)! }}\n'
        raw += f'\npub fn (message {name}) validate_protojson(data string) ! {{ {name}.from_json(data)! }}\n'
    content = formatted_source(raw, 'core_pb.v')
    destination = root / 'temporal/core_pb.v'
    if check:
        if destination.read_text() != content:
            raise RuntimeError('core_pb.v differs from pinned protobuf schemas; regenerate')
    else:
        destination.write_text(content)
    print('Generated Core protobuf schemas with the pinned formatter')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    artifacts = Path(os.environ.get('SPIKE_ARTIFACTS', ROOT.parent / 'artifacts/v-sdk-spike')).resolve()
    generate(ROOT, artifacts, args.check)

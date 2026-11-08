"""Reproduce schema failures with unmodified generators in isolated directories."""
import json
import os
from pathlib import Path
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = Path(os.environ.get('SPIKE_ARTIFACTS', ROOT.parent / 'artifacts/v-sdk-spike'))
PROBES = ARTIFACTS / 'probes'
LOGS = ARTIFACTS / 'logs'
V = ARTIFACTS / 'tools/v/v'
SCHEMAS = ARTIFACTS / 'tools/sdk-rust/crates/protos/protos'
REVISION = '64811280130b2b4bda45eba707512412ace968ad'


def run(label, args, cwd, env):
    result = subprocess.run([str(arg) for arg in args], cwd=cwd, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
    (LOGS / f'{label}.log').write_text(result.stdout)
    return result


def main():
    modules = PROBES / 'vmodules'
    raw_source = modules / 'protobuf'
    raw_output = PROBES / 'raw-core'
    (raw_output / 'temporal').mkdir(parents=True, exist_ok=True)
    if not raw_source.exists():
        raw_source.mkdir(parents=True)
        archive = PROBES / 'protobuf.tar.gz'
        urllib.request.urlretrieve(f'https://codeload.github.com/we-be/protobuf.v/tar.gz/{REVISION}', archive)
        subprocess.run(['tar', '-xzf', str(archive), '-C', str(raw_source), '--strip-components=1'], check=True)
    env = dict(os.environ, VMODULES=str(modules), V_C_ERROR_BUG_REPORT_DISABLED='1')
    raw_generator = PROBES / 'raw-vpbgen'
    built = run('baseline-generator-build', [V, '-d', 'use_bundled_libgc', '-o', raw_generator,
                raw_source / 'cmd/vpbgen'], ROOT, env)
    assert built.returncode == 0, built.stdout

    service = SCHEMAS / 'api_upstream/temporal/api/workflowservice/v1/service.proto'
    upstream = run('upstream-schema-attempt', [V, '-d', 'use_bundled_libgc', 'pbgen', '-check',
                   '-m', 'temporal', service], SCHEMAS / 'api_upstream', env)
    assert upstream.returncode != 0, 'Unexpectedly succeeded: re-evaluate findings'
    thirdparty = run('thirdparty-service-attempt', [raw_generator, '-m', 'temporal', '-I',
                     SCHEMAS / 'api_upstream', '-o', PROBES / 'service_pb.v', service], ROOT, env)
    assert thirdparty.returncode != 0 and 'extend' in thirdparty.stdout, thirdparty.stdout

    generated = run('baseline-core-generation', [raw_generator, '-m', 'temporal',
                    '-I', SCHEMAS / 'api_upstream', '-I', SCHEMAS / 'local',
                    '-o', raw_output / 'temporal/core_pb.v',
                    SCHEMAS / 'local/temporal/sdk/core/core_interface.proto'], ROOT, env)
    assert generated.returncode == 0, generated.stdout
    (raw_output / 'v.mod').write_text("Module { name: 'schema_probe' }\n")
    (raw_output / 'main.v').write_text('module main\nimport temporal\nfn main() {\n'
                                     ' println(temporal.ActivityTaskCompletion{}.encode().len)\n}\n')
    compiled = run('baseline-core-build', [V, '-d', 'use_bundled_libgc', '-o', PROBES / 'raw-probe',
                   raw_output], raw_output, env)
    assert compiled.returncode != 0 and 'Callback_Nexus' in compiled.stdout, compiled.stdout[-4000:]
    report = {
        'upstream_public_service_exit': upstream.returncode,
        'thirdparty_public_service_exit': thirdparty.returncode,
        'thirdparty_core_generation_exit': generated.returncode,
        'thirdparty_unpatched_core_compile_exit': compiled.returncode,
        'generated_core_lines': len((raw_output / 'temporal/core_pb.v').read_text().splitlines()),
        'findings': ['Public service annotations contain unsupported extend declarations',
                     'Core message generation succeeds but unpatched generated code fails to compile',
                     'Oneof wrapper names collide with real nested message names',
                     'Single-member oneofs use type checks on aliases rather than sum types'],
    }
    (ARTIFACTS / 'schema-results.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

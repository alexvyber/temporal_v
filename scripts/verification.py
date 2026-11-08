"""Bind local build/check evidence to exact source, native library and executables."""
import ast
from validation_artifacts import verify_instrumented
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import re
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = Path(os.environ.get('SPIKE_ARTIFACTS', ROOT.parent / 'artifacts/v-sdk-spike')).resolve()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_snapshot():
    files = {}
    for path in sorted(ROOT.rglob('*')):
        if any(part in ('.git', '__pycache__') for part in path.relative_to(ROOT).parts): continue
        if path.is_file(): files[str(path.relative_to(ROOT))] = digest(path)
    fingerprint = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
    return {'sha256': fingerprint, 'files': files}


def compiled_inputs(snapshot):
    # Documentation and test-driver edits require new checks, but do not change
    # compiled V/C/Rust inputs. Keep both complete snapshots in the evidence.
    files = {name: value for name, value in snapshot['files'].items()
             if Path(name).suffix in ('.v', '.c', '.h', '.rs', '.proto', '.patch') or name == 'v.mod'
             or name.startswith(('locks/', 'patches/'))
             or name in ('scripts/build.sh', 'scripts/env.sh', 'scripts/bootstrap.sh', 'scripts/generate_service.py', 'scripts/build_payload_bridge.py', 'scripts/harden_protobuf.py', 'scripts/verify_core_source.py', 'scripts/prepare_v_runtime.py', 'scripts/generate_activation_decoder.py', 'scripts/build_upgrade_baseline.py', 'scripts/api_contract.py')
             or name in ('release/api-contract.json', 'release/compatibility/sdk120.json', 'release/compatibility/sdk120-source.tar.gz')}
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()


def write(name, value):
    (ARTIFACTS / name).write_text(json.dumps(value, indent=2) + '\n')


def build_start():
    snapshot = source_snapshot()
    write('build-verification-state.json', {'compiled_inputs': compiled_inputs(snapshot), 'started_at': time.time()})


def build():
    state = json.loads((ARTIFACTS / 'build-verification-state.json').read_text())
    if state['compiled_inputs'] != compiled_inputs(source_snapshot()):
        raise RuntimeError('compiled inputs changed while building executables; rebuild from a stable source')

    profile = os.environ.get('SPIKE_BUILD_PROFILE', 'debug')
    binaries = {f'tools/{name}': digest(ARTIFACTS / 'tools' / name) for name in
                ('v/v', 'v-worker', 'v-workflow-worker', 'v-upgrade-baseline-worker', 'v-upgrade-baseline-activities', 'v-upgrade-baseline-replay', 'v-upgrade-baseline-side-effects', 'v-client', 'v-replay', 'v-service', 'v-nexus-worker', 'v-environment', 'v-storage', 'v-plugins', 'v-transport', 'v-metrics', 'v-user-metrics', 'v-profiles', 'v-grpc', 'v-eager-activity', 'v-metric-export', 'v-deployment', 'v-legacy', 'v-schedule', 'v-batch', 'v-protobuf', 'v-protobuf-json', 'v-mixed-values', 'v-typed-values', 'v-raw-history', 'v-wait-deadlines', 'v-metadata-transport', 'v-visibility', 'v-concurrency', 'v-management', 'v-workflow-pause', 'v-replay-batch', 'v-side-effects', 'v-tracing', 'v-panic', 'v-codec-ownership', 'protobuf-rust-oracle', 'payload-bridge-tests', 'temporal', 'temporal-protocol', 'test-server-1.40.0/temporal-test-server')}
    library = f'rust-target/{profile}/libtemporalio_sdk_core_c_bridge.' + ('dylib' if platform.system() == 'Darwin' else 'so')
    binaries[library] = digest(ARTIFACTS / library)
    payload_library = f'rust-target/{profile}/libtemporal_v_payloads.' + ('dylib' if platform.system() == 'Darwin' else 'so')
    binaries[payload_library] = digest(ARTIFACTS / payload_library)
    snapshot = source_snapshot()
    write('build-verification.json', {'source': snapshot, 'compiled_inputs': compiled_inputs(snapshot), 'binaries': binaries, 'profile': profile, 'compiler_environment_flags': os.environ.get('VFLAGS', ''),
                                     'platform': platform.system(), 'architecture': platform.machine(), 'built_at': time.time()})


def start():
    snapshot = source_snapshot()
    write('check-verification.json', {'passed': False, 'source': snapshot, 'errors': ['verification has not completed']})
    stamp = json.loads((ARTIFACTS / 'build-verification.json').read_text())
    if stamp['compiled_inputs'] != compiled_inputs(snapshot):
        raise RuntimeError('compiled inputs changed since the last verified build; rebuild before running checks')
    for name, expected in stamp['binaries'].items():
        if digest(ARTIFACTS / name) != expected: raise RuntimeError('built artifact changed: ' + name)
    write('check-verification-state.json', {'build': stamp, 'source': snapshot, 'started_at': time.time()})


def finish(code):
    state = json.loads((ARTIFACTS / 'check-verification-state.json').read_text())
    errors = []
    if code: errors.append(f'check.sh exited with status {code}')
    if state['source']['sha256'] != source_snapshot()['sha256']: errors.append('source changed during checks')
    for name, expected in state['build']['binaries'].items():
        if digest(ARTIFACTS / name) != expected: errors.append('built artifact changed during checks: ' + name)
    reports = {}
    report_scripts = {}
    # Every required probe in check.sh writes a result report. Avoid accepting a
    # passing report left over from another source revision or interrupted run.
    for line in (ROOT / 'scripts/check.sh').read_text().splitlines():
        if not line.startswith('python3 "$SPIKE_ROOT/scripts/'): continue
        script = line.split('/scripts/', 1)[1].split('"', 1)[0]
        if script == 'verification.py': continue
        tree = ast.parse((ROOT / 'scripts' / script).read_text())
        declared = [node.value.value for node in tree.body if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == 'RESULT_REPORT' for target in node.targets)
                    and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)]
        if len(declared) != 1 or not re.fullmatch(r'[\w-]+-results\.json', declared[0]):
            errors.append('probe must declare exactly one RESULT_REPORT: ' + script); continue
        name = declared[0]
        if name in report_scripts:
            errors.append(f'duplicate report identity: {script} and {report_scripts[name]}: {name}'); continue
        report_scripts[name] = script
        path = ARTIFACTS / name
        if not path.is_file() or path.stat().st_mtime < state['started_at']:
            errors.append('missing/stale report: ' + name); continue
        result = json.loads(path.read_text())
        passed = bool(result) and all(case.get('passed') is True for case in result) if isinstance(result, list) else result.get('passed') is True
        reports[name] = {'passed': passed, 'sha256': digest(path)}
        if not passed: errors.append('failed report: ' + name)
    validation_binaries={}
    if not code:
        try:
            sanitizer=json.loads((ARTIFACTS/'sanitizer-results.json').read_text())
            validation_binaries=verify_instrumented(ARTIFACTS,sanitizer['instrumented_builds'],state['build']['binaries']['tools/v/v'])
        except (KeyError,ValueError,OSError) as failure:
            errors.append('invalid instrumented build evidence: '+str(failure))
    report = {**state, 'finished_at': time.time(), 'reports': reports, 'validation_binaries':validation_binaries, 'errors': errors, 'passed': not errors}
    write('check-verification.json', report)
    archive = ARTIFACTS / 'check-runs' / str(int(state['started_at']))
    archive.mkdir(parents=True, exist_ok=True)
    for name in reports: shutil.copy2(ARTIFACTS / name, archive / name)
    (archive / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print(f'Verified {len(reports)} reports against source {state["source"]["sha256"]}')
    return 0


if __name__ == '__main__':
    action = sys.argv[1]
    if action == 'build-start': build_start()
    elif action == 'build': build()
    elif action == 'start': start()
    elif action == 'finish': sys.exit(finish(int(sys.argv[2])))
    else: raise ValueError('unknown verification action')

"""Compile and run an external app using only the packaged V modules/library."""

RESULT_REPORT = 'package-results.json'
import hashlib
import json
import os
from pathlib import Path
import subprocess

from integration import TemporalFixture, ARTIFACTS, ROOT


SOURCE = '''module main
import os
import temporal_v.sdk
import time

struct Workflow {}
fn create(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow { return Workflow{} }
fn (mut workflow Workflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
    if event.kind == .cancel_requested { context.cancel()!; return }
    if event.kind == .started { context.timer(100 * time.millisecond)! }
    else if event.kind == .timer_fired { context.complete('"packaged SDK"')! }
}

fn main() {
    options := sdk.options_from_env() or { panic(err) }
    if os.args.len > 1 && os.args[1] == 'worker' {
        mut runtime := sdk.Runtime{}
        runtime.register('packaged.workflow', create) or { panic(err) }
        sdk.run_workflows(options, runtime) or { panic(err) }
    } else {
        client := sdk.connect(sdk.ClientOptions{address: options.address}) or { panic(err) }
        defer { client.close() }
        handle := client.start(sdk.WorkflowStart{workflow_id: 'packaged-workflow', workflow_type: 'packaged.workflow', task_queue: options.task_queue}) or { panic(err) }
        result := client.get_result(handle, 10000) or { panic(err) }
        assert result.status == .completed && result.data == '"packaged SDK"'
        println('PACKAGED_SDK_PASSED')
    }
}
'''


def main():
    report = {'passed': False, 'cases': []}
    try:
        packaged = subprocess.run(['python3', str(ROOT / 'scripts/package_sdk.py')], cwd=ROOT, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        assert packaged.returncode == 0, packaged.stderr
        package = Path(packaged.stdout.strip())
        manifest = json.loads((package / 'manifest.json').read_text())
        prepared=subprocess.run(['python3',str(package/'toolchain/scripts/prepare_v_runtime.py'),str(ARTIFACTS/'tools/v'),'--check'],capture_output=True,text=True,timeout=30)
        assert prepared.returncode==0,prepared.stderr
        for relative, expected in manifest['sha256'].items():
            assert hashlib.sha256((package / relative).read_bytes()).hexdigest() == expected, relative
        consumer = ARTIFACTS / 'external-consumer'
        consumer.mkdir(exist_ok=True)
        (consumer / 'v.mod').write_text("Module { name: 'external_consumer', version: '0.0.1', dependencies: ['temporal_v'] }\n")
        (consumer / 'main.v').write_text(SOURCE)
        environment = dict(os.environ, V_MACOS_V3_NO_FALLBACK="1", VMODULES=str(package / 'modules'))
        environment.pop('DYLD_LIBRARY_PATH', None)
        environment.pop('LD_LIBRARY_PATH', None)
        binary = consumer / 'application'
        compiled = subprocess.run([str(ARTIFACTS / 'tools/v/v'), '-d', 'use_bundled_libgc', '-o', str(binary), str(consumer)],
                                  cwd=consumer, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=120)
        (ARTIFACTS / 'logs/package-build.log').write_text(compiled.stdout + compiled.stderr)
        assert compiled.returncode == 0, compiled.stderr[-4000:]
        with TemporalFixture('package') as fixture:
            worker = fixture.launch(binary, 'worker', args=('worker',))
            result = subprocess.run([str(binary)], cwd=consumer, env=fixture.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
            assert result.returncode == 0 and 'PACKAGED_SDK_PASSED' in result.stdout, result.stderr
            worker.terminate()
            assert worker.wait(timeout=15) == 0
        report['cases'].append('Separate application imported temporal_v.sdk, compiled without custom include/link flags, and ran client/workflow against Temporal')
        report['cases'].append('Package contained native library/header, generated schemas and protobuf runtime; all file SHA256 checks passed')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

"""Compile a real consumer with ordinary V and the normally installed module."""
RESULT_REPORT = 'dependency-results.json'
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

from integration import TemporalFixture, ARTIFACTS, ROOT, counts

SOURCE = 'module main\nimport os\nimport time\nimport alexvyber.temporal_v.sdk\nstruct Greeting {}\nfn create(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow { return Greeting{} }\nfn (mut workflow Greeting) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {\n if event.kind == .started { context.timer(3 * time.second)! }\n if event.kind == .timer_fired { context.complete(\'"Hello"\')! }\n if event.kind == .cancel_requested { context.cancel()! }\n}\nfn main() {\n options := sdk.options_from_env() or { panic(err) }\n if os.args.len > 1 && os.args[1] == \'client\' {\n  client := sdk.connect(sdk.ClientOptions{address: options.address, namespace: options.namespace}) or { panic(err) }\n  defer { client.close() }\n  handle := client.start(sdk.WorkflowStart{workflow_id: \'normal-v-dependency\', workflow_type: \'greeting\', task_queue: options.task_queue}) or { panic(err) }\n  result := client.get_result(handle, 30000) or { panic(err) }\n  assert result.status == .completed && result.data == \'"Hello"\'\n  println(\'DEPENDENCY_OK\')\n  return\n }\n mut runtime := sdk.Runtime{}\n runtime.register(\'greeting\', create) or { panic(err) }\n sdk.run_workflows(options, runtime) or { panic(err) }\n}\n'


def main():
    report = {'passed': False, 'cases': []}
    try:
        compiler = shutil.which('v')
        if not compiler:
            raise RuntimeError('Install V normally and put v on PATH before running the consumer probe')
        environment = dict(os.environ, V_C_ERROR_BUG_REPORT_DISABLED='1')
        for name in ('VMODULES', 'VFLAGS', 'VEXE', 'LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH'):
            environment.pop(name, None)
        installed = Path.home() / '.vmodules/alexvyber/temporal_v'
        if not installed.exists():
            subprocess.run([compiler, 'link'], cwd=ROOT, env=environment, check=True)
        # Refuse to accidentally prove an unrelated installed SDK revision.
        for folder in ('sdk', 'corebridge', 'temporal', 'protobuf'):
            for path in (ROOT / folder).glob('*.v'):
                if (installed / path.relative_to(ROOT)).read_bytes() != path.read_bytes():
                    raise RuntimeError('Installed dependency differs from this source: ' + str(path))
        target = platform.system().lower() + '-' + {'aarch64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine(), platform.machine())
        manifest_path = installed / 'native/prebuilt' / target / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        for name, expected in manifest['files'].items():
            assert hashlib.sha256((manifest_path.parent / name).read_bytes()).hexdigest() == expected, name
        consumer = ARTIFACTS / 'external-consumer'
        consumer.mkdir(exist_ok=True)
        (consumer / 'v.mod').write_text("Module { name: 'external_consumer', version: '0.1.0', dependencies: ['https://github.com/alexvyber/temporal_v'] }\n")
        (consumer / 'main.v').write_text(SOURCE)
        binary = consumer / 'application'
        compiled = subprocess.run([compiler, '-o', str(binary), '.'], cwd=consumer, env=environment, capture_output=True, text=True, timeout=180)
        (ARTIFACTS / 'logs/dependency-build.log').write_text(compiled.stdout + compiled.stderr)
        compiled.check_returncode()
        with TemporalFixture('dependency') as fixture:
            worker = fixture.launch(binary, 'worker')
            client = fixture.launch(binary, 'client', args=('client',))
            handle = {'workflow_id': 'normal-v-dependency'}
            def timer_started():
                assert worker.poll() is None, 'consumer worker exited before timer started'
                try:
                    return counts(fixture.history(handle))['timerstarted'] > 0
                except AssertionError:
                    return False
            fixture.wait_for(timer_started)
            worker.kill()
            worker.wait(timeout=10)
            replacement = fixture.launch(binary, 'replacement')
            assert client.wait(timeout=40) == 0
            replacement_log = (ARTIFACTS / 'logs/dependency-replacement.log').read_text()
            assert 'replay=true start_timer' in replacement_log, replacement_log
            history = fixture.history(handle)
            assert counts(history)['workflowexecutioncompleted'] == 1
            assert counts(history)['timerstarted'] == 1
            assert counts(history)['workflowtaskfailed'] == 0
            replacement.terminate()
            assert replacement.wait(timeout=15) == 0
        report.update(passed=True, compiler=compiler,
                      compiler_sha256=hashlib.sha256(Path(compiler).read_bytes()).hexdigest(),
                      version=subprocess.check_output([compiler, 'version'], text=True).strip(),
                      installed_module=str(installed), native=manifest,
                      cases=['External project imported alexvyber.temporal_v.sdk and compiled with plain v, no VMODULES, compiler flags or loader environment',
                             'V client started a workflow; SIGKILL replacement replayed its timer history and completed it once',
                             'Replacement worker drained and exited successfully on SIGTERM'])
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

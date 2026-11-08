"""Core TOML/environment profiles, client operations and disposable TLS credentials."""
RESULT_REPORT = 'profiles-results.json'
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
from integration import ARTIFACTS, ROOT, TemporalFixture
from tls_probe import certificates, tls_proxy

BINARY = ARTIFACTS / 'tools/v-profiles'


def invoke(role, *args, env=None):
    result = subprocess.run([str(BINARY), role, *map(str, args)], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=35)
    (ARTIFACTS / f'logs/profiles-{role}.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        assert invoke('validation') == 'VALIDATION_OK'
        report['cases'].append('Core selected TOML profiles, normalized/removed gRPC metadata, applied environment precedence and disable flags, retained codec metadata and API-key TLS defaults; strict/malformed configs and invalid certificate sources failed with redacted default errors')
        with tempfile.TemporaryDirectory(prefix='v-profile-') as directory, TemporalFixture('profiles') as fixture:
            directory = Path(directory)
            profile = directory / 'client.toml'
            profile.write_text('[profile.default]\naddress = "127.0.0.1:1"\nnamespace = "file-must-be-overridden"\n[profile.default.grpc_meta]\nx_file = "file"\n')
            fixture.env['V_SDK_EXAMPLE_PROFILE_PATH'] = str(profile)
            before = fixture.workflows('before')
            handle = json.loads(invoke('start', env=fixture.env))
            state = fixture.call('query', handle, name='state', input='null')
            assert json.loads(state['data'])['total'] == 16, state
            before.kill(); assert before.wait(timeout=10) == -9
            after = fixture.workflows('after')
            assert invoke('finish', env=fixture.env) == 'CLIENT_OK'
            fixture.assert_clean_history(handle)
            history = ARTIFACTS / 'binary-histories/profiles.bin'
            invoke('history', history, env=fixture.env)
            after.terminate(); assert after.wait(timeout=20) == 0
            certificates(directory)
            for mutual in (False, True):
                with tls_proxy(directory, fixture.address, mutual) as (port, peers):
                    config = f'[profile.default]\naddress = "127.0.0.1:{port}"\n[profile.default.tls]\nserver_name = "localhost"\n'
                    if mutual:
                        for key, filename in (('server_ca_cert_path', 'ca.pem'), ('client_cert_path', 'client.pem'), ('client_key_path', 'client.key')):
                            config += key + ' = ' + json.dumps(str(directory / filename)) + '\n'
                    else:
                        config += 'server_ca_cert_data = ' + json.dumps((directory / 'ca.pem').read_text()) + '\n'
                    profile.write_text(config)
                    assert invoke('tls', profile, env=fixture.env) == 'TLS_OK'
                    if mutual: assert peers, 'profile did not authenticate the disposable client certificate'
                    profile.write_text(config.replace('server_name = "localhost"', 'server_name = "wrong.invalid"'))
                    assert invoke('tls-reject', profile, env=fixture.env) == 'TLS_REJECTED'
            report['cases'].append('A profile-configured V client started, signaled, queried and completed a workflow after SIGKILL/replacement; environment address/namespace overrides won and history stayed clean')
            report['cases'].append('Inline CA bytes and file-backed mTLS certificates crossed Rust profile parsing/V JSON decoding/Core connection setup; real TLS calls passed and wrong hostnames were rejected')
        result = subprocess.run([str(ARTIFACTS / 'tools/v-replay'), '--history', str(history), '--workflow-id', handle['workflow_id'], '--task-queue', fixture.queue],
                                cwd=ROOT, capture_output=True, text=True, timeout=35)
        assert result.returncode == 0, result.stderr
        report['cases'].append('The completed profile-client history replayed offline with the server stopped')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'profiles-sanitized-results.json'), default=RESULT_REPORT)
    args = parser.parse_args()
    main(args.binary, args.report)

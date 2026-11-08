"""Real Core gRPC through an ephemeral TLS/mTLS loopback proxy to Temporal."""

RESULT_REPORT = 'tls-results.json'
from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import threading

from integration import TemporalFixture, ARTIFACTS


def openssl(directory, *arguments):
    result = subprocess.run(['openssl', *arguments], cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert result.returncode == 0, result.stderr


def certificates(directory):
    # All keys are disposable local test material, never human/server credentials.
    openssl(directory, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=V SDK test CA',
            '-keyout', 'ca.key', '-out', 'ca.pem')
    for name in ('server', 'client'):
        openssl(directory, 'req', '-newkey', 'rsa:2048', '-nodes', '-subj', f'/CN={name}', '-keyout', f'{name}.key', '-out', f'{name}.csr')
        extensions = 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' if name == 'server' else 'extendedKeyUsage=clientAuth\n'
        (directory / f'{name}.ext').write_text(extensions)
        openssl(directory, 'x509', '-req', '-days', '1', '-in', f'{name}.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key',
                '-CAcreateserial', '-out', f'{name}.pem', '-extfile', f'{name}.ext')


@contextmanager
def tls_proxy(directory, upstream, require_client):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(directory / 'server.pem', directory / 'server.key')
    context.set_alpn_protocols(['h2'])
    if require_client:
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=str(directory / 'ca.pem'))
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(0.2)
    stopping = threading.Event()
    connections = []
    handlers = []
    peers = []

    def handle(raw):
        try:
            secured = context.wrap_socket(raw, server_side=True)
            connections.append(secured)
            if secured.getpeercert():
                peers.append(secured.getpeercert())
            assert secured.selected_alpn_protocol() == 'h2'
            plain = socket.create_connection(('127.0.0.1', int(upstream.split(':')[1])))
            connections.append(plain)

            def forward(source, destination):
                try:
                    while not stopping.is_set():
                        data = source.recv(65536)
                        if not data:
                            break
                        destination.sendall(data)
                except (OSError, ssl.SSLError):
                    pass
                finally:
                    for connection in (source, destination):
                        try:
                            connection.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
            outgoing = threading.Thread(target=forward, args=(secured, plain), daemon=True)
            outgoing.start()
            forward(plain, secured)
            outgoing.join(timeout=2)
        except (OSError, ssl.SSLError):
            raw.close()

    def accept():
        while not stopping.is_set():
            try:
                raw, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            connections.append(raw)
            thread = threading.Thread(target=handle, args=(raw,), daemon=True)
            handlers.append(thread)
            thread.start()

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[1], peers
    finally:
        stopping.set()
        listener.close()
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        thread.join(timeout=2)
        for handler in handlers:
            handler.join(timeout=2)


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('tls') as fixture, tempfile.TemporaryDirectory(prefix='temporal-v-tls-') as temp:
            directory = Path(temp)
            certificates(directory)
            client = ARTIFACTS / 'tools/v-client'
            for mutual in (False, True):
                with tls_proxy(directory, fixture.address, mutual) as (port, peers):
                    environment = dict(os.environ, TEMPORAL_TLS='true', TEMPORAL_TLS_DOMAIN='localhost',
                                       TEMPORAL_TLS_SERVER_CA_FILE=str(directory / 'ca.pem'),
                                       TEMPORAL_API_KEY='test-key-do-not-log')
                    if mutual:
                        environment.update(TEMPORAL_TLS_CLIENT_CERT_FILE=str(directory / 'client.pem'),
                                           TEMPORAL_TLS_CLIENT_KEY_FILE=str(directory / 'client.key'))
                    else:
                        environment.pop('TEMPORAL_TLS_CLIENT_CERT_FILE', None)
                        environment.pop('TEMPORAL_TLS_CLIENT_KEY_FILE', None)
                    address = f'https://127.0.0.1:{port}'
                    environment['TEMPORAL_ADDRESS'] = address
                    environment['TEMPORAL_TASK_QUEUE'] = fixture.queue
                    worker = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', f'worker-{mutual}', env=environment)
                    args = [str(client), '--address', address, '--task-queue', fixture.queue,
                            '--workflow-id', fixture.queue + f'-{mutual}', '--type', 'v.timer-cancel', '--input', '{}']
                    result = subprocess.run(args + ['start'], env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                    assert result.returncode == 0, result.stderr
                    handle = json.loads(result.stdout)
                    finished = subprocess.run([str(client), '--address', address, '--workflow-id', handle['workflow_id'],
                                               '--run-id', handle['run_id'], 'result'], env=environment,
                                              text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                    assert finished.returncode == 0 and json.loads(finished.stdout)['status'] == 'completed', finished.stderr
                    invalid = dict(environment, TEMPORAL_TLS_DOMAIN='incorrect.invalid')
                    failed = subprocess.run(args + ['start'], env=invalid, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                    assert failed.returncode != 0 and 'test-key-do-not-log' not in failed.stderr
                    if mutual:
                        assert peers, 'proxy did not authenticate a client certificate'
                        invalid = dict(environment)
                        invalid.pop('TEMPORAL_TLS_CLIENT_CERT_FILE')
                        invalid.pop('TEMPORAL_TLS_CLIENT_KEY_FILE')
                        failed = subprocess.run(args + ['start'], env=invalid, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                        assert failed.returncode != 0, 'mTLS unexpectedly accepted no client certificate'
                    worker.terminate()
                    assert worker.wait(timeout=15) == 0
                    report['cases'].append(('mTLS' if mutual else 'TLS') + ' client and worker completed a workflow; hostname verification rejected mismatch')
            report['cases'].append('mTLS rejected an absent client certificate and authenticated the configured certificate')
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

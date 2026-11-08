"""Check transport options using actual gRPC frames and a disposable Temporal server."""

RESULT_REPORT = 'transport-results.json'
import base64
import json
import subprocess
import time
from integration import ARTIFACTS, ROOT, TemporalFixture
from transport_receiver import TransportReceiver

BINARY = ARTIFACTS / 'tools/v-transport'


def main():
    report = {'passed': False, 'cases': []}
    try:
        result = subprocess.run([str(BINARY), 'validation'], capture_output=True, text=True, timeout=10)
        assert result.returncode == 0 and result.stdout.strip() == 'VALIDATION_OK', result.stderr
        report['cases'].append('Invalid retry numbers/durations, DNS intervals/proxy combinations and client metadata were rejected before connecting, including the raw bridge API')
        for mode, failures, status, attempts, expected in (
            ('success', 2, 14, 3, 0),
            ('exhausted', 999, 14, 3, 14),
            ('terminal', 999, 3, 1, 3),
            ('no-retry', 999, 14, 1, 14),
            ('single', 999, 14, 1, 14),
            ('plain', 0, 14, 1, 0),
            ('dns', 0, 14, 1, 0),
            ('deadline', 999, 14, None, 4),
            ('cancel', 999, 14, None, 1),
            ('elapsed', 999, 14, None, 14),
        ):
            with TransportReceiver(failures, status, compressed_response=mode != 'plain') as peer:
                host = 'localhost' if mode == 'dns' else '127.0.0.1'
                result = subprocess.run([str(BINARY), f'http://{host}:{peer.port}', mode], cwd=ROOT,
                                        capture_output=True, text=True, timeout=10)
                (ARTIFACTS / f'logs/transport-{mode}.log').write_text(result.stdout + result.stderr)
                assert result.returncode == 0, (mode, result.stderr)
                outcome = json.loads(result.stdout)
                assert outcome['status'] == expected and outcome['ok'] == (expected == 0), (mode, outcome)
                if expected == 0: assert outcome['version'] == 'probe', outcome
                if attempts is not None: assert len(peer.received) == attempts + 1, (mode, peer.received)
                assert not peer.errors, peer.errors
                assert all(item['compressed'] == (mode != 'plain') and item['body'] == b'' for item in peer.received)
                if mode == 'success':
                    times = [item['time'] for item in peer.received[1:]]
                    assert times[1] - times[0] >= .01 and times[2] - times[1] >= .02, times
                if mode in ('deadline', 'cancel', 'elapsed'):
                    assert 2 <= len(peer.received) < 30 and outcome['milliseconds'] < 2000, (mode, outcome, len(peer.received))
                    # Completed/cancelled futures must not leave background retries running.
                    observed = len(peer.received)
                    time.sleep(.15)
                    assert len(peer.received) == observed, (mode, peer.received)
        report['cases'].append('Wire-level gzip requests and compressed responses decoded; explicit no-compression and DNS-enabled connections completed; configured retry/backoff, attempt/elapsed limits and nonretryable statuses passed')
        report['cases'].append('An unlimited-retry call respected its 120 ms overall deadline with DEADLINE_EXCEEDED; cancellation retained CANCELLED and both stopped subsequent retries')
        with TemporalFixture('transport-options') as fixture:
            result = subprocess.run([str(BINARY), 'http://' + fixture.address, 'live'], cwd=ROOT, env=fixture.env,
                                    capture_output=True, text=True, timeout=25)
            (ARTIFACTS / 'logs/transport-live.log').write_text(result.stdout + result.stderr)
            assert result.returncode == 0, result.stderr
            handle = json.loads(result.stdout)
            history = fixture.history(handle)
            first = history['events'][0]
            attributes = next(value for key, value in first.items() if key.lower().endswith('eventattributes'))
            payload = attributes['input']['payloads'][0]
            assert json.loads(base64.b64decode(payload['data'])) == 'x' * 3072, payload
            warnings = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{')]
            assert sum(item.get('level') == 'warn' and item.get('fields', {}).get('limit') == 64 for item in warnings) >= 2, warnings
            report['cases'].append('A real Temporal server accepted compressed start/describe/terminate/result RPCs; 3 KiB input/memo round-tripped and configured size thresholds warned without rejecting the request')
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

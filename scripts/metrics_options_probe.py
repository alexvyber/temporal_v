"""Inspect exporter options in real Prometheus text and OTLP protobuf messages."""

RESULT_REPORT = 'metrics-options-results.json'
import json
import selectors
import struct
import subprocess
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from integration import ARTIFACTS, ROOT, TemporalFixture
from otlp_receiver import fields
from smoke import free_port

BINARY = ARTIFACTS / 'tools/v-metrics'


def wait_line(process, expected):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        assert selector.select(timeout=10), f'timed out waiting for {expected}'
        line = process.stdout.readline().strip()
        assert line == expected, (line, process.poll())


def metrics(body):
    for number, wire, resource in fields(body):
        if (number, wire) != (1, 2): continue
        for number, wire, scope in fields(resource):
            if (number, wire) != (2, 2): continue
            for number, wire, metric in fields(scope):
                if (number, wire) != (2, 2): continue
                entries = list(fields(metric))
                name = next(value.decode() for number, wire, value in entries if (number, wire) == (1, 2))
                unit = next((value.decode() for number, wire, value in entries if (number, wire) == (3, 2)), '')
                for kind, wire, data in entries:
                    if kind not in (5, 7, 9, 10) or wire != 2: continue
                    values = list(fields(data))
                    temporality = next((value for number, wire, value in values if (number, wire) == (2, 0)), 0)
                    for number, wire, point in values:
                        if (number, wire) != (1, 2): continue
                        details = list(fields(point))
                        count = next((int.from_bytes(value, 'little') for number, wire, value in details if (number, wire) == (4, 1)), 0)
                        boundaries = []
                        for number, wire, value in details:
                            if kind == 9 and (number, wire) == (7, 2):
                                boundaries.extend(struct.unpack('<' + 'd' * (len(value) // 8), value))
                        scale = next((value for number, wire, value in details if kind == 10 and (number, wire) == (6, 0)), 0)
                        scale = (scale >> 1) ^ -(scale & 1)
                        yield {'name': name, 'unit': unit, 'kind': kind, 'temporality': temporality,
                               'count': count, 'boundaries': boundaries, 'scale': scale}


def main():
    report = {'passed': False, 'cases': []}
    try:
        result = subprocess.run([str(BINARY), 'validation'], capture_output=True, text=True, timeout=10)
        assert result.returncode == 0 and result.stdout.strip() == 'VALIDATION_OK', result.stderr
        report['cases'].append('Nonfinite, unordered and empty buckets, incompatible exporters/aggregations, invalid scales/sizes and empty exponential suffixes were rejected before network construction')
        with TemporalFixture('metrics-options') as fixture:
            port = free_port()
            with (ARTIFACTS / 'logs/metrics-options-prometheus.log').open('w') as log:
                process = subprocess.Popen([str(BINARY), 'prometheus', 'http://' + fixture.address, f'127.0.0.1:{port}'],
                                           env=fixture.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True)
                try:
                    wait_line(process, 'READY')
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=5) as response: text = response.read().decode()
                    (ARTIFACTS / 'metrics-options.prom').write_text(text)
                    assert 'vtest_request{' in text and 'vtest_request_total' not in text, text
                    assert 'vtest_request_latency_bucket' in text and '_milliseconds' not in text and '_seconds' not in text
                    assert all(f'le="{boundary}"' in text for boundary in ('1', '5', '50'))
                    assert 'fixture="metrics"' in text and 'service_name=' not in text
                    process.stdin.write('\n'); process.stdin.flush(); wait_line(process, 'SECOND')
                    process.stdin.write('\n'); process.stdin.flush()
                    assert process.wait(timeout=10) == 0
                finally:
                    if process.poll() is None: process.kill(); process.wait(timeout=5)
            report['cases'].append('Prometheus custom prefix/tags, disabled counter/unit suffixes, millisecond durations, bucket boundaries and service-name suppression appeared in the actual scrape')
            for mode in ('cumulative', 'delta', 'exponential'):
                received = []
                ready = threading.Event()
                class Receiver(BaseHTTPRequestHandler):
                    def do_POST(self):
                        body = self.rfile.read(int(self.headers['Content-Length']))
                        received.append(body)
                        self.send_response(200); self.send_header('Content-Length', '0'); self.end_headers()
                        ready.set()
                    def log_message(self, *_): pass
                server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
                thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
                try:
                    with (ARTIFACTS / f'logs/metrics-options-{mode}.log').open('w') as log:
                        process = subprocess.Popen([str(BINARY), mode, 'http://' + fixture.address, f'http://127.0.0.1:{server.server_port}/v1/metrics'],
                                                   env=fixture.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True)
                        try:
                            wait_line(process, 'READY'); assert ready.wait(timeout=10)
                            observed = len(received)
                            process.stdin.write('\n'); process.stdin.flush(); wait_line(process, 'SECOND')
                            fixture.wait_for(lambda: len(received) > observed, timeout=10)
                            process.stdin.write('\n'); process.stdin.flush(); assert process.wait(timeout=10) == 0
                        finally:
                            if process.poll() is None: process.kill(); process.wait(timeout=5)
                    observations = [item for body in received for item in metrics(body)]
                    (ARTIFACTS / f'metrics-options-{mode}.json').write_text(json.dumps(observations, indent=2))
                    histograms = [item for item in observations if item['name'].startswith('vtest_request_latency')]
                    assert histograms and any(item['count'] > 0 for item in histograms), observations
                    assert all(item['temporality'] == (1 if mode == 'delta' else 2) for item in histograms), histograms
                    if mode == 'delta': assert sum(item['count'] for item in histograms) == 21, histograms
                    else: assert max(item['count'] for item in histograms) == 21, histograms
                    if mode == 'exponential':
                        assert all(item['kind'] == 10 and item['name'].endswith('_exp') and item['unit'] == 's' and item['scale'] <= 4 for item in histograms), histograms
                    else:
                        assert all(item['kind'] == 9 and item['unit'] == 'ms' and item['boundaries'] == [1, 5, 50] for item in histograms), histograms
                    report['cases'].append(f'OTLP {mode} exported the selected aggregation/temporality, duration units and histogram configuration in independently parsed protobuf messages')
                finally:
                    server.shutdown(); server.server_close(); thread.join(timeout=3)
        report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()

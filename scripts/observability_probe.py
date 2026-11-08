"""Scrape real Core metrics, inspect structured logs, and receive OTLP metrics."""

RESULT_REPORT = 'observability-results.json'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import urllib.request

from integration import TemporalFixture, ARTIFACTS, LOGS, counts
from smoke import free_port
from otlp_receiver import OtlpGrpcReceiver, metric_names


def scrape(port):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=5) as response:
        return response.read().decode()


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('observability') as fixture:
            fixture.env.update(TEMPORAL_LOG_FORMAT='json', TEMPORAL_CORE_LOG_FILTER='info')
            workflow_port, activity_port = free_port(), free_port()
            workflow_environment = dict(fixture.env, TEMPORAL_METRICS_ADDRESS=f'127.0.0.1:{workflow_port}')
            activity_environment = dict(fixture.env, TEMPORAL_METRICS_ADDRESS=f'127.0.0.1:{activity_port}')
            worker = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'workflow-before', env=workflow_environment)
            activity = fixture.launch(ARTIFACTS / 'tools/v-worker', 'activity', env=activity_environment)
            run = fixture.start('workflow', 'v.logged', {})
            fixture.wait_for(lambda: counts(fixture.history(run))['timerstarted'] == 1)
            workflow_metrics = scrape(workflow_port)
            activity_metrics = scrape(activity_port)
            assert 'temporal_workflow_task' in workflow_metrics and fixture.queue in workflow_metrics
            assert 'temporal_activity_execution' in activity_metrics and fixture.queue in activity_metrics
            (ARTIFACTS / 'workflow-metrics.prom').write_text(workflow_metrics)
            (ARTIFACTS / 'activity-metrics.prom').write_text(activity_metrics)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            replacement = dict(fixture.env, TEMPORAL_METRICS_ADDRESS=f'127.0.0.1:{free_port()}')
            worker = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'workflow-after', env=replacement)
            assert fixture.call('result', run)['status'] == 'completed'
            before = [json.loads(line) for line in (LOGS / 'observability-workflow-before.log').read_text().splitlines() if line.strip()]
            after = [json.loads(line) for line in (LOGS / 'observability-workflow-after.log').read_text().splitlines() if line.strip()]
            assert sum(item.get('message') == 'workflow user started' for item in before) == 1
            assert not any(item.get('message') == 'workflow user started' for item in after)
            completed = [item for item in after if item.get('message') == 'workflow user completed']
            assert completed and completed[0]['fields']['workflow_id'] == run['workflow_id']
            assert any(item.get('source') == 'temporal-core' for item in before), before[:2]
            report['cases'].append('Prometheus exported real workflow/Activity metrics with task-queue labels')
            report['cases'].append('SDK/Core logs were JSON; workflow log fields carried identity and replay suppressed duplicate user logs')

            received = []
            ready = threading.Event()
            class Receiver(BaseHTTPRequestHandler):
                def do_POST(self):
                    body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    received.append((self.headers.get('Content-Type'), body))
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/x-protobuf')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    ready.set()
                def log_message(self, *args):
                    pass
            server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
            receiver = threading.Thread(target=server.serve_forever, daemon=True)
            receiver.start()
            try:
                otel_environment = dict(fixture.env, TEMPORAL_TASK_QUEUE=fixture.queue + '-otel',
                                        TEMPORAL_OTEL_METRICS_URL=f'http://127.0.0.1:{server.server_port}/v1/metrics', TEMPORAL_OTEL_PROTOCOL='http_protobuf')
                otel = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'otel', env=otel_environment)
                original_queue = fixture.queue
                fixture.queue += '-otel'
                try:
                    otel_run = fixture.start('metrics', 'v.timer-cancel', {})
                    assert fixture.call('result', otel_run)['status'] == 'completed'
                finally:
                    fixture.queue = original_queue
                assert ready.wait(timeout=15), 'Core did not export OTLP metrics'
                assert any(content_type == 'application/x-protobuf' and body for content_type, body in received), received
                fixture.wait_for(lambda: any(any('workflow' in name for name in metric_names(body)) for _, body in received), timeout=15)
                otel.terminate()
                assert otel.wait(timeout=15) == 0
            finally:
                server.shutdown(); server.server_close(); receiver.join(timeout=3)
            report['cases'].append('Configured OTLP HTTP exporter delivered binary metrics to a real local receiver')
            with OtlpGrpcReceiver() as collector:
                grpc_environment = dict(fixture.env, TEMPORAL_TASK_QUEUE=fixture.queue + '-grpc',
                                        TEMPORAL_OTEL_METRICS_URL=f'http://127.0.0.1:{collector.port}', TEMPORAL_OTEL_PROTOCOL='grpc')
                grpc_worker = fixture.launch(ARTIFACTS / 'tools/v-workflow-worker', 'grpc', env=grpc_environment)
                original_queue = fixture.queue
                fixture.queue += '-grpc'
                try:
                    grpc_run = fixture.start('metrics', 'v.timer-cancel', {})
                    assert fixture.call('result', grpc_run)['status'] == 'completed'
                finally:
                    fixture.queue = original_queue
                fixture.wait_for(lambda: collector.errors or any(any('workflow' in name for name in names) for names, _ in collector.received), timeout=15)
                assert not collector.errors, collector.errors
                names, body = next((names, body) for names, body in collector.received if any('workflow' in name for name in names))
                (ARTIFACTS / 'otlp-grpc-export.bin').write_bytes(body)
                (ARTIFACTS / 'otlp-grpc-metric-names.json').write_text(json.dumps(names, indent=2))
                grpc_worker.terminate()
                assert grpc_worker.wait(timeout=15) == 0
            report['cases'].append('OTLP gRPC exporter completed real HTTP/2 unary exports; decoded request contained workflow metrics and receiver returned gRPC success trailers')
            worker.terminate(); activity.terminate()
            assert worker.wait(timeout=15) == 0 and activity.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

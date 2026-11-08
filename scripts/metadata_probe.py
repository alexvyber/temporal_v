"""Memo modification/search indexing, observed independently, with restart replay."""

RESULT_REPORT = 'metadata-results.json'
import base64
import json
import subprocess

from integration import TemporalFixture, ARTIFACTS, CLI, counts


def main():
    report = {'passed': False, 'cases': []}
    try:
        with TemporalFixture('metadata') as fixture:
            for name, kind in (('VSDKKeyword', 'Keyword'), ('VSDKInt', 'Int'), ('VSDKKeywords', 'KeywordList')):
                created = subprocess.run([str(CLI), 'operator', 'search-attribute', 'create', '--address', fixture.address,
                                          '--name', name, '--type', kind], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
                assert created.returncode == 0, created.stderr
            worker = fixture.workflows('before')
            handle = fixture.call('start', workflow_id=fixture.queue + '-workflow', type='v.metadata', input='{}', memo='{"origin":"initial"}', task_timeout_seconds=7, run_timeout_seconds=30)
            fixture.wait_for(lambda: counts(fixture.history(handle))['upsertworkflowsearchattributes'] == 1)
            worker.kill()
            assert worker.wait(timeout=5) == -9
            worker = fixture.workflows('after')
            assert fixture.call('result', handle)['data'] == '"metadata complete"'
            history = fixture.assert_clean_history(handle)
            assert history['workflowpropertiesmodified'] == 3, history
            described = subprocess.run([str(CLI), 'workflow', 'describe', '--address', fixture.address,
                                        '--workflow-id', handle['workflow_id'], '--run-id', handle['run_id'], '--output', 'json'],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
            assert described.returncode == 0, described.stderr
            (ARTIFACTS / 'metadata-description.json').write_text(described.stdout)
            info = json.loads(described.stdout)['workflowExecutionInfo']
            memo = info['memo']['fields']
            assert 'old' not in memo and memo['keep'] == 'updated', memo
            attributes = info['searchAttributes']['indexedFields']
            assert base64.b64decode(attributes['VSDKKeyword']['data']) == b'"ready"'
            assert base64.b64decode(attributes['VSDKInt']['data']) == b'42'
            report['cases'].append('Typed search attributes and memo upsert/deletion matched independent server description and replayed after SIGKILL')
            worker.terminate()
            assert worker.wait(timeout=15) == 0
            report['passed'] = True
    finally:
        (ARTIFACTS / RESULT_REPORT).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

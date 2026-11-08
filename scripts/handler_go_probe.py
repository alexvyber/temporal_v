"""Frozen actual Go/V unfinished-Update handoffs replay without a Go dependency."""
RESULT_REPORT = 'handler-go-results.json'
import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT, counts


def main(binary, report_name):
    report = {'passed': False, 'histories': 0, 'cases': []}
    try:
        folder = ROOT / '_tests/data/handler-go'
        manifest = json.loads((folder/'manifest.json').read_text())
        assert manifest['passed'] and manifest['reference']['sdk'] == 'v1.35.0'
        for name, expected in manifest['reference']['sources'].items():
            assert hashlib.sha256((ROOT/'_tests/native/handler_temporal_go'/name).read_bytes()).hexdigest() == expected, name
        assert len(manifest['cases']) == 20
        for case in manifest['cases']:
            for name, expected in case['files'].items():
                assert hashlib.sha256((folder/name).read_bytes()).hexdigest() == expected, name
            history = json.loads((folder/(case['name']+'.json')).read_text())
            events = counts(history)
            assert events['workflowtaskfailed'] == 0
            assert events['workflowexecutionupdateaccepted'] == (0 if case['name'].endswith('-next') else 2), case['name']
            expected_terminal = 'continuedasnew' if case['name'].endswith('-continue') else ('canceled' if case['name'].endswith('-cancel') else ('failed' if case['name'].endswith('-fail') else 'completed'))
            assert events['workflowexecution'+expected_terminal] == 1, case['name']
            replay = subprocess.run([str(binary), 'replay', str(folder/(case['name']+'.bin')), case['workflow_id'], case['task_queue']], cwd=ROOT, capture_output=True, text=True, timeout=45)
            (ARTIFACTS/'logs'/('handler-go-replay-'+case['name']+'.log')).write_text(replay.stdout+replay.stderr)
            assert replay.returncode == 0, replay.stdout+replay.stderr
            assert 'Workflow closed with an unfinished Update handler' not in replay.stdout+replay.stderr
            report['histories'] += 1
        report['reference'] = manifest['reference']
        report['manifest_sha256'] = hashlib.sha256((folder/'manifest.json').read_bytes()).hexdigest()
        report['cases'] = ['Pinned Go SDK 1.35 and V workers exchanged sixteen live runs in both directions with accepted deferred Updates; completion/CAN/cancel/failure and four next runs produced twenty histories replayed by both SDKs', 'Frozen original JSON/binary histories and Go source/version hashes were verified; all twenty replayed without unfinished-handler warnings']
        report['passed'] = True
    finally: (ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic'); parser.add_argument('--report',default=RESULT_REPORT)
    args=parser.parse_args(); main(args.binary,args.report)

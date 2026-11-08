"""Cardinality-changing payload codecs on actual Temporal and replay."""
RESULT_REPORT = 'codec-counts-results.json'
import argparse
import json
import os
import subprocess
from pathlib import Path
from integration import ARTIFACTS, ROOT
import typed_details_probe
import typed_nexus_probe


def main(binary=ARTIFACTS / 'tools/v-typed-values', report_name=RESULT_REPORT):
    report = {'passed': False, 'cases': []}
    try:
        validation = subprocess.run([str(binary), 'counts-validation'], cwd=ROOT,
                                    capture_output=True, text=True, timeout=40)
        (ARTIFACTS / f'logs/{report_name}-validation.log').write_text(validation.stdout + validation.stderr)
        assert validation.returncode == 0 and validation.stdout.strip() == 'CODEC_COUNTS_VALIDATION_OK', validation.stderr
        os.environ['V_SDK_DETAILS_PACK'] = 'true'
        typed_details_probe.BINARY = binary
        typed_nexus_probe.BINARY = binary
        report['detail_histories'] = typed_details_probe.exercise()
        report['nexus_histories'] = typed_nexus_probe.exercise()
        assert report['detail_histories'] == 112 and report['nexus_histories'] == 98
        report['cases'].append('Empty/single/seven-argument pack/split/pack conversion; malformed/truncated/reordered envelopes and zero/two-valued singular codecs rejected without indexing')
        report['cases'].append('Both servers and seven converter profiles: seven arguments packed to one wire payload, nested failure rebinding, checkpoint retry, deferred completion, query/Update interceptors, cancellation, typed Nexus inputs/results, SIGKILL/replacement and 210 offline histories; 49 standalone Nexus operations on the supporting server')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=ARTIFACTS / 'tools/v-typed-values')
    parser.add_argument('--report', choices=(RESULT_REPORT, 'codec-counts-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)

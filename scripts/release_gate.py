"""Fail closed until the full-parity release requirements have been verified.

This is a readiness audit, not a substitute for tests. A verified requirement must
link to committed verification instructions and a successful evidence report.
"""
import json
from pathlib import Path
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    manifest = json.loads((root / 'release/readiness.json').read_text())
    failures = []
    requirements = manifest['requirements']
    identifiers = [item['id'] for item in requirements]
    if len(set(identifiers)) != len(identifiers) or not requirements:
        failures.append('requirements must have unique, nonempty IDs')
    if manifest['targets'] != ['linux/amd64', 'linux/arm64']:
        failures.append('both requested Linux container architectures are required')
    for item in requirements:
        if item['status'] != 'verified':
            failures.append(f"{item['id']}: {item['status']} — {item['detail']}")
            continue
        for key in ('verification', 'evidence'):
            path = Path(item.get(key, ''))
            if not item.get(key) or path.is_absolute() or '..' in path.parts or not (root / path).is_file():
                failures.append(f"{item['id']}: missing repository {key} file")
        evidence_path = root / item.get('evidence', '')
        if evidence_path.is_file():
            evidence = json.loads(evidence_path.read_text())
            if evidence.get('passed') is not True:
                failures.append(f"{item['id']}: evidence did not pass")
    if manifest.get('release_ready') is not True:
        failures.append('release_ready is false')
    if failures:
        print('RELEASE BLOCKED', file=sys.stderr)
        for failure in failures:
            print(f'- {failure}', file=sys.stderr)
        return 1
    print('Full-parity readiness audit passed; run platform release verification before publishing.')
    return 0


if __name__ == '__main__':
    sys.exit(main())

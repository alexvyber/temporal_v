"""One formatter for handwritten V, generated V, and read-only format checks."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile

from prepare_formatter import location, verify


def format_files(files, check=False, artifacts=None, **options):
    verify(artifacts)
    formatter = location(artifacts) / 'cmd/tools/vfmt'
    environment = dict(os.environ, VFLAGS='', VEXE=str(location(artifacts) / 'v'))
    options.setdefault('timeout', 180)
    paths = list(map(str, files))
    command = [str(formatter), 'fmt', '-verify' if check else '-w', *paths]
    try:
        if check:
            return subprocess.run(command, env=environment, **options)
        # A raw generated line can require a second layout pass in V 0.5.2.
        # Require convergence instead of leaving output that fails verification.
        for attempt in range(3):
            written = subprocess.run(command, env=environment, **options)
            if written.returncode:
                return written
            verified = subprocess.run([str(formatter), 'fmt', '-verify', *paths],
                                      env=environment, capture_output=True, text=True, timeout=options['timeout'])
            if verified.returncode == 0:
                return written
        raise RuntimeError('V formatting did not converge: ' + verified.stdout + verified.stderr)
    finally:
        verify(artifacts)


def formatted_source(content, filename):
    """Render generated text to a verified formatter fixed point off-tree."""
    with tempfile.TemporaryDirectory(prefix='v-format-') as temporary:
        rendered = Path(temporary) / filename
        rendered.write_text(content)
        format_files([rendered], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE).check_returncode()
        return rendered.read_text()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('files', nargs='+', type=Path)
    args = parser.parse_args()
    result = format_files(args.files, check=args.check)
    raise SystemExit(result.returncode)

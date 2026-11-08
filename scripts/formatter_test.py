"""Targeted formatter/generator regression tests; no Temporal server or SDK build."""
import hashlib
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from build_payload_bridge import generate_targets
from format_v import format_files
from generate_activation_decoder import regenerate
from generate_service import generate
from generate_protobuf import generate as generate_protobuf
from prepare_formatter import ROOT, location, verify


class FormatterTests(unittest.TestCase):
    def test_verify_is_read_only_and_write_converges(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'example.v'
            content = b'module example\nfn answer() int {return 42}\n'
            source.write_bytes(content)
            result = format_files([source], check=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(source.read_bytes(), content)
            format_files([source], capture_output=True).check_returncode()
            formatted = source.read_bytes()
            format_files([source], capture_output=True).check_returncode()
            self.assertEqual(source.read_bytes(), formatted)
            format_files([source], check=True, capture_output=True).check_returncode()

    def test_tampered_formatter_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = location()
            installed = location(temporary)
            installed.mkdir(parents=True)
            # Receipt verification rejects the damaged input before invocation.
            receipt = verify()
            name = next(iter(receipt['sources']))
            target = installed / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('changed')
            shutil.copy2(source / '.formatter-receipt.json', installed)
            with self.assertRaisesRegex(RuntimeError, 'input/image changed'):
                format_files([], artifacts=temporary, check=True)

    def test_real_schemas_regenerate_exactly_and_detect_drift(self):
        rust = Path(os.environ.get('SPIKE_ARTIFACTS', ROOT.parent / 'artifacts/v-sdk-spike')) / 'tools/sdk-rust'
        before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in ROOT.rglob('*.v')}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'sdk').mkdir()
            (root / 'temporal').mkdir()
            shutil.copy2(ROOT / 'temporal/core_pb.v', root / 'temporal/core_pb.v')
            shutil.copytree(ROOT / 'schemas', root / 'schemas')
            generate_protobuf(root, rust.parent.parent, check=True)
            generate(root, rust)
            regenerate(root)
            generate_targets(root, rust)
            for path in (root / 'sdk').glob('*.v'):
                self.assertEqual(path.read_bytes(), (ROOT / 'sdk' / path.name).read_bytes(), path.name)
            generate(root, rust, check=True)
            regenerate(root, check=True)
            generate_targets(root, rust, check=True)
            # Change real generated output: check must fail and preserve the change.
            for filename, check in (
                ('sdk/workflow_service.v', lambda: generate(root, rust, check=True)),
                ('sdk/activation_payloads.v', lambda: regenerate(root, check=True)),
                ('sdk/storage_targets.v', lambda: generate_targets(root, rust, check=True)),
                ('temporal/core_pb.v', lambda: generate_protobuf(root, rust.parent.parent, check=True)),
            ):
                destination = root / filename
                content = destination.read_bytes() + b'// drift\n'
                destination.write_bytes(content)
                with self.assertRaises(RuntimeError):
                    check()
                self.assertEqual(destination.read_bytes(), content)
        self.assertEqual(before, {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before})


if __name__ == '__main__':
    unittest.main()

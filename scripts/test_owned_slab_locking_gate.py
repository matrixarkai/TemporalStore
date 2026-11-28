# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Output-adapter checks only: these tests never start Cargo or product code."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('owned_slab_locking_gate', HERE / 'owned_slab_locking_gate.py')
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)
SELECTOR = 'block_store::owned_writer::tests::interrupted_reservation_names_are_never_reused_after_reopen'
GOOD = f'\nrunning 1 test\ntest {SELECTOR} ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 999 filtered out; finished in 0.01s\n'


class GateOutputTests(unittest.TestCase):
    def test_one_exact_positive_result(self):
        self.assertIn('1 passed', GATE.require_one_pass(SELECTOR, GOOD, 0))

    def test_rejects_empty_filter(self):
        with self.assertRaises(ValueError): GATE.require_one_pass(SELECTOR, GOOD.replace('running 1 test', 'running 0 tests').replace('1 passed', '0 passed'), 0)

    def test_rejects_wrong_name(self):
        with self.assertRaises(ValueError): GATE.require_one_pass(SELECTOR, GOOD.replace(SELECTOR, 'block_store::tests::other'), 0)

    def test_rejects_ignored_and_nonpositive(self):
        for output in [GOOD.replace('... ok', '... ignored'), GOOD.replace('0 ignored', '1 ignored'), GOOD.replace('1 passed', '0 passed')]:
            with self.subTest(output=output), self.assertRaises(ValueError): GATE.require_one_pass(SELECTOR, output, 0)

    def test_rejects_multiple_harnesses(self):
        with self.assertRaises(ValueError): GATE.require_one_pass(SELECTOR, GOOD + GOOD, 0)

    def test_rejects_missing_or_failed_summary(self):
        for output in [GOOD.split('test result:')[0], GOOD.replace('result: ok', 'result: FAILED')]:
            with self.subTest(output=output), self.assertRaises(ValueError): GATE.require_one_pass(SELECTOR, output, 0)

    def test_rejects_nonzero_or_signal_exit(self):
        for code in [1, 101, -9, -15]:
            with self.subTest(code=code), self.assertRaises(ValueError): GATE.require_one_pass(SELECTOR, GOOD, code)

    def test_rejects_cancelled_output(self):
        for marker in ['Operation canceled', 'cancelled', 'interrupted', 'signal: SIGTERM']:
            with self.subTest(marker=marker), self.assertRaises(ValueError): GATE.require_one_pass(SELECTOR, GOOD + marker, 0)

    def test_default_features_release_exact_command(self):
        self.assertEqual(GATE.command(SELECTOR), ['cargo', 'test', '--locked', '--release', '-p', 'temporalstore-rust', '--lib', SELECTOR, '--', '--exact', '--test-threads=1'])

    def test_all_manifest_controls_have_fixed_denominators(self):
        data = json.loads((HERE / 'owned_slab_locking_controls.json').read_text())
        self.assertEqual(len(GATE.controls_from(data)), 100)

    def test_rejects_duplicate_or_removed_controls(self):
        data = json.loads((HERE / 'owned_slab_locking_controls.json').read_text())
        for mutation in ['duplicate', 'new_removed', 'existing_removed']:
            changed = copy.deepcopy(data)
            if mutation == 'duplicate': changed['controls'][-1] = changed['controls'][0]
            elif mutation == 'new_removed': changed['controls'].pop(0)
            else:
                changed['controls'].pop(); changed['affected_existing_count'] -= 1
            with self.subTest(mutation=mutation), self.assertRaises(ValueError): GATE.controls_from(changed)


if __name__ == '__main__': unittest.main()

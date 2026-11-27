# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Real hosted controller faults; never launch a product library or compiler."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import ci
import owned_children


class HostedControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _, _, _, cls.consumer, _ = ci.hosted_context()
        owned_children.enable()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="checkpoint-controls-", dir=os.environ["RUNNER_TEMP"])
        self.root = Path(self.temporary.name)
        self.env = ci.clean_env()
        self.env["CARGO_TARGET_DIR"] = os.environ["CARGO_TARGET_DIR"]

    def tearDown(self):
        self.assertEqual(owned_children.children(), [], "controller left owned processes behind")
        self.temporary.cleanup()

    def test_detached_child_after_launcher_exit_is_reaped(self):
        program = ("import subprocess,sys; subprocess.Popen([sys.executable,'-c',"
                   "'import time;time.sleep(120)'],start_new_session=True)")
        with self.assertRaisesRegex(RuntimeError, "surviving descendants"):
            ci.run([sys.executable, "-c", program], self.root / "orphan.log", self.env, 10)

    def test_timeout_reaps_launcher_and_its_child(self):
        witness = self.root / "child-ready.json"
        child = ("import json,os,pathlib,time;"
                 "pathlib.Path(" + repr(str(witness)) + ").write_text(json.dumps({'pid':os.getpid()}));"
                 "time.sleep(120)")
        program = ("import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',"
                   + repr(child) + "]);time.sleep(120)")
        with self.assertRaisesRegex(RuntimeError, "bounded duration"):
            ci.run([sys.executable, "-c", program], self.root / "timeout.log", self.env, 5)
        ready = json.loads(witness.read_text())
        self.assertEqual(set(ready), {"pid"})
        self.assertIs(type(ready["pid"]), int)
        self.assertGreater(ready["pid"], 0)

    def test_fast_output_overflow_is_refused(self):
        program = "import sys;sys.stdout.buffer.write(b'x'*(33<<20));sys.stdout.buffer.flush()"
        with self.assertRaisesRegex(RuntimeError, "output exceeded"):
            ci.run([sys.executable, "-c", program], self.root / "overflow.log", self.env, 10)

    def test_staged_source_change_cannot_match_clean_head(self):
        repository = self.root / "repo"
        subprocess.run(["git", "init", "--quiet", str(repository)], env=self.env, check=True)
        source = repository / "source.rs"
        source.write_text("const VALUE: u8 = 1;\n")
        (repository / ".gitignore").write_text("target/\nignored-source.rs\n")
        subprocess.run(["git", "-C", str(repository), "add", "source.rs", ".gitignore"], env=self.env, check=True)
        subprocess.run(["git", "-C", str(repository), "-c", "user.name=CI fixture",
                        "-c", "user.email=ci@example.invalid", "commit", "--quiet", "-m", "Fixture"],
                       env=self.env, check=True)
        ci.clean_checkout(repository)
        nested = repository / "target/checkpoint-producer"
        nested.mkdir(parents=True)
        subprocess.run(["git", "init", "--quiet", str(nested)], env=self.env, check=True)
        (nested / "old-source.rs").write_text("const OLD: u8 = 1;\n")
        ci.clean_checkout(repository, workspace_target=True)
        with self.assertRaises(RuntimeError):
            ci.clean_checkout(repository)
        for name in ("ignored-source.rs", "target-evil.rs"):
            unexpected = repository / name
            unexpected.write_text("const UNEXPECTED: u8 = 1;\n")
            with self.assertRaises(RuntimeError):
                ci.clean_checkout(repository, workspace_target=True)
            unexpected.unlink()
        source.write_text("const VALUE: u8 = 2;\n")
        subprocess.run(["git", "-C", str(repository), "add", "source.rs"], env=self.env, check=True)
        with self.assertRaises(subprocess.CalledProcessError):
            ci.clean_checkout(repository)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(HostedControls)
    if suite.countTestCases() != 4:
        raise RuntimeError("expected four real hosted controller checks")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful() or result.testsRun != 4 or result.skipped:
        raise SystemExit(1)
    print(json.dumps({"consumer": HostedControls.consumer, "controller_checks_passed": 4,
                      "product_tests_executed": False}))

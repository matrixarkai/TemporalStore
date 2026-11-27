# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Controller-only checks; no Cargo, product process or local admission."""
import copy
import json
import os
from pathlib import Path
import re
import unittest
from unittest.mock import patch

import ci
import owned_children


class SourceChecks(unittest.TestCase):
    def source(self, revision=ci.OLD):
        return f"git+{ci.REPO}?rev={revision}#{revision}"

    def metadata(self):
        return {"packages": [{"name": "temporalstore-rust", "id": "lib", "source": self.source()}],
                "resolve": {"root": "bridge", "nodes": [{"id": "bridge", "deps": [
                    {"name": "temporalstore", "pkg": "lib"}]}]}}

    def test_exact_git_source(self):
        ci.git_source(self.source(), ci.OLD)
        ci.git_source(self.source().replace("TemporalStore", "temporalstore"), ci.OLD)

    def test_non_git_or_wrong_identity_refused(self):
        cases = [None, "path+file:///tmp/product", self.source().replace("github.com", "example.org"),
                 self.source().replace("?rev=", "?branch="), self.source() + "extra",
                 self.source().replace("TemporalStore", "OtherStore")]
        for source in cases:
            with self.subTest(source=source), self.assertRaises(ValueError):
                ci.git_source(source, ci.OLD)

    def test_graph_selects_actual_product_edge(self):
        self.assertEqual(ci.product_package(self.metadata(), ci.OLD)["id"], "lib")

    def test_wrong_edge_and_duplicate_product_refused(self):
        wrong = self.metadata()
        wrong["resolve"]["nodes"][0]["deps"][0]["pkg"] = "different"
        duplicate = copy.deepcopy(self.metadata())
        duplicate["packages"].append(copy.deepcopy(duplicate["packages"][0]))
        for metadata in (wrong, duplicate):
            with self.assertRaises(ValueError):
                ci.product_package(metadata, ci.OLD)

    def test_local_context_refuses_before_any_command(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(ci.subprocess, "check_output") as call:
            with self.assertRaises(RuntimeError):
                ci.hosted_context()
            call.assert_not_called()

    def test_clean_environment_drops_recovery_and_compiler_overrides(self):
        original = {"PATH": "/usr/bin", "TS_DISABLE_INDEX": "1", "MATRIXARK_BULK_INGEST": "1",
                    "RUSTC": "other", "RUSTFLAGS": "flags", "RUSTC_BOOTSTRAP": "1",
                    "GIT_OBJECT_DIRECTORY": "/other", "CARGO_PROFILE_RELEASE_OPT_LEVEL": "0"}
        with patch.dict(os.environ, original, clear=True):
            actual = ci.clean_env()
        for key in original.keys() - {"PATH"}:
            self.assertNotIn(key, actual)
        self.assertEqual(actual["PATH"], "/usr/bin")
        self.assertEqual(actual["GIT_TERMINAL_PROMPT"], "0")
        self.assertEqual(actual["CARGO_BUILD_JOBS"], "1")

    def test_format_source_contract(self):
        declaration = "pub(super) const SHARD_INDEX_FORMAT_VERSION: u32 = {};"
        self.assertEqual(ci.declared_format(declaration.format(32), "producer"), 32)
        self.assertEqual(ci.declared_format(declaration.format(34), "consumer"), 34)
        for body, role in ((declaration.format(33), "producer"), (declaration.format(32), "consumer"),
                           ("", "consumer"), (declaration.format(34) * 2, "consumer")):
            with self.assertRaises(ValueError):
                ci.declared_format(body, role)

    def test_result_parser_requires_one_exact_structured_result(self):
        row = {"passed": True, "phase": "reopen"}
        text = "warning: diagnostic\n" + json.dumps(row) + "\n"
        self.assertEqual(ci.one_json(text, row.keys()), row)
        for text in ("", json.dumps({"passed": True}), json.dumps(row) + "\n" + json.dumps(row)):
            with self.assertRaises(ValueError):
                ci.one_json(text, row.keys())

    def test_bridge_revision_and_format_are_build_bound(self):
        root = Path(ci.__file__).resolve().parent
        common = (root / "common.rs").read_text()
        consumer = (root / "consumer/main.rs").read_text()
        self.assertIn('const NEW: &str = env!("UPGRADE_CONSUMER_COMMIT");', common)
        self.assertIn('const LIB_COMMIT: &str = env!("UPGRADE_CONSUMER_COMMIT");', consumer)
        self.assertIn('env!("UPGRADE_TARGET_FORMAT")', common)
        self.assertEqual((root / "consumer/Cargo.toml").read_text().count("CONSUMER_REVISION"), 1)

    def test_checkout_checks_staged_and_untracked_sources(self):
        with patch.object(ci.subprocess, "run") as diff, patch.object(ci.subprocess, "check_output", return_value=b""):
            ci.clean_checkout(Path("/product"))
        self.assertIn("HEAD", diff.call_args.args[0])
        for status in (b"A  src/injected.rs\0", b"?? src/injected.rs\0", b"!! src/hidden.rs\0"):
            with patch.object(ci.subprocess, "run"), patch.object(ci.subprocess, "check_output", return_value=status):
                with self.assertRaises(RuntimeError):
                    ci.clean_checkout(Path("/product"))

    def test_only_workspace_target_is_excluded_from_source_census(self):
        with patch.object(ci.subprocess, "run"), patch.object(ci.subprocess, "check_output", return_value=b"") as status:
            ci.clean_checkout(Path("/workspace"), workspace_target=True)
        self.assertEqual(status.call_args.args[0][-3:], ["--", ".", ":(top,literal,exclude)target"])

    def test_child_signal_checks_identity_and_parent(self):
        child = owned_children.Child(7, 100, 44, "S")
        for current in (child, owned_children.Child(7, 100, 45, "S"),
                        owned_children.Child(7, 101, 44, "S")):
            with patch.object(owned_children.os, "getpid", return_value=100), \
                    patch.object(owned_children.os, "pidfd_open", return_value=99), \
                    patch.object(owned_children.os, "close") as close, \
                    patch.object(owned_children, "read_child", return_value=current), \
                    patch.object(owned_children.signal, "pidfd_send_signal") as send:
                owned_children.send(child, owned_children.signal.SIGTERM)
                self.assertEqual(send.call_count, int(current == child))
                close.assert_called_once_with(99)

    def test_reaping_never_steals_the_leader_wait_status(self):
        rows = [owned_children.Child(7, 100, 44, "Z"), owned_children.Child(8, 100, 45, "Z")]
        with patch.object(owned_children.os, "waitpid") as wait:
            owned_children.reap(rows, 7)
        wait.assert_called_once_with(8, os.WNOHANG)

    def test_fast_exit_cannot_bypass_output_bound(self):
        from tempfile import TemporaryDirectory
        from unittest.mock import Mock
        with TemporaryDirectory() as temporary:
            log = Path(temporary) / "run.log"
            process = Mock(pid=7)
            process.poll.return_value = 0
            process.wait.return_value = 0
            with patch.object(ci.subprocess, "Popen", return_value=process), \
                    patch.object(owned_children, "children", return_value=[]), \
                    patch.object(owned_children, "terminate") as terminate, \
                    patch.object(ci.shutil, "disk_usage") as disk:
                disk.return_value.free = 1 << 30
                with patch.object(ci.Path, "stat") as stat:
                    stat.return_value.st_size = 33 << 20
                    with self.assertRaises(RuntimeError):
                        ci.run(["not-started"], log, {"CARGO_TARGET_DIR": temporary}, 5)
                terminate.assert_called_once_with(process)

    def test_surviving_adopted_child_fails_the_phase(self):
        from tempfile import TemporaryDirectory
        from unittest.mock import Mock
        with TemporaryDirectory() as temporary:
            process = Mock(pid=7)
            process.poll.return_value = 0
            process.wait.return_value = 0
            orphan = owned_children.Child(8, 100, 45, "S")
            with patch.object(ci.subprocess, "Popen", return_value=process), \
                    patch.object(owned_children, "children", side_effect=[[], [orphan]]), \
                    patch.object(owned_children, "terminate") as terminate, \
                    patch.object(ci.shutil, "disk_usage") as disk:
                disk.return_value.free = 1 << 30
                with self.assertRaisesRegex(RuntimeError, "surviving descendants"):
                    ci.run(["not-started"], Path(temporary) / "run.log", {"CARGO_TARGET_DIR": temporary}, 5)
                terminate.assert_called_once_with(process)

    def test_physical_oracle_core_is_unchanged(self):
        body = (Path(ci.__file__).resolve().parent / "common.rs").read_text()
        matches = list(re.finditer(r"^fn ([a-z_]+)\(", body, re.M))
        functions = {match.group(1): body[match.start():matches[index + 1].start()
                     if index + 1 < len(matches) else len(body)]
                     for index, match in enumerate(matches)}
        expected = {
            "header": "3d8d747c7f548f1a4aaffd75e4e99d1948ff8964229aaa6de71d48fc48aae694",
            "verify": "714b896aa8195bc35bd53e8530bee6d5c97cd1452d03a2442a650569a1261d0b",
            "scan": "b3fb02a32114540ba2f471151842d70264b0355fa57a7d2010223bec7a2b4b77",
            "checkpoint": "3032c9fbaf2cbd99ffd114b25d3e464c726029a8e8076efe6be8bbcd4e69cdc1",
            "reclaim": "919bc786f874a3136080a8824b605d8dff3b9997f2e7206712d2ba0db7689b25",
            "produce": "803374d915d0f49e5e55788384fc86c54e9146c6c9c92d212599cf2111ca24fe",
            "producer_receipt": "d3d77a9269ded6ac44ee96bd7ff97952f8f1a00dd41de5643ee4f6d1ccb7279d",
            "negative": "961098509ad8e6b4c6a483149c360e43cc939c82fac6424d58473862541e9cdc",
        }
        self.assertEqual({name: ci.sha(functions[name].encode()) for name in expected}, expected)


if __name__ == "__main__":
    unittest.main()

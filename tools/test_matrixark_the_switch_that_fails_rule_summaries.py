#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The switch that fails rule summaries is not read by the summariser this build runs.

`ingestion.require_model_summaries` is offered on the setup page beside the encoder's
`Fail instead of falling back`, labelled `Fail instead of writing rule summaries`, and its help
said:

    On, a summary that cannot reach the model errors instead of silently falling back to the
    local rule summariser.

**No Python module reads the variable.** `MATRIXARK_REQUIRE_MODEL_SUMMARIES` is checked in the
engine, in `context_workflow/model_provider.rs`, which refuses the extraction with
`model_required_but_provider_is_mock`. The summariser on the local-adapter path never asks, so
where that path writes the summaries the switch changes nothing.

It matters because the state it exists to prevent is a documented default. `summary.provider`'s own
help says an Anthropic extraction provider returns rule-written summaries and no error -- so a
deployment on Anthropic that turns this on to stop exactly that gets rule summaries anyway, and a
settings page saying the opposite.

Both halves are derived here rather than asserted: the Python tree is scanned for a reader, and the
engine source for the read that makes `enforced_by` true. Either changing fails this, and the
wording has to change with it.
"""
from __future__ import annotations

import ast
import io
import json
import os
import re
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

VARIABLE = "MATRIXARK_REQUIRE_MODEL_SUMMARIES"

#: Files allowed to name the variable without reading it, and WHY -- an exclusion that does not say
#: what it hides is how a scan comes back empty and means nothing.
NAMED_BUT_NOT_READ = {
    "matrixark_gateway_config.py": "declares the setting; naming its own variable is the point",
    "matrixark_load_config.py": "maps a config-file key to the variable; a name in a table",
    "matrixark_v1_gateway.py": "REPORTS it on the model status surface. It reads the value and "
                               "acts on nothing: this file is why the switch is visible, not why "
                               "it is enforced -- and adding it here is the change feeding the "
                               "guard its own list, which is why the summariser check below is "
                               "the load-bearing one.",
}

#: The modules that WRITE the summaries. The claim is about these, and none of them may name it.
SUMMARY_WRITERS = ("matrixark_mcp_summaries.py", "matrixark_local_adapter_summaries.py",
                   "matrixark_mcp_oss_understanding.py")


def snapshot(extraction: str, summary, require: str) -> dict:
    """The snapshot a deployment with these three would show, read from a child process.

    `summary=None` is the variable NOT SET, which follows the extraction provider; `summary=""` is
    set to nothing, which does not. Flattening them tests a deployment nobody has -- the sibling
    suite made that mistake first and says so.
    """
    script = ("import json, matrixark_v1_gateway as gw;"
              "print(json.dumps(gw._model_config_snapshot()))")
    environ = dict(os.environ)
    environ["MATRIXARK_UNDERSTANDING_PROVIDER"] = extraction
    if summary is None:
        environ.pop("MATRIXARK_SUMMARY_PROVIDER", None)
    else:
        environ["MATRIXARK_SUMMARY_PROVIDER"] = summary
    environ[VARIABLE] = require
    environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/matrixark-require-summaries-test.json"
    out = subprocess.run([sys.executable, "-c", script], cwd=TOOLS, env=environ,
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    return json.loads(out.stdout)


def warned(snap: dict) -> bool:
    return any("Fail instead of writing rule summaries is on" in w
               for w in snap.get("warnings", []))


class NothingInPythonReadsItTest(unittest.TestCase):
    """The claim `require_model_enforced_by` makes, checked against the tree."""

    def _mentions(self) -> dict:
        found = {}
        for name in sorted(os.listdir(TOOLS)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            with io.open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
                if VARIABLE in handle.read():
                    found[name] = True
        return found

    def test_only_the_registry_and_the_name_map_mention_it(self) -> None:
        self.assertEqual(sorted(NAMED_BUT_NOT_READ), sorted(self._mentions()),
                         "a Python module now names %s; if it READS it, "
                         "require_model_enforced_by and the setting's help are both wrong" % VARIABLE)

    #: Of the three, the two that only NAME it. The gateway is deliberately not here: it reads
    #: the value to put it on the status surface, which is the whole point of the change, and a
    #: test that let that count as "nothing reads it" would be reading its own allow-list.
    DECLARERS = ("matrixark_gateway_config.py", "matrixark_load_config.py")

    def test_the_two_declarers_do_not_dereference_it(self) -> None:
        """Naming is not reading. This is the half the exclusion above would otherwise hide."""
        for name in self.DECLARERS:
            with self.subTest(module=name):
                with io.open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
                    self.assertIsNone(self._reader().search(handle.read()),
                                      "%s reads it after all" % name)

    def test_the_gateway_reads_it_and_only_reports_it(self) -> None:
        """The third file, stated rather than excluded: it reads the value, and the only thing it
        does with it is put it in the snapshot and raise a warning. Nothing here writes a summary,
        so reading it here does not make the switch enforced."""
        with io.open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            text = handle.read()
        self.assertIsNotNone(self._reader().search(text))
        self.assertIn('"require_model": _require_model_summaries', text)

    @staticmethod
    def _reader():
        return re.compile(r"(?:getenv|environ\.get|environ\[|_env|env_bool|_truthy_env)\s*\(?\s*"
                          r"[\"']%s[\"']" % VARIABLE)

    def test_no_module_that_writes_a_summary_names_it(self) -> None:
        """The load-bearing claim, and the one an allow-list cannot soften: the switch says a
        summary that falls back to rules will error, and the code that writes that summary has
        never heard of it."""
        for name in SUMMARY_WRITERS:
            path = os.path.join(TOOLS, name)
            if not os.path.exists(path):
                continue
            with io.open(path, encoding="utf-8") as handle:
                self.assertNotIn(VARIABLE, handle.read(),
                                 "%s names it; if it acts on it the help is now right and this "
                                 "test is what is wrong" % name)

    def test_the_summary_writers_are_really_there(self) -> None:
        """The floor for the test above: an empty loop asserts nothing."""
        present = [n for n in SUMMARY_WRITERS if os.path.exists(os.path.join(TOOLS, n))]
        self.assertGreaterEqual(len(present), 2, present)

    def test_the_scan_would_notice_a_reader(self) -> None:
        """The floor. The sibling variable IS read in Python, so the same scan finds more than
        two files for it -- which proves the scan is not simply returning the allow-list."""
        sibling = "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS"
        hits = []
        for name in sorted(os.listdir(TOOLS)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            with io.open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
                if sibling in handle.read():
                    hits.append(name)
        self.assertGreater(len(hits), len(NAMED_BUT_NOT_READ), hits)

    def test_the_engine_is_where_it_is_read(self) -> None:
        """The other half of the claim: `enforced_by: engine` has to be true of the engine."""
        source = os.path.join(REPO, "crates", "temporalstore-rust", "src",
                              "context_workflow", "model_provider.rs")
        if not os.path.exists(source):
            self.skipTest("the engine source is not in this checkout")
        with io.open(source, encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn('std::env::var("%s")' % VARIABLE, text,
                      "the engine no longer reads it, so nothing enforces the switch anywhere")


class TheHelpSaysWhereItIsEnforcedTest(unittest.TestCase):
    """The help is what a reader acts on, and it was the thing that was wrong.

    It said the switch makes a summary that cannot reach the model error. On the path the local
    adapter serves, it does not: the summariser never reads the variable.
    """

    @staticmethod
    def _help(key: str) -> str:
        import matrixark_gateway_config as cfg
        return next(s for s in cfg.SETTINGS if s.key == key).help

    def test_it_names_the_engine(self) -> None:
        text = self._help("ingestion.require_model_summaries").lower()
        self.assertIn("engine", text,
                      "the help promises an error without saying who enforces it")

    def test_it_says_this_build_does_not_read_it(self) -> None:
        text = self._help("ingestion.require_model_summaries").lower()
        self.assertTrue("nothing in this python build reads it" in text
                        or "changes nothing" in text, text)

    def test_the_encoder_switch_needs_no_such_qualifier(self) -> None:
        """The floor. If every help mentioned an engine the tests above would pass on noise --
        and the encoder's switch IS read in Python, so it must not carry the same caveat."""
        self.assertNotIn("engine", self._help("embedding.require_model_embeddings").lower())


class TheBlockCarriesItTest(unittest.TestCase):

    def test_the_summary_block_says_both(self) -> None:
        block = snapshot("openai_compatible", None, "0")["summary"]
        self.assertIn("require_model", block)
        self.assertEqual("engine", block["require_model_enforced_by"])

    def test_it_reports_the_switch_as_set(self) -> None:
        self.assertTrue(snapshot("openai_compatible", None, "1")["summary"]["require_model"])
        self.assertFalse(snapshot("openai_compatible", None, "0")["summary"]["require_model"])

    def test_the_encoder_block_still_carries_its_own(self) -> None:
        """The switch this one is offered beside. If it vanished, the pair would be misleading in
        the other direction."""
        self.assertIn("require_model_embeddings", snapshot("openai_compatible", None, "0")["embedding"])


class TheWarningFiresOnTheStateItWasSetToPreventTest(unittest.TestCase):

    def test_on_and_writing_rules_is_reported(self) -> None:
        """Anthropic extraction with the switch on: the documented case, and the reason for this."""
        self.assertTrue(warned(snapshot("anthropic", None, "1")))

    def test_on_and_writing_with_a_model_is_not(self) -> None:
        self.assertFalse(warned(snapshot("openai_compatible", None, "1")))

    def test_off_and_writing_rules_is_not(self) -> None:
        """Off, rule summaries are a choice, and the summary warning already names them."""
        self.assertFalse(warned(snapshot("anthropic", None, "0")))

    def test_naming_a_model_provider_clears_it(self) -> None:
        """The remedy the warning offers has to work, or it is worse than silence."""
        self.assertFalse(warned(snapshot("anthropic", "openai_compatible", "1")))


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal offers the third model: the one that writes summaries.

A deployment runs three models. The portal offered two -- extraction and embedding -- and the
summary model was reachable only by setting ``MATRIXARK_SUMMARY_*`` by hand. It is the model called
most: every context node gets a summary, where extraction runs once per ingest.

It needs no endpoint or key of its own. ``openai_compatible_json_call`` sends a summary to
``EXTRACTION_LLM_BASE_URL`` with the key named by ``MATRIXARK_EXTRACTION_API_KEY_ENV`` and takes only
the model and the budget as arguments, so three controls complete the surface and they belong in the
extraction group beside the endpoint and key they use.

**Adding a control must not change what an untouched deployment does.** The two string defaults are
blank, and a blank is not seeded into the environment, so the reader's chain
(``SUMMARY -> UNDERSTANDING -> EXTRACTION -> deterministic``) is untouched. That is asserted here
rather than assumed, in a process started the way a gateway starts.

Every claim the help text makes is checked in a subprocess, because these are module-scope constants
bound at import: a test that sets the variable afterwards would be testing something no deployment
does.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

# summary.model was here. A node summary is made by the extraction endpoint with the extraction
# key, so a separate model was a second name for the same call -- and the pair could be set to models
# one endpoint does not both serve, with no screen showing both. The summary uses the extraction
# model; what is left are choices ABOUT the summary rather than a second model.
SUMMARY_CONTROLS = ("summary.provider", "summary.max_tokens")

PROBE = """
import json, sys
sys.path.insert(0, %r)
import matrixark_mcp_core as core
print(json.dumps({"provider": core.summary_provider(),
                  "model": core.SUMMARY_LLM_MODEL,
                  "max_tokens": core.SUMMARY_LLM_MAX_TOKENS}))
""" % TOOLS

CLEAR = ("MATRIXARK_SUMMARY_PROVIDER", "MATRIXARK_SUMMARY_MODEL", "MATRIXARK_SUMMARY_MAX_TOKENS",
         "MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
         "MATRIXARK_EXTRACTION_MODEL", "OPENAI_MODEL")


def started_with(**overrides):
    """What a gateway resolves when it STARTS with this environment."""
    env = dict(os.environ)
    for name in CLEAR:
        env.pop(name, None)
    env.update({k: v for k, v in overrides.items() if v is not None})
    proc = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                          timeout=600, env=env, cwd=TOOLS)
    if proc.returncode != 0:
        raise AssertionError("the probe did not run: %s" % proc.stderr[-400:])
    return json.loads(proc.stdout.strip().splitlines()[-1])


class TheControlsExistTest(unittest.TestCase):

    def test_both_are_offered(self) -> None:
        for key in SUMMARY_CONTROLS:
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)

    def test_they_sit_with_the_endpoint_and_key_they_use(self) -> None:
        """A summary is sent to the extraction endpoint with the extraction key. Putting these in
        their own group would separate them from the two fields they depend on."""
        for key in SUMMARY_CONTROLS:
            with self.subTest(setting=key):
                self.assertEqual("extraction", cfg.SETTINGS_BY_KEY[key].group)

    def test_they_wait_for_a_restart(self) -> None:
        """SUMMARY_LLM_MODEL and SUMMARY_LLM_MAX_TOKENS are module constants in two modules, bound
        at import. `restart` is the only label true of all three."""
        for key in SUMMARY_CONTROLS:
            with self.subTest(setting=key):
                self.assertEqual("restart", cfg.SETTINGS_BY_KEY[key].applies)

    def test_the_provider_can_be_set_back_to_following_extraction(self) -> None:
        """The portal renders a `choices` setting as a select, so a list without the blank would be
        a one-way door: a customer who picked a provider could never return to "same as
        extraction"."""
        self.assertIn("", cfg.SETTINGS_BY_KEY["summary.provider"].choices)

    def test_the_provider_offers_only_what_writes_a_summary(self) -> None:
        """anthropic is offered for extraction and would be accepted here, and produce rule-written
        summaries with no error. It is left off the list on purpose."""
        self.assertNotIn("anthropic", cfg.SETTINGS_BY_KEY["summary.provider"].choices)


class AddingThemChangesNothingTest(unittest.TestCase):

    def test_the_string_defaults_are_blank(self) -> None:
        """A blank is not seeded into the environment, so the reader's fallback chain is untouched
        for a deployment that never opens the page."""
        self.assertEqual("", cfg.SETTINGS_BY_KEY["summary.provider"].default)

    def test_the_budget_default_is_the_one_the_code_uses(self) -> None:
        self.assertEqual(900, started_with()["max_tokens"])
        self.assertEqual("900", cfg.SETTINGS_BY_KEY["summary.max_tokens"].default)

    def test_storing_the_defaults_seeds_nothing(self) -> None:
        """The actual no-op guarantee: a customer who saves the form untouched must not pin the
        summary provider to whatever the blank resolves to."""
        environment = dict(os.environ)

        def restore() -> None:
            os.environ.clear()
            os.environ.update(environment)

        self.addCleanup(restore)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(tmp.name, "runtime.json")
        for name in CLEAR:
            os.environ.pop(name, None)

        cfg.update({"summary.provider": "", "summary.max_tokens": ""}, actor="test")
        cfg.apply_boot()
        self.assertIsNone(os.environ.get("MATRIXARK_SUMMARY_PROVIDER"))
        self.assertIsNone(os.environ.get("MATRIXARK_SUMMARY_MAX_TOKENS"))


class TheHelpTextIsTrueTest(unittest.TestCase):
    """Each claim, in a process started the way a gateway starts."""

    def test_blank_follows_the_extraction_provider_and_model(self) -> None:
        got = started_with(MATRIXARK_UNDERSTANDING_PROVIDER="openai_compatible",
                           MATRIXARK_EXTRACTION_MODEL="deepseek-chat")
        self.assertEqual("openai_compatible", got["provider"])
        self.assertEqual("deepseek-chat", got["model"])

    def test_a_named_summary_model_no_longer_wins(self) -> None:
        """This used to be the reason to offer the control: a smaller model for the call made most
        often. The call is made against the extraction endpoint with the extraction key, so the two
        names could be models one endpoint does not both serve -- and no screen showed the pair. The
        summary uses the extraction model; the token cap, which IS a property of the summary, still
        applies."""
        got = started_with(MATRIXARK_UNDERSTANDING_PROVIDER="openai_compatible",
                           MATRIXARK_EXTRACTION_MODEL="deepseek-chat",
                           MATRIXARK_SUMMARY_MODEL="deepseek-chat-lite",
                           MATRIXARK_SUMMARY_MAX_TOKENS="400")
        self.assertEqual("deepseek-chat", got["model"])
        self.assertEqual(400, got["max_tokens"])

    def test_anthropic_extraction_leaves_summaries_deterministic(self) -> None:
        """What the help warns about. The provider passes through, and the generator calls a model
        only for the openai-compatible names -- so the summary is rule-written and nothing errors."""
        got = started_with(MATRIXARK_UNDERSTANDING_PROVIDER="anthropic")
        self.assertEqual("anthropic", got["provider"])
        with open(os.path.join(TOOLS, "matrixark_mcp_core.py"), encoding="utf-8") as handle:
            source = handle.read()
        marker = source.index("You generate MatrixArk ContextNode traversal summaries")
        window = source[max(0, marker - 1200):marker]
        self.assertIn('if provider in {"openai", "openai_compatible", "openai_compatible_llm"}',
                      window)
        self.assertNotIn("anthropic", window.rsplit("if provider in", 1)[-1])

    def test_the_help_says_all_of_it(self) -> None:
        provider_help = cfg.SETTINGS_BY_KEY["summary.provider"].help
        self.assertIn("Blank follows the extraction provider", provider_help)
        self.assertIn("anthropic", provider_help)
        # There is no summary model control to describe. What remains is a cap on a call whose
        # model is the extraction model, and the cap says why it is separate.
        tokens_help = cfg.SETTINGS_BY_KEY["summary.max_tokens"].help
        self.assertIn("summary", tokens_help.lower())


@unittest.skipUnless(__import__("shutil").which("node"),
                     "node is not installed; the page JS cannot be run")
class TheSelectHonoursTheBlankTest(unittest.TestCase):
    """Asserting that "" is in `choices` proves what the registry says, not what the page draws.

    The page renders a `choices` setting as a `<select>`. A running value the list does not hold
    used to leave NO option selected -- the browser then showed the first, the screen reported a
    provider the deployment was not using, and saving made that report true. This runs the shipped
    `controlHtml` to check the blank is drawn and selected, is still offered once a provider has
    been picked, and that a value outside the list is carried and marked rather than dropped.

    The last part is not hypothetical: `summary_provider()` maps `oss`, `open_source`, `local_llm`
    and `oss_llm` onto `openai_compatible`, and the extraction reader takes `oss` and
    `oss_with_fallback`. None of those are offered here, so a deployment configured with one had a
    dropdown that did not contain its own provider.
    """

    HARNESS = os.path.join(TOOLS, "portal", "summary_provider_select_harness.js")
    PAGE = os.path.join(TOOLS, "portal", "setup_portal.html")

    def _run(self):
        return subprocess.run(["node", self.HARNESS, self.PAGE],
                              capture_output=True, text=True, timeout=300)

    def test_the_harness_passes(self) -> None:
        proc = self._run()
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_blank_is_drawn_and_selected(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   the blank option is there", out)
        self.assertIn("ok   and is the one selected when nothing is chosen", out)

    def test_it_is_not_a_one_way_door(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   and blank is still offered, so it is not a one-way door", out)

    def test_the_blank_reads_as_something(self) -> None:
        """It is the default on this setting, so it is the row most people see selected, and it
        used to be an empty line."""
        self.assertIn("ok   the blank reads as something rather than nothing", self._run().stdout)

    def test_a_value_outside_the_list_is_carried_and_marked(self) -> None:
        out = self._run().stdout
        self.assertIn("ok   a running value the list does not hold is still selected", out)
        self.assertIn("ok   an accepted alias is shown as the running value", out)
        self.assertIn("ok   and is marked as one the list does not offer", out)
        self.assertIn("ok   without dropping any offered choice", out)
        self.assertIn("ok   and exactly one option is selected", out)
        # The floor: an offered value must NOT pick up the marking, or the mark means nothing.
        self.assertIn("ok   FLOOR: a value the list does offer carries no marking", out)


class TheControlsAreNotDecorativeTest(unittest.TestCase):
    """A control whose variable nothing reads is worse than no control."""

    def test_each_variable_is_read_on_the_serving_path(self) -> None:
        for key in SUMMARY_CONTROLS:
            name = cfg._env_name(cfg.SETTINGS_BY_KEY[key], {})
            readers = []
            for entry in sorted(os.listdir(TOOLS)):
                if not entry.endswith(".py") or entry.startswith("test_"):
                    continue
                if entry == "matrixark_gateway_config.py":
                    continue
                with open(os.path.join(TOOLS, entry), encoding="utf-8") as handle:
                    if name in handle.read():
                        readers.append(entry)
            with self.subTest(setting=key, variable=name):
                self.assertTrue(readers, "%s is offered and nothing reads %s" % (key, name))


class TheTwoCopiesOfTheControlAgreeTest(unittest.TestCase):
    """`controlHtml` exists twice: in the shipped page and in the builder that writes pages.

    The harness runs the PAGE's copy, so an edit made to only one of them passes every test here
    while the next generated page carries the old behaviour. They were identical before this
    change and have to stay that way.
    """

    FILES = (os.path.join(TOOLS, "portal", "setup_portal.html"),
             os.path.join(TOOLS, "portal", "build_portal_pages.py"))

    @staticmethod
    def _control(path: str) -> str:
        text = io.open(path, encoding="utf-8").read()
        start = text.find("function controlHtml(f)")
        if start < 0:
            raise AssertionError("controlHtml is not in %s" % os.path.basename(path))
        depth = 0
        for index in range(text.index("{", start), len(text)):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    return text[start:index + 1]
        raise AssertionError("controlHtml is not closed in %s" % os.path.basename(path))

    def test_both_files_still_hold_one(self) -> None:
        # The floor: if the reader stops finding them, the equality below is "" == "".
        for path in self.FILES:
            body = self._control(path)
            self.assertGreater(len(body), 400, os.path.basename(path))
            self.assertIn("f.choices", body, os.path.basename(path))

    def test_they_are_the_same_function(self) -> None:
        page, builder = (self._control(path) for path in self.FILES)
        self.assertEqual(page, builder,
                         "the page and the builder disagree about how a control is drawn; the "
                         "harness only exercises the page")


if __name__ == "__main__":
    unittest.main()

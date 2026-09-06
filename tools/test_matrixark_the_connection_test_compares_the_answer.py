#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The connection test says whether the model that answered is the one that was asked for.

"Test connection" reported the model name from the response and stopped there. An operator reads
that as confirmation the endpoint serves that model. It is not: the co-located encoder server
returned ``payload.get("model", MODEL_NAME)`` -- the caller's own request, echoed -- while
encoding every text with the one model it loaded at startup. Ask a server running
``all-MiniLM-L6-v2`` for ``text-embedding-3-small`` and it answered, quickly, with 384 MiniLM
values labelled ``text-embedding-3-small``.

Nothing downstream could notice. Candidate encoders are routinely the same width --
``all-MiniLM-L6-v2`` and BGE-M3 truncated to 384 both emit 384 values -- so there is no length
mismatch to raise, retrieval degrades to noise against everything already stored, and the logs say
nothing. The one moment the question is actually put to the endpoint is this probe, so this is
where the answer has to be checked.

Two halves, and both are needed: the server now reports the model it loaded (which is also what
the OpenAI response contract says that field is), and the probe compares it with what it asked.
Fixing only the server would leave the portal not looking; fixing only the probe would leave it
comparing a value against itself.
"""
from __future__ import annotations

import ast
import io
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

SERVER = os.path.join(TOOLS, "context_minilm_embed_server.py")


class TheServerReportsWhatItLoadedTest(unittest.TestCase):
    """Read structurally, not by import: the module builds a SentenceTransformer at import time,
    so importing it here would download a model to run an assertion about a dict key."""

    @staticmethod
    def _embeddings_response() -> dict:
        """The dict literal sent back from the embeddings handler, as {key: source-of-value}."""
        with io.open(SERVER, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            keys = [k.value for k in node.keys if isinstance(k, ast.Constant)]
            if "data" in keys and "model" in keys:
                out = {}
                for key, value in zip(node.keys, node.values):
                    if isinstance(key, ast.Constant):
                        out[key.value] = value
                return out
        raise AssertionError("the embeddings response literal is not in %s"
                             % os.path.basename(SERVER))

    def test_the_response_literal_was_found(self) -> None:
        # The floor: every assertion below is about this dict, and a reader that finds nothing
        # would make them all vacuous.
        found = self._embeddings_response()
        self.assertIn("model", found)
        self.assertIn("data", found)

    def test_the_model_reported_is_the_one_it_loaded(self) -> None:
        value = self._embeddings_response()["model"]
        self.assertIsInstance(value, ast.Name,
                              "the model field is no longer a plain name")
        self.assertEqual("MODEL_NAME", value.id,
                         "the response reports something other than the loaded model; echoing "
                         "the request back is what made a mismatch invisible")

    def test_the_request_is_still_reported_separately(self) -> None:
        """Dropping it would lose the evidence of what was asked, which is half the comparison."""
        found = self._embeddings_response()
        self.assertIn("requested_model", found)
        self.assertIsInstance(found["requested_model"], ast.Name)

    def test_the_health_endpoint_still_names_the_loaded_model(self) -> None:
        with io.open(SERVER, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn('{"status": "ok", "model": MODEL_NAME}', source)


class TheComparisonIsNotFooledBySpellingTest(unittest.TestCase):
    """A false alarm on the shipped configuration is a warning nobody reads."""

    def test_an_organisation_prefix_is_not_a_difference(self) -> None:
        # The shipped local_minilm preset writes the short name; the encoder catalogue lists the
        # long one. One model, two spellings.
        self.assertTrue(cfg._same_model(
            "paraphrase-multilingual-MiniLM-L12-v2",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"))

    def test_nor_is_case(self) -> None:
        self.assertTrue(cfg._same_model("deepseek-chat", "DeepSeek-Chat"))

    def test_but_a_different_model_is(self) -> None:
        self.assertFalse(cfg._same_model("text-embedding-3-small", "all-MiniLM-L6-v2"))

    def test_and_a_different_model_under_the_same_org_is(self) -> None:
        self.assertFalse(cfg._same_model(
            "sentence-transformers/all-MiniLM-L6-v2",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"))

    def test_the_shipped_preset_would_not_raise_a_false_alarm(self) -> None:
        """Derived from the preset rather than written down, so a change to it is caught here."""
        import matrixark_v1_gateway as gw

        preset = cfg.PRESETS["local_minilm"]["values"]["embedding.model"]
        # The embedding catalogue is the gateway's MEASURED encoder table, not the config
        # module's -- asking cfg for it raises, on purpose.
        catalogue = [row["model"] for row in gw.embedding_picker_catalogue()]
        self.assertGreater(len(catalogue), 1, catalogue)
        exact = [name for name in catalogue if name == preset]
        same = [name for name in catalogue if cfg._same_model(preset, name)]
        # The point of the whole comparison: this pair is NOT equal as strings and IS the same
        # model. Comparing by equality would warn about the configuration this repo ships.
        self.assertEqual([], exact, "the spellings agree now; this test no longer proves anything")
        self.assertTrue(same, "the preset's model matches nothing in the catalogue: %r" % preset)


class TheSummaryCarriesBothSidesTest(unittest.TestCase):

    @staticmethod
    def _embedding(model, requested):
        return cfg._summarize("embedding",
                              {"model": model, "data": [{"embedding": [0.0] * 384}]}, requested)

    def test_a_mismatch_is_reported(self) -> None:
        out = self._embedding("all-MiniLM-L6-v2", "text-embedding-3-small")
        self.assertEqual("text-embedding-3-small", out["requested_model"])
        self.assertIs(False, out["model_matches_request"])

    def test_a_match_is_reported_too(self) -> None:
        self.assertIs(True, self._embedding("gpt", "gpt")["model_matches_request"])

    def test_nothing_asked_means_no_verdict(self) -> None:
        """Absent, not False. A probe that named no model has not been contradicted."""
        self.assertNotIn("model_matches_request", self._embedding("all-MiniLM-L6-v2", None))

    def test_nothing_answered_means_no_verdict(self) -> None:
        self.assertNotIn("model_matches_request", self._embedding(None, "anything"))

    def test_the_extraction_branches_compare_too(self) -> None:
        """Three branches build a summary and each had to be given the comparison; one left out
        would be a whole provider family silently unchecked."""
        chat = cfg._summarize("extraction",
                              {"model": "b", "choices": [{"message": {"content": "hi"}}]}, "a")
        anthropic = cfg._summarize("extraction_anthropic",
                                   {"model": "b", "content": [{"type": "text", "text": "hi"}]}, "a")
        for name, out in (("openai-shaped", chat), ("anthropic-shaped", anthropic)):
            self.assertIs(False, out.get("model_matches_request"), name)


class ThePageSaysSoTest(unittest.TestCase):

    HARNESS = os.path.join(PORTAL, "probe_model_harness.js")
    PAGE = os.path.join(PORTAL, "setup_portal.html")

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_the_shipped_panel_says_it(self) -> None:
        out = subprocess.run(["node", self.HARNESS, self.PAGE],
                             capture_output=True, text=True, timeout=600)
        self.assertIn("all ok", out.stdout + out.stderr)

    def test_both_copies_of_the_panel_agree(self) -> None:
        """probeHtml lives in the page AND in the builder that writes pages; the harness runs the
        page's copy, so an edit to one alone passes while the next generated page keeps the old
        behaviour."""
        bodies = []
        for name in ("setup_portal.html", "build_portal_pages.py"):
            with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
                text = handle.read()
            start = text.find("function probeHtml(r)")
            self.assertGreater(start, 0, name)
            depth = 0
            for index in range(text.index("{", start), len(text)):
                if text[index] == "{":
                    depth += 1
                elif text[index] == "}":
                    depth -= 1
                    if depth == 0:
                        bodies.append(text[start:index + 1])
                        break
        self.assertEqual(2, len(bodies))
        self.assertGreater(len(bodies[0]), 400)
        self.assertEqual(bodies[0], bodies[1])


if __name__ == "__main__":
    unittest.main()

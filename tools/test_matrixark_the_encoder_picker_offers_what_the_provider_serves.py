#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The encoder picker offers encoders the selected provider can actually serve.

Every entry in the measured catalogue is a model the deployment RUNS -- the hit@1 and throughput
figures come from encoding a real corpus on the machine that runs it, which is only possible for a
model we host. A deployment on Voyage was offered all five of them and none of the ones Voyage
serves.

The fix is a split rather than a filter on one list, because the obvious shape breaks a deliberate
rule. `test_matrixark_models` requires that the picker serves the MEASURED catalogue and nothing
else, and that every row in it carries the numbers a choice is made on -- a rule that exists because
a hand-written list beside the measured one drifted, omitting the whole e5 family and recommending
the encoder fifteen points of hit@1 behind. Adding unmeasured rows to `catalogue` would have relaxed
that; giving them invented numbers would have been worse, because the comparison table sorts and
marks a "best" column.

So `catalogue` is untouched -- it is evidence, and a deployment on a hosted encoder is still
entitled to see what self-hosting would score. `applicable` narrows what is OFFERED, and
`provider_models` carries what the provider serves instead, with notes and no measurements. Which of
those the dropdown shows is decided in the page, so `portal/encoder_options_harness.js` runs that
code out of the built page.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

try:
    from tools import matrixark_v1_gateway as gw  # type: ignore
except ImportError:
    import matrixark_v1_gateway as gw  # type: ignore

cfg = gw._gwconfig

PARTICIPATING = ("MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_EMBEDDING_MODEL",
                 "MATRIXARK_EMBEDDING_API_BASE")


class Case(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = {n: os.environ.get(n) for n in PARTICIPATING}
        for name in PARTICIPATING:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def body(self, provider: str) -> dict:
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = provider
        return gw._model_picker_body("embedding")

    def offered(self, provider: str) -> list:
        return [row["model"] for row in self.body(provider)["catalogue"]
                if row.get("applicable") is not False]

    def hosted(self, provider: str) -> list:
        return [row["model"] for row in self.body(provider)["provider_models"]]


class TheMeasuredCatalogueIsUntouchedTest(Case):
    """The rule this change had to work within, asserted here so a later simplification that folds
    the two lists together fails against the reason rather than against a style preference."""

    def test_every_row_still_carries_its_measurement(self) -> None:
        for provider in ("voyage", "openai_compatible", "deterministic"):
            for row in self.body(provider)["catalogue"]:
                with self.subTest(provider=provider, model=row["model"]):
                    self.assertIsInstance(row["hit_at_1"], (int, float))
                    self.assertGreater(row["hit_at_1"], 0)

    def test_the_evidence_is_shown_even_where_it_cannot_be_used(self) -> None:
        """A deployment on Voyage is still entitled to see what self-hosting would score."""
        self.assertEqual(len(self.body("openai_compatible")["catalogue"]),
                         len(self.body("voyage")["catalogue"]))

    def test_no_hosted_encoder_carries_a_measurement(self) -> None:
        """Invented numbers would be sorted and marked "best" against measured ones."""
        for row in gw._HOSTED_ENCODERS:
            for field in ("hit_at_1", "hit_at_5", "texts_per_s", "vectors_mb_per_doc"):
                with self.subTest(model=row["model"], field=field):
                    self.assertNotIn(field, row)


class TheOfferMatchesWhatTheProviderCanServeTest(Case):

    def test_a_hosted_provider_is_not_offered_a_self_hosted_encoder(self) -> None:
        self.assertEqual([], [name for name in self.offered("voyage") if "/" in name])

    def test_it_is_offered_what_that_provider_serves(self) -> None:
        self.assertEqual(["voyage-3"], self.hosted("voyage"))

    def test_an_openai_compatible_endpoint_keeps_both_worlds(self) -> None:
        """One provider value covers OpenAI itself and a local server behind the same protocol,
        which is what the MiniLM preset configures. Narrowing to either would be wrong."""
        self.assertTrue([name for name in self.offered("openai_compatible") if "/" in name])
        self.assertIn("text-embedding-3-small", self.hosted("openai_compatible"))
        self.assertNotIn("voyage-3", self.hosted("openai_compatible"))

    def test_an_in_process_encoder_is_offered_no_hosted_name(self) -> None:
        """It loads a model rather than calling one, so a hosted name is not something it could be
        pointed at."""
        self.assertEqual([], self.hosted("oss"))
        self.assertTrue([name for name in self.offered("oss") if "/" in name])

    def test_nothing_chosen_yet_narrows_nothing(self) -> None:
        for provider in ("deterministic", "cohere", ""):
            with self.subTest(provider=provider):
                self.assertTrue([name for name in self.offered(provider) if "/" in name])
                self.assertEqual(len(gw._HOSTED_ENCODERS), len(self.hosted(provider)))

    def test_every_hosted_name_is_one_the_code_already_uses(self) -> None:
        """Not written from general knowledge: each is the encoder's own default for that provider,
        or a preset. An invented name is a model nobody can check."""
        with open(os.path.join(TOOLS, "matrixark_mcp_embeddings.py"), encoding="utf-8") as handle:
            encoder = handle.read()
        preset_models = {str(preset["values"].get("embedding.model", ""))
                         for preset in cfg.PRESETS.values()}
        for row in gw._HOSTED_ENCODERS:
            with self.subTest(model=row["model"]):
                self.assertTrue(row["model"] in encoder or row["model"] in preset_models,
                                "%s is named nowhere in the code" % row["model"])

    def test_the_extraction_side_gains_neither_field(self) -> None:
        """These two only mean something for encoders; carrying them on the other target would be a
        field the page has to learn to ignore."""
        body = gw._model_picker_body("extraction")
        self.assertNotIn("provider_models", body)
        self.assertTrue(all("applicable" not in row for row in body["catalogue"]))


class ThePageOffersWhatTheRouteMarkedTest(unittest.TestCase):
    """`applicable` only means anything if the dropdown reads it, and that is page code."""

    def test_the_harness_passes(self) -> None:
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is not available")
        result = subprocess.run(
            [node, os.path.join(PORTAL, "encoder_options_harness.js"),
             os.path.join(PORTAL, "setup_portal.html")],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("all ok", result.stdout)

    def test_the_built_page_carries_the_change(self) -> None:
        """The page is generated; a change made only to the generator ships nothing."""
        with open(os.path.join(PORTAL, "setup_portal.html"), encoding="utf-8") as handle:
            page = handle.read()
        self.assertIn("d.provider_models", page)
        self.assertIn("c.applicable === false", page)


if __name__ == "__main__":
    unittest.main()

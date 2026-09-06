#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal offers one encoder model field, because there is one encoder model.

There were two: ``embedding.model`` and ``embedding.model_path``. They are not two choices. The
encoder reads ``MODEL_PATH or MODEL``, so the path only ever **overrode** the name -- and only on the
in-process path, because a hosted provider is sent the model NAME and never looks at the path at all:

    in-process   model_ref = MODEL_PATH or MODEL or "intfloat/multilingual-e5-large"
    hosted       model     = MODEL

So one of the two fields silently won on one path and did nothing on the other, and a customer had
to know which encoder they were on to know which field they were filling in. A model name and a
local path are both just "the encoder to load", and the field takes either, because the encoder does.

The variable is still honoured where a launcher sets it -- that is not the portal's to clear -- and
the panel says so, with the consequence spelled out per provider. A value in force that no screen
shows is the thing that panel exists to prevent.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

try:
    from tools import matrixark_v1_gateway as gw  # type: ignore
except ImportError:
    import matrixark_v1_gateway as gw  # type: ignore

try:
    from tools import matrixark_mcp_embeddings as encoder  # type: ignore
except ImportError:
    import matrixark_mcp_embeddings as encoder  # type: ignore

cfg = gw._gwconfig
ENCODER = "matrixark_mcp_embeddings.py"
PATH_VARIABLE = "MATRIXARK_EMBEDDING_MODEL_PATH"
NAME_VARIABLE = "MATRIXARK_EMBEDDING_MODEL"
PARTICIPATING = ("MATRIXARK_EMBEDDING_PROVIDER", NAME_VARIABLE, PATH_VARIABLE,
                 "MATRIXARK_EMBEDDING_API_BASE", "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS")


def in_process_resolutions() -> list:
    """Every ``a or b or "<the in-process default>"`` chain in the encoder, as its variable names.

    Identified by the DEFAULT it ends in rather than by the variable it starts with, so a read site
    that stopped consulting the path is still found -- which is the whole point: a site that resolves
    an in-process model and does not prefer the path makes the panel's warning wrong.
    """
    with open(os.path.join(TOOLS, ENCODER), encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=ENCODER)
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.BoolOp) or not isinstance(node.op, ast.Or):
            continue
        literals = [c.value for c in ast.walk(node)
                    if isinstance(c, ast.Constant) and isinstance(c.value, str)]
        names = []
        for value in node.values:
            for sub in ast.walk(value):
                if (isinstance(sub, ast.Call) and sub.args
                        and isinstance(sub.args[0], ast.Constant)
                        and isinstance(sub.args[0].value, str)):
                    names.append(sub.args[0].value)
                    break
        if literals and names:
            found.append((names, literals[-1]))
    return found


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

    def on(self, provider: str, **environment) -> None:
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = provider
        for name, value in environment.items():
            os.environ[name] = value

    def warnings_about_the_path(self, provider: str, path: str = "") -> list:
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = provider
        if path:
            os.environ[PATH_VARIABLE] = path
        return [w for w in gw._model_config_snapshot()["warnings"] if PATH_VARIABLE in w]


class ThePremiseIsStillTrueTest(Case):
    """Everything here rests on what the encoder DOES with the two variables, so it is asked rather
    than read. An earlier version of this parsed the source for `PATH or NAME`; a mutation that
    flipped one of the three read sites left the parse counting two chains against two mentions and
    passing. Behaviour does not have that hole."""

    def test_every_in_process_resolution_prefers_the_path(self) -> None:
        """Found by the default each chain ends in, not by the variable it starts with -- a site
        that quietly stopped consulting the path would otherwise leave the count matching and the
        panel's warning wrong. `embedding_model_name` supplies the default, so this cannot drift
        from the encoder either."""
        self.on("oss")
        os.environ.pop(NAME_VARIABLE, None)
        os.environ.pop(PATH_VARIABLE, None)
        default = encoder.embedding_model_name()
        chains = [names for names, literal in in_process_resolutions() if literal == default]
        self.assertTrue(chains, "no chain ends in the in-process default %r" % default)
        for names in chains:
            with self.subTest(chain=names):
                self.assertEqual(PATH_VARIABLE, names[0])
                self.assertIn(NAME_VARIABLE, names)

    def test_an_in_process_encoder_prefers_the_path(self) -> None:
        self.on("oss", **{NAME_VARIABLE: "a-name", PATH_VARIABLE: "/a/path"})
        self.assertEqual("/a/path", encoder.embedding_model_name())

    def test_an_in_process_encoder_uses_the_name_when_no_path_is_set(self) -> None:
        """Which is what lets one field carry both: a name reaches the in-process encoder too."""
        self.on("oss", **{NAME_VARIABLE: "a-name"})
        self.assertEqual("a-name", encoder.embedding_model_name())

    def test_a_hosted_encoder_ignores_the_path_entirely(self) -> None:
        self.on("voyage", **{NAME_VARIABLE: "voyage-3", PATH_VARIABLE: "/a/path"})
        self.assertEqual("voyage-3", encoder.embedding_model_name())

    def test_the_two_answers_really_do_differ(self) -> None:
        """The floor. If this function returned the same thing everywhere, the three assertions
        above would agree with each other and say nothing."""
        self.on("oss", **{NAME_VARIABLE: "a-name", PATH_VARIABLE: "/a/path"})
        in_process = encoder.embedding_model_name()
        self.on("voyage", **{NAME_VARIABLE: "voyage-3", PATH_VARIABLE: "/a/path"})
        self.assertNotEqual(in_process, encoder.embedding_model_name())


class ThePortalOffersOneFieldTest(unittest.TestCase):

    def test_the_second_field_is_gone(self) -> None:
        self.assertNotIn("embedding.model_path", cfg.SETTINGS_BY_KEY)

    def test_exactly_one_setting_names_an_encoder_model(self) -> None:
        """Counted rather than asserted about one key, so a third field cannot appear beside it."""
        naming = [key for key, setting in cfg.SETTINGS_BY_KEY.items()
                  if setting.group == "embedding" and "model" in key
                  and key != "embedding.require_model_embeddings"]
        self.assertEqual(["embedding.model"], naming)

    def test_the_field_says_it_takes_either_spelling(self) -> None:
        """A customer should not have to know which encoder path they are on to know what to type."""
        help_text = cfg.SETTINGS_BY_KEY["embedding.model"].help
        self.assertIn("a path to one you have downloaded", help_text)
        self.assertIn("hosted provider is sent the name", help_text)

    def test_a_stored_value_for_the_retired_field_is_no_longer_applied(self) -> None:
        """`apply_boot` walks the registry, so a key that left it stops reaching the environment --
        which is what makes "one field decides" true rather than aspirational."""
        os.environ.pop(PATH_VARIABLE, None)
        seeded = cfg.apply_boot({"values": {"embedding.model_path": "/models/left-behind",
                                            "embedding.model": "e5-small"}})
        self.assertNotIn(PATH_VARIABLE, seeded)
        self.assertIsNone(os.environ.get(PATH_VARIABLE))

    def test_the_field_that_remains_is_still_applied(self) -> None:
        """The floor: if apply_boot seeded nothing, the assertion above would pass on a build that
        had stopped applying settings altogether."""
        seeded = cfg.apply_boot({"values": {"embedding.model": "e5-small"}})
        self.assertIn(NAME_VARIABLE, seeded)


class ALauncherSetPathIsReportedTest(Case):
    """The portal cannot clear a variable the launcher set, so it has to say what it is doing."""

    def test_an_in_process_encoder_is_told_the_field_is_overridden(self) -> None:
        warning = self.warnings_about_the_path("oss", "/models/e5-large")
        self.assertEqual(1, len(warning))
        self.assertIn("/models/e5-large", warning[0])
        self.assertIn("not the one making vectors", warning[0])

    def test_a_hosted_provider_is_told_it_does_nothing(self) -> None:
        """Two different consequences. Reporting the in-process wording here would send someone
        looking for a problem that is not affecting them."""
        for provider in ("openai_compatible", "voyage"):
            with self.subTest(provider=provider):
                warning = self.warnings_about_the_path(provider, "/models/e5-large")
                self.assertEqual(1, len(warning))
                self.assertIn("never reads it", warning[0])
                self.assertNotIn("not the one making vectors", warning[0])

    def test_it_says_where_to_put_the_path_instead(self) -> None:
        warning = self.warnings_about_the_path("oss", "/models/e5-large")[0]
        self.assertIn("Embedding model", warning)

    def test_nothing_is_said_when_nothing_is_set(self) -> None:
        for provider in ("oss", "openai_compatible", "deterministic"):
            with self.subTest(provider=provider):
                self.assertEqual([], self.warnings_about_the_path(provider))


if __name__ == "__main__":
    unittest.main()

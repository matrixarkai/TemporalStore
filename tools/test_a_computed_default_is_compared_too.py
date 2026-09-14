#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A portal default whose code counterpart is a CALL was never compared to anything.

`test_the_portal_shows_the_default_the_code_actually_uses` compares the portal's declared default
against the literal fallback at the read site. Its own docstring says what it leaves out: settings
that "have no literal fallback in Python at all". A fallback written as a call is exactly that, so
the comparison skips them silently -- there is no failure, no count, nothing to notice.

One was wrong by 4x and had been for as long as the call existed:

    matrixark_resource_parser   DEFAULT_EMBEDDING_TEXT_MAX_TOKENS = int(
                                    os.environ.get(...) or str(encoder_window_tokens()))   -> 512
    portal Setting              "embedding.text_max_tokens" declared                       -> "128"
    matrixark_v1_gateway        config snapshot fallback                                   -> "128"

128 is the window chunking STOPPED using. `matrixark_resource_parser` records the change beside the
constant: chunk size and embedded window "used to disagree -- chunks were 240 tokens and only 128
were embedded, so roughly half of every chunk was findable only through a lexical index whose terms
the retrieve path cannot consult." Both now follow the encoder. The portal did not.

AND IT WAS NOT ONLY A DISPLAY. `setting.default` is what the config resolver RETURNS when nothing
is set -- `_effective` ends `return setting.default, "default"` -- so the resolver answered 128
while the ingest path ran 512.

The fix uses the mechanism this file already has: `_EXPLICIT_BUILD_DEFAULT` maps a setting key to a
build constant, `_apply_build_defaults` rewrites the declared default from it and appends "With
nothing set this deployment runs N." to the help. No number is re-typed anywhere.

This guard asserts the two agree, and asserts the MECHANISM is what makes them agree, so replacing
the wiring with a hand-typed "512" fails here.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_gateway_config as config_module
import matrixark_resource_parser as parser_module

KEY = "embedding.text_max_tokens"
ENV = "MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS"


def _setting():
    return config_module.SETTINGS_BY_KEY.get(KEY)


class AComputedDefaultIsComparedToo(unittest.TestCase):

    def test_the_setting_is_still_offered(self) -> None:
        """A floor. Every assertion below passes vacuously if the key moved."""
        setting = _setting()
        self.assertIsNotNone(setting, "%s is no longer offered on the portal" % KEY)
        self.assertEqual(ENV, setting.env)

    def test_the_declared_default_is_what_the_code_runs(self) -> None:
        """The defect this file exists for, stated as the number an operator is told."""
        setting = _setting()
        self.assertEqual(
            str(parser_module.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS), str(setting.default),
            "the portal declares %r for %s and the ingest path runs %r. `setting.default` is also "
            "what the config resolver returns when nothing is set, so this is not only a display."
            % (setting.default, ENV, parser_module.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS))

    def test_the_default_follows_the_encoder_rather_than_a_typed_number(self) -> None:
        """Asserts the MECHANISM, not just today's value.

        Without this, someone could delete the wiring and type the current number into the Setting,
        the test above would still pass, and the two would drift again the next time the encoder
        window changed.
        """
        self.assertIn(
            KEY, config_module._EXPLICIT_BUILD_DEFAULT,
            "%s is no longer wired to a build constant, so its declared default is a second copy "
            "of a number decided elsewhere" % KEY)
        constant = config_module._EXPLICIT_BUILD_DEFAULT[KEY]
        self.assertEqual(("DEFAULT_EMBEDDING_TEXT_MAX_TOKENS", "matrixark_resource_parser"),
                         constant)
        self.assertEqual(
            str(parser_module.encoder_window_tokens()),
            str(parser_module.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS),
            "the parser's default stopped following the encoder window, so the chunk and the "
            "embedded window can disagree again")

    def test_the_help_says_what_the_build_runs(self) -> None:
        """The operator-facing half of the mechanism."""
        setting = _setting()
        self.assertIn(
            "With nothing set this deployment runs %s." % setting.default, setting.help,
            "the build-default sentence is missing from the help, so an operator reading the page "
            "cannot tell which number is in force")

    def test_the_gateway_snapshot_does_not_carry_its_own_copy(self) -> None:
        """The third surface. It used to hardcode the same stale 128."""
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8",
                  errors="replace") as handle:
            body = handle.read()
        self.assertNotIn(
            '_env("MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS", "128")', body,
            "the gateway config snapshot has gone back to a literal window size")
        self.assertIn(
            'SETTINGS_BY_KEY["embedding.text_max_tokens"].default', body,
            "the gateway snapshot no longer takes the window from the Setting, so it is free to "
            "report a different number than the portal")


if __name__ == "__main__":
    unittest.main()

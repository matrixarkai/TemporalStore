#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A warning names the control, not the environment variable behind it.

Every one of these is rendered on the Setup page, inches from a control for the thing it names, and
each told the reader to go and set an environment variable::

    Set MATRIXARK_EXTRACTION_PROVIDER=openai_compatible with
    MATRIXARK_EXTRACTION_BASE_URL/_MODEL/_API_KEY_ENV to enable model extraction.

One of them could not be acted on at all. ``OPENAI_API_KEY is empty`` names a variable **two**
controls write -- Extraction API key and Embedding API key -- so it does not say which one to fill
in. All 103 labels are distinct, so naming the control always is.

The rule below is the general one, because the specific wordings are what drifted: **a warning that
mentions a variable the portal has a control for must name that control.** Where there is no control
it may name the variable, and must -- ``MATRIXARK_REQUIRE_AUTH`` and ``MATRIXARK_ACCESS_MODE`` have
none, so the anonymous-access warning names them and that is the only actionable thing it can say.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402

VARIABLE = re.compile(r"\b(MATRIXARK_[A-Z0-9_]+|OPENAI_API_KEY|DEEPSEEK_API_KEY)\b")

# Everything the portal offers a control for, and what that control is called.
CONTROLS: dict = {}
for _setting in cfg.SETTINGS:
    _name = cfg._env_name(_setting, {})
    if _name:
        CONTROLS.setdefault(_name, []).append(_setting.label)

KNOBS = ("MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
         "MATRIXARK_EXTRACTION_BASE_URL", "MATRIXARK_EXTRACTION_API_KEY_ENV",
         "MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_EMBEDDING_API_BASE",
         "MATRIXARK_EMBEDDING_API_KEY_ENV", "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS",
         "OPENAI_API_KEY", "DEEPSEEK_API_KEY", "MATRIXARK_EMBED_BASE_URL")

# Configurations chosen to raise as many different warnings as possible.
SHAPES = (
    {},
    {"MATRIXARK_UNDERSTANDING_PROVIDER": "openai_compatible",
     "MATRIXARK_EMBEDDING_PROVIDER": "openai_compatible",
     "MATRIXARK_EMBEDDING_API_BASE": "https://api.openai.com"},
    {"MATRIXARK_UNDERSTANDING_PROVIDER": "openai_compatible",
     "MATRIXARK_EXTRACTION_BASE_URL": "https://api.deepseek.com/v1",
     "MATRIXARK_EMBEDDING_PROVIDER": "openai_compatible",
     "MATRIXARK_EMBEDDING_API_BASE": "https://api.openai.com/v1",
     "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS": "1",
     "OPENAI_API_KEY": "sk-something"},
)


class _Configured(unittest.TestCase):

    def setUp(self) -> None:
        previous = {name: os.environ.get(name) for name in KNOBS}

        def restore() -> None:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

        self.addCleanup(restore)

    def every_warning(self):
        seen = []
        for shape in SHAPES:
            for name in KNOBS:
                os.environ.pop(name, None)
            for name, value in shape.items():
                os.environ[name] = value
            for warning in gw._model_config_snapshot().get("warnings") or []:
                if warning not in seen:
                    seen.append(warning)
        return seen


class AWarningNamesTheControlTest(_Configured):

    def test_there_are_warnings_to_check(self) -> None:
        """The rule below is a loop; over an empty list it proves nothing."""
        self.assertGreaterEqual(len(self.every_warning()), 5)

    def test_the_rule_has_something_to_bite_on(self) -> None:
        """At least one warning must mention a variable that HAS a control, or the check is inert."""
        mentions = [w for w in self.every_warning()
                    if any(v in CONTROLS for v in VARIABLE.findall(w))]
        self.assertTrue(mentions, "no warning names a variable the portal controls")

    def test_every_variable_with_a_control_is_named_by_its_control(self) -> None:
        for warning in self.every_warning():
            for variable in sorted(set(VARIABLE.findall(warning))):
                labels = CONTROLS.get(variable)
                if not labels:
                    continue
                with self.subTest(variable=variable):
                    self.assertTrue(
                        any(label in warning for label in labels),
                        "this warning names %s and none of its controls (%s):\n  %s"
                        % (variable, ", ".join(labels), warning))

    def test_no_warning_tells_a_customer_to_set_a_variable_it_has_a_control_for(self) -> None:
        """The specific phrasing that started this: an instruction to go to a shell."""
        for warning in self.every_warning():
            for variable in sorted(set(VARIABLE.findall(warning))):
                if variable not in CONTROLS:
                    continue
                with self.subTest(variable=variable):
                    self.assertNotRegex(
                        warning, r"[Ss]et %s[= ]" % re.escape(variable),
                        "this warning tells a customer to set %s, which is a control:\n  %s"
                        % (variable, warning))


class AVariableWithNoControlIsStillNamedTest(_Configured):
    """The other half. Dropping the variable where there is nothing to point at would leave the
    reader with no way to act at all."""

    def test_the_anonymous_access_warning_still_names_its_variables(self) -> None:
        self.assertNotIn("MATRIXARK_REQUIRE_AUTH", CONTROLS)
        self.assertNotIn("MATRIXARK_ACCESS_MODE", CONTROLS)
        previous = dict(gw._AUTH_POSTURE)

        def restore() -> None:
            gw._AUTH_POSTURE.clear()
            gw._AUTH_POSTURE.update(previous)

        self.addCleanup(restore)
        gw._AUTH_POSTURE["require_auth"] = False
        said = [w for w in gw._model_config_snapshot()["warnings"] if "anonymous" in w]
        self.assertEqual(1, len(said), said)
        self.assertIn("MATRIXARK_REQUIRE_AUTH=1", said[0])
        self.assertIn("MATRIXARK_ACCESS_MODE=enforced", said[0])


class TheLabelsAreWorthNamingTest(unittest.TestCase):

    def test_every_label_is_distinct(self) -> None:
        """Naming a control only works if the name picks one out. If two ever share a label, a
        warning that names it becomes as ambiguous as the variable it replaced."""
        labels = [s.label for s in cfg.SETTINGS]
        self.assertEqual(len(labels), len(set(labels)),
                         sorted({l for l in labels if labels.count(l) > 1}))

    def test_the_ambiguous_variable_is_why_this_matters(self) -> None:
        """OPENAI_API_KEY is written by two controls, so naming the variable cannot say which key
        to fill in. This pins the reason rather than leaving it in a commit message."""
        self.assertEqual(2, len(CONTROLS.get("OPENAI_API_KEY", [])), CONTROLS.get("OPENAI_API_KEY"))


if __name__ == "__main__":
    unittest.main()

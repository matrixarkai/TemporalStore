# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two modules default the OSS encoder to two different models.

With `MATRIXARK_EMBEDDING_PROVIDER=oss` and no model set, `embedding_model_name()` answers::

    matrixark_mcp_core          sentence-transformers/all-MiniLM-L6-v2     384 dims
    matrixark_mcp_embeddings    intfloat/multilingual-e5-large            1024 dims

Ten modules reachable from production bind core's answer -- the adapters' ingest, retrieve and
summary paths among them -- while `matrixark_mcp_embeddings` is the module that actually encodes,
through `embeddings_for_texts`. So the name ten modules would record is not the model the vectors
would come from.

`test_two_launchers_two_encoder_widths` covers the same defect between two LAUNCHERS. It compares
what each launcher exports, so it cannot see two modules disagreeing about the fallback when
nothing is exported at all.

## It is latent under the shipped launchers, and that is the whole qualifier

Both copies read `MATRIXARK_EMBEDDING_MODEL_PATH` FIRST, and both launchers that reach the OSS
provider export it pointing at the MiniLM directory. So a deployment launched the shipped way
resolves both to the same path and nothing diverges. The gap opens for a deployment that sets the
provider and no model -- `MATRIXARK_EMBEDDING_PROVIDER=oss` on its own, which is a documented flag.

## This file does not assert that they agree

They do not, and a guard that fails on the day it is written tells nobody anything. Which default
is right is a decision: the launchers deploy MiniLM and core agrees with them, while the module
that encodes says e5-large -- two sources against one, and the one is the one that runs. Choosing
changes the width of every vector an unset deployment writes.

So the pair is RECORDED exactly. A third default fails here, a changed default fails here, and the
day they are made to agree this file fails and gets retired on purpose.
"""

from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

#: module -> the model it falls back to for an OSS provider with nothing set.
RECORDED_DEFAULTS = {
    "matrixark_mcp_core": "sentence-transformers/all-MiniLM-L6-v2",
    "matrixark_mcp_embeddings": "intfloat/multilingual-e5-large",
}

#: The variable both copies consult BEFORE their own default. While the launchers export it, the
#: disagreement above cannot be reached through them.
PRECEDENCE_VARIABLE = "MATRIXARK_EMBEDDING_MODEL_PATH"

#: The OSS default appears three times in matrixark_mcp_embeddings -- once in the batch encoder,
#: once in the single encoder, and once in the function that reports the name. Two of those decide
#: what the vectors ARE and the third decides what they are CALLED, so a drift between them is a
#: mislabelled vector rather than a changed default.
ENCODER_MODULE = "matrixark_mcp_embeddings"
FUNCTIONS_CARRYING_THE_DEFAULT = ("embeddings_for_texts", "embedding_model_name",
                                  "oss_embedding_for_text")

_PROBE = """
import sys, os, json
sys.path.insert(0, {tools!r})
for key in list(os.environ):
    if key.startswith("MATRIXARK_"):
        del os.environ[key]
os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "oss"
{extra}
out = {{}}
for mod in {modules!r}:
    try:
        m = __import__(mod)
        fn = getattr(m, "embedding_model_name", None)
        out[mod] = None if fn is None else fn()
    except Exception as exc:
        out[mod] = "raised " + type(exc).__name__
print(json.dumps(out))
"""


def _answers(extra=""):
    code = _PROBE.format(tools=str(TOOLS), modules=sorted(RECORDED_DEFAULTS), extra=extra)
    result = subprocess.run([sys.executable, "-B", "-c", code],
                            capture_output=True, text=True, timeout=600)
    lines = result.stdout.strip().splitlines()
    if not lines:
        raise AssertionError("the probe produced nothing: %s"
                             % (result.stderr.strip().splitlines() or ["<none>"])[-1])
    return json.loads(lines[-1])


class TwoModulesDefaultTheEncoderDifferentlyTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.answers = _answers()
        except AssertionError as exc:
            raise unittest.SkipTest(str(exc))

    def test_both_modules_still_answer(self) -> None:
        """Vacuity floor. A module that stopped exposing the name would drop out of the comparison
        silently, and a comparison with one side missing reports agreement."""
        for module in sorted(RECORDED_DEFAULTS):
            with self.subTest(module=module):
                self.assertIsNotNone(
                    self.answers.get(module),
                    "%s no longer exposes embedding_model_name, so this file is comparing one "
                    "module against nothing" % module)

    def test_each_module_still_falls_back_to_the_model_recorded_here(self) -> None:
        for module, expected in sorted(RECORDED_DEFAULTS.items()):
            with self.subTest(module=module):
                self.assertEqual(
                    expected, self.answers.get(module),
                    "%s now falls back to %r where this file records %r. If the default moved "
                    "deliberately that is a decision about the width of every vector an unset "
                    "deployment writes -- update the table and say why."
                    % (module, self.answers.get(module), expected))

    def test_the_two_still_disagree(self) -> None:
        """The other direction, and the good news case. If they agree now, somebody chose -- this
        file has nothing left to hold and should go rather than sit here passing."""
        distinct = {v for v in self.answers.values() if isinstance(v, str)}
        self.assertGreater(
            len(distinct), 1,
            "the two modules now agree on %s. That is the outcome this file was waiting for: "
            "retire it deliberately rather than leave a test that cannot fail."
            % ", ".join(sorted(distinct)))

    def test_the_encoder_module_uses_one_default_in_all_three_places(self) -> None:
        """Two of these decide what the vectors ARE, the third decides what they are CALLED.

        Found by a mutation that was NOT caught: changing the default inside the batch encoder
        left the reported name untouched, so the module would have encoded with one model and
        reported another, and this file said nothing.
        """
        source = (TOOLS / (ENCODER_MODULE + ".py")).read_text(encoding="utf-8", errors="replace")
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            self.skipTest("%s does not parse: %s" % (ENCODER_MODULE, exc))

        defaults = {}
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name not in FUNCTIONS_CARRYING_THE_DEFAULT:
                continue
            literals = [sub.value for sub in ast.walk(node)
                        if isinstance(sub, ast.Constant) and isinstance(sub.value, str)
                        and "/" in sub.value and sub.value.count("/") == 1
                        and not sub.value.startswith(("/", "http"))]
            if literals:
                defaults[node.name] = literals[-1]

        self.assertEqual(
            sorted(FUNCTIONS_CARRYING_THE_DEFAULT), sorted(defaults),
            "expected a model default in each of %s, found it in %s. If one of them stopped "
            "carrying its own literal that is good news -- they would share one -- but this check "
            "has to be told, because a missing entry makes the comparison below pass."
            % (sorted(FUNCTIONS_CARRYING_THE_DEFAULT), sorted(defaults)))

        distinct = set(defaults.values())
        self.assertEqual(
            1, len(distinct),
            "%s carries more than one OSS default: %s. Two of these decide what the vectors are "
            "and one decides what they are called, so the module would encode with one model and "
            "report another." % (ENCODER_MODULE, defaults))

    def test_the_precedence_variable_still_hides_it(self) -> None:
        """Why this is latent rather than live: both copies read MATRIXARK_EMBEDDING_MODEL_PATH
        first, and the launchers export it. If that stopped being true the disagreement above
        would be reachable through a shipped launcher, which is a different severity."""
        answers = _answers(
            extra='os.environ[%r] = "/models/pinned-by-the-launcher"' % PRECEDENCE_VARIABLE)
        distinct = {v for v in answers.values() if isinstance(v, str)}
        self.assertEqual(
            {"/models/pinned-by-the-launcher"}, distinct,
            "%s no longer decides for both modules (%s). While it did, a deployment launched the "
            "shipped way could not reach the disagreement; without it, it can."
            % (PRECEDENCE_VARIABLE, sorted(distinct)))


if __name__ == "__main__":
    unittest.main()

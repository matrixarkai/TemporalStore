# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The chunk window and the embedding window both follow the encoder, not two constants.

They used to disagree: chunks were capped at 240 tokens and only the first 128 reached the encoder,
while multilingual-e5-large reads 512. Three quarters of the window went unused, and the tail of
every chunk was absent from its own vector -- findable only through a lexical index whose terms the
retrieve path cannot consult.

What is pinned here is the RELATIONSHIP, not the number. A deployment on a different encoder gets
that encoder's window; hard-coding 512 would silently over-feed a 384-token model and under-feed an
8192-token one.

`test_the_embedding_window_is_the_encoder_window` compared the two in this process, with the
environment as it stood when the module was imported -- the one condition in which they cannot
disagree. `encoder_window_tokens()` resolves per call and the constant did not, so they parted
company the moment the encoder changed, which is the only circumstance in which the relationship
can break at all. `embedding.model` is labelled `live` on the operator page, so that circumstance
is one the page invites.

Measured on 32,000 words of input -- far more than any window here -- by asking the BUILDER what
it produces, not by reading a constant:

    moment                          model window   words built, before   after
    MiniLM, at import                        512                   465     465
    bge-m3, after a write                   8192                   465    7447
    mpnet, after a write                     384                   465     349
    back to MiniLM                           512                   465     465

465 words against a 384-token window is the over-feeding the comment on `encoder_window_tokens`
names: the tail is truncated by the model's own tokenizer and never reaches the vector. Against
8192 it is fifteen times less text than the encoder would have read.

The change case runs in a FRESH SUBPROCESS. This module's environment is process-wide and the
suite runs in one process, so setting an encoder here to prove a point would leave it set for
whatever runs next.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_resource_parser as parser


class WindowsFollowTheEncoder(unittest.TestCase):
    def test_the_chunk_window_is_deliberately_not_enlarged(self):
        """Enlarging chunks shrinks the footprint but covers the text with fewer vectors.

        Measured: chunks at the encoder's 512 gives 2,510 records -> 744 and 6.83 MB -> 2.90 MB,
        but vectors fall from 50.9% of the footprint to 35.6% because the same text is covered by
        a third as many. The chunk is also the retrieval unit, so it stays the finer one. This is a
        choice, and it is the knob a deployment overrides if it wants the smaller footprint.
        """
        self.assertLess(parser.DEFAULT_MAX_CHUNK_TOKENS, parser.encoder_window_tokens())

    def test_the_embedding_window_is_the_encoder_window(self):
        """As this process started. `test_the_ceiling_in_use_follows_a_changed_encoder` asks the
        same thing of the case this one cannot reach."""
        self.assertEqual(parser.encoder_window_tokens(),
                         parser.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS)

    def test_the_declared_default_is_what_the_module_started_with(self):
        """The constant is the DECLARED default -- what the operator page shows and what
        `test_a_computed_default_is_compared_too` compares against. It is read at import on
        purpose, so it must stay a plain int and stay equal to the resolver at startup."""
        self.assertIsInstance(parser.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS, int)
        self.assertEqual(parser.embedding_text_max_tokens(),
                         parser.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS,
                         "nothing has changed the encoder in this process, so the value in use "
                         "and the declared default must agree")

    def test_the_whole_chunk_reaches_its_own_vector(self):
        """The failure the old defaults produced: text stored but absent from its own vector.

        Chunks were 240 tokens and only the first 128 were embedded, so the tail of every chunk was
        findable solely through a lexical index whose terms the retrieve path cannot consult.
        """
        self.assertGreaterEqual(parser.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS,
                                parser.DEFAULT_MAX_CHUNK_TOKENS,
                                "a chunk longer than the embedding window has a tail that never "
                                "reaches its vector")

    def test_the_window_is_per_model_not_a_constant(self):
        """512 is e5-large's limit, not a universal one."""
        self.assertEqual(512, parser.encoder_window_tokens("intfloat/multilingual-e5-large"))
        self.assertEqual(8192, parser.encoder_window_tokens("BAAI/bge-m3"))
        self.assertEqual(512, parser.encoder_window_tokens("something/nobody-has-heard-of"),
                         "an unknown encoder falls back to the conservative default")

    def test_the_environment_still_overrides_both(self):
        """Restores rather than rebuilds.

        A reload leaves a second module object behind, and whoever imported the first one keeps it.
        That is order-dependent, so the module attribute is set and put back instead.
        """
        for name, attr in (("MATRIXARK_RESOURCE_MAX_CHUNK_TOKENS", "DEFAULT_MAX_CHUNK_TOKENS"),
                           ("MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS",
                            "DEFAULT_EMBEDDING_TEXT_MAX_TOKENS")):
            previous_env = os.environ.get(name)
            previous_attr = getattr(parser, attr)
            os.environ[name] = "99"
            try:
                setattr(parser, attr, int(os.environ[name]))
                self.assertEqual(99, getattr(parser, attr), "%s no longer overrides" % name)
            finally:
                setattr(parser, attr, previous_attr)
                os.environ.pop(name, None)
                if previous_env is not None:
                    os.environ[name] = previous_env



class TheCeilingInUseFollowsAChangedEncoder(unittest.TestCase):
    """The live case, asked of the BUILDER rather than of a constant.

    `embedding.model` is labelled `live`, so an operator can move the encoder under a running
    worker. What matters is not which number a module attribute holds but how much text actually
    reaches the vector, so this builds the text and measures it.
    """

    PROBE = r"""
import json, os, sys
sys.path.insert(0, %r)
for name in ("MATRIXARK_EMBEDDING_MODEL", "MATRIXARK_EMBEDDING_MODEL_PATH",
             "MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS"):
    os.environ.pop(name, None)
os.environ["MATRIXARK_EMBEDDING_MODEL"] = "sentence-transformers/all-MiniLM-L6-v2"
import matrixark_resource_parser as rp
words = " ".join("alpha bravo charlie delta echo foxtrot golf hotel".split() * 4000)
meta = {"heading_path": ["Guide", "Section"], "relative_path": "doc.md"}
out = {"input_words": len(words.split()), "rows": []}
for model in ("sentence-transformers/all-MiniLM-L6-v2", "BAAI/bge-m3",
              "sentence-transformers/all-mpnet-base-v2"):
    os.environ["MATRIXARK_EMBEDDING_MODEL"] = model
    out["rows"].append({
        "model": model,
        "window": rp.encoder_window_tokens(),
        "built": len(rp.build_embedding_text(words, meta, "ref://doc").split()),
    })
print(json.dumps(out))
"""

    @classmethod
    def setUpClass(cls):
        import json
        import subprocess

        environ = dict(os.environ)
        for name in ("MATRIXARK_EMBEDDING_MODEL", "MATRIXARK_EMBEDDING_MODEL_PATH",
                     "MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS"):
            environ.pop(name, None)
        tools = os.path.dirname(os.path.abspath(__file__))
        result = subprocess.run([sys.executable, "-B", "-c", cls.PROBE % tools],
                                capture_output=True, text=True, cwd=tools, env=environ)
        if result.returncode != 0:
            raise AssertionError("probe failed:\n%s" % result.stderr[-800:])
        cls.out = json.loads(result.stdout.strip().splitlines()[-1])

    def test_every_row_was_built(self):
        """A probe that raised and was read for a key would report agreement it never measured."""
        self.assertEqual(3, len(self.out["rows"]))
        for row in self.out["rows"]:
            self.assertGreater(row["built"], 0, row)

    def test_the_input_is_larger_than_every_window(self):
        """Otherwise every row returns the whole input and they agree for the wrong reason."""
        for row in self.out["rows"]:
            self.assertGreater(self.out["input_words"], row["window"] * 2, row)

    def test_the_text_built_tracks_the_encoder(self):
        built = {row["model"]: row["built"] for row in self.out["rows"]}
        windows = {row["model"]: row["window"] for row in self.out["rows"]}
        self.assertEqual(
            len(set(windows.values())), len(set(built.values())),
            "the encoder changed under a running process and the text built for it did not: "
            "%r against windows %r. The ceiling in use was decided once, at import."
            % (built, windows))
        ordered = sorted(built, key=lambda m: windows[m])
        for smaller, larger in zip(ordered, ordered[1:]):
            self.assertLess(
                built[smaller], built[larger],
                "a %d-token encoder was fed at least as much as a %d-token one"
                % (windows[smaller], windows[larger]))

    def test_the_explicit_override_still_wins_over_the_encoder(self):
        """`MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS` is a deployment saying a number outright, and it
        outranks the window derived from the model name.

        Exercised through the RESOLVER, in a fresh process. `test_the_environment_still_overrides_both`
        below sets the module attribute and reads it back, which is a fact about `setattr`: it
        passed unchanged while a mutation that dropped the override from the resolver entirely
        went by without a word.
        """
        import json
        import subprocess

        probe = r"""
import json, os, sys
sys.path.insert(0, %r)
for name in ("MATRIXARK_EMBEDDING_MODEL", "MATRIXARK_EMBEDDING_MODEL_PATH",
             "MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS"):
    os.environ.pop(name, None)
os.environ["MATRIXARK_EMBEDDING_MODEL"] = "BAAI/bge-m3"
import matrixark_resource_parser as rp
out = {"window": rp.encoder_window_tokens(), "without": rp.embedding_text_max_tokens()}
os.environ["MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS"] = "77"
out["with"] = rp.embedding_text_max_tokens()
out["built"] = len(rp.build_embedding_text(
    " ".join(["alpha"] * 40000), {"relative_path": "d.md"}, "ref://d").split())
print(json.dumps(out))
""" % os.path.dirname(os.path.abspath(__file__))

        environ = dict(os.environ)
        for name in ("MATRIXARK_EMBEDDING_MODEL", "MATRIXARK_EMBEDDING_MODEL_PATH",
                     "MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS"):
            environ.pop(name, None)
        result = subprocess.run(
            [sys.executable, "-B", "-c", probe], capture_output=True, text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)), env=environ)
        self.assertEqual(0, result.returncode, result.stderr[-800:])
        out = json.loads(result.stdout.strip().splitlines()[-1])

        self.assertEqual(8192, out["window"], "the fixture encoder is the 8192-token one")
        self.assertEqual(8192, out["without"],
                         "with no override the ceiling is the encoder's window")
        self.assertEqual(77, out["with"],
                         "an explicit MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS must outrank the "
                         "window derived from the model name")
        self.assertLessEqual(out["built"], 77,
                             "the builder must apply the override, not only report it")

    def test_a_smaller_encoder_is_not_over_fed(self):
        """The failure this module names: text past the model's window is truncated by its own
        tokenizer and never reaches the vector."""
        for row in self.out["rows"]:
            with self.subTest(model=row["model"]):
                self.assertLessEqual(
                    row["built"], row["window"],
                    "%s reads %d tokens and was built %d words of text"
                    % (row["model"], row["window"], row["built"]))


if __name__ == "__main__":
    unittest.main()

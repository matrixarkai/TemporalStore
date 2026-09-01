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
"""
import importlib
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
        self.assertEqual(parser.encoder_window_tokens(),
                         parser.DEFAULT_EMBEDDING_TEXT_MAX_TOKENS)

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
        for name, attr in (("MATRIXARK_RESOURCE_MAX_CHUNK_TOKENS", "DEFAULT_MAX_CHUNK_TOKENS"),
                           ("MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS",
                            "DEFAULT_EMBEDDING_TEXT_MAX_TOKENS")):
            previous = os.environ.get(name)
            os.environ[name] = "99"
            try:
                reloaded = importlib.reload(parser)
                self.assertEqual(99, getattr(reloaded, attr), "%s no longer overrides" % name)
            finally:
                os.environ.pop(name, None)
                if previous is not None:
                    os.environ[name] = previous
                importlib.reload(parser)


if __name__ == "__main__":
    unittest.main()

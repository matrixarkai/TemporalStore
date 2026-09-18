#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The engine is sent a ranking policy, not None.

`_native_ranking_weights` exists so the engine cannot hold its own copy of the ranking policy and
drift from the local packer's. Its own docstring says the symptom of that drift is "worse answers
rather than an error".

It returned `None` on every retrieve. It imported `onebox_embedding_first` from
`matrixark_mcp_core`, which does not define or re-export it; `from X import name` where the module
loads but the name is absent raises **ImportError**, not AttributeError, so the handler written for
a different import shape swallowed it and the function fell out of its own bottom. `ranking_weights`
was null in every engine request, and the engine used its own copy of the policy -- exactly what the
function was written to prevent.

Nothing caught it because nothing tested it: before this file, no test in the tree so much as named
`ranking_weights`.

WHAT THIS ASSERTS, and why each part is here. That the function returns weights at all is the
finding. That the weights FOLLOW the flag is what makes it a policy rather than a constant -- a
function hardcoding the dense-only numbers would pass the first assertion and fail the second. And
that the flag is read from the module that defines it is the specific defect, checked directly, so
a future edit that points the import back at a module without the name fails here rather than
silently returning None again.
"""
from __future__ import annotations

import contextlib
import os
import sys
import unittest
from unittest import mock

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

# Import order matters: matrixark_temporal_direct_read and matrixark_mcp_temporal_adapters import
# each other, so reaching the read module first raises on a partially initialised module.
import matrixark_mcp_temporal_adapters  # noqa: E402,F401
import matrixark_temporal_direct_read as direct_read  # noqa: E402
import matrixark_local_adapter_retrieval as retrieval  # noqa: E402

#: The policy the local packer uses, which is the one the engine must be handed.
DENSE_ONLY = {"dense": 1.00, "sparse": 0.00, "index_hint": 0.08}
MIXED = {"dense": 0.72, "sparse": 0.28, "index_hint": 0.08}


def _weights():
    """The method ignores `self`; calling it unbound keeps the test off the adapter's setup."""
    return direct_read._TemporalDirectReadMixin._native_ranking_weights(None)


@contextlib.contextmanager
def _flag_returns(value):
    """Replace the flag on EVERY module object that spells it.

    `tools.matrixark_local_adapter_retrieval` and `matrixark_local_adapter_retrieval` are two
    different module objects with their own attributes, and which one the function under test
    binds depends on whether the repository root happens to be on `sys.path` -- which depends on
    what else ran first in the same process. Patching one of them passed this file when it ran
    alone and failed it when it ran beside modules that put the root on the path. Patching both
    is not belt-and-braces; it is the only version that tests the same thing either way.
    """
    names = ("matrixark_local_adapter_retrieval", "tools.matrixark_local_adapter_retrieval")
    loaded = [sys.modules[name] for name in names if name in sys.modules]
    if not loaded:  # pragma: no cover - the import at the top of this file guarantees one
        raise AssertionError("no module holding the flag is loaded, so this patch would be inert")
    with contextlib.ExitStack() as stack:
        for module in loaded:
            stack.enter_context(mock.patch.object(module, "onebox_embedding_first", value))
        yield


class TheEngineIsSentTheRankingPolicy(unittest.TestCase):

    def test_the_module_it_reads_the_flag_from_actually_has_it(self) -> None:
        """The defect itself, named. `matrixark_mcp_core` never had this."""
        self.assertTrue(
            hasattr(retrieval, "onebox_embedding_first"),
            "matrixark_local_adapter_retrieval no longer defines onebox_embedding_first, so the "
            "ranking policy has nowhere to come from and every retrieve is back to sending None",
        )

    def test_weights_are_sent_at_all(self) -> None:
        weights = _weights()
        self.assertIsNotNone(
            weights,
            "the engine is handed no ranking policy, so it falls back to its own copy -- the "
            "divergence this function exists to prevent",
        )
        self.assertIn(weights, (DENSE_ONLY, MIXED), "unrecognised policy: %r" % (weights,))

    def test_the_weights_follow_the_flag(self) -> None:
        """A function hardcoding one of the two answers passes the test above and fails this."""
        for dense_only, expected in ((True, DENSE_ONLY), (False, MIXED)):
            with self.subTest(embedding_first=dense_only):
                with _flag_returns(lambda: dense_only):
                    self.assertEqual(expected, _weights())

    def test_a_flag_that_raises_does_not_fail_the_retrieve(self) -> None:
        """The existing contract, kept: ranking policy must never turn a retrieve into an error."""
        def boom():
            raise RuntimeError("the flag is unavailable")

        with _flag_returns(boom):
            self.assertIsNone(_weights())

    def test_the_request_carries_the_field(self) -> None:
        """A floor. If the request stops naming `ranking_weights`, the weights go nowhere and
        every assertion above is about a value nobody reads."""
        path = os.path.join(TOOLS, "matrixark_temporal_direct_read.py")
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn(
            '"ranking_weights": self._native_ranking_weights()',
            source,
            "the engine request no longer sends the weights this file is about",
        )


if __name__ == "__main__":
    unittest.main()

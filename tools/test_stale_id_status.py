#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A stale memory id is the caller's state to fix, so it answers 404 rather than 500.

Updating a memory twice by its ORIGINAL id is the ordinary way to reach this: the first update
supersedes that id and returns a new one, so the second call addresses something that no longer
names a live memory. That raised a plain adapter error, which the edge classifies as 500 -- and a
500 tells the caller the server failed, inviting a retry or a page, when nothing is wrong with it.

The subtle part is WHICH error class. There are two unrelated `MatrixArkError` definitions in the
tree; the not-found type has to subclass the one the adapters actually raise and catch, or
`except MatrixArkError` on that path stops catching it and the raise site cannot even resolve the
name. That is what these tests pin, alongside the status mapping itself.
"""
import unittest

try:
    from tools import matrixark_mcp_local_adapter as adapter_mod
    from tools import matrixark_v1_gateway as gateway
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as adapter_mod
    import matrixark_v1_gateway as gateway


class NotFoundErrorTypeTests(unittest.TestCase):
    def test_the_raise_site_can_resolve_it(self):
        """It reaches the adapter through the same star import as MatrixArkError."""
        self.assertTrue(hasattr(adapter_mod, "MatrixArkNotFoundError"))

    def test_it_subclasses_the_error_the_adapters_use(self):
        """Not the identically-named class in matrixark_mcp_errors -- that one is a different type,
        and an `except MatrixArkError` on this path would let it through uncaught."""
        self.assertTrue(issubclass(adapter_mod.MatrixArkNotFoundError, adapter_mod.MatrixArkError))

    def test_existing_handlers_still_catch_it(self):
        try:
            raise adapter_mod.MatrixArkNotFoundError("gone")
        except adapter_mod.MatrixArkError as exc:
            self.assertEqual("gone", str(exc))
        else:  # pragma: no cover - the assertion above either runs or the test fails
            self.fail("MatrixArkError did not catch MatrixArkNotFoundError")


class StatusClassificationTests(unittest.TestCase):
    def test_a_not_found_error_maps_to_404(self):
        self.assertEqual(404, gateway._classify_backend_error(
            adapter_mod.MatrixArkNotFoundError("update target memory not found")))

    def test_an_ordinary_backend_error_still_maps_to_500(self):
        """The point is to distinguish the two, so a real fault must not become a 404."""
        self.assertEqual(500, gateway._classify_backend_error(
            adapter_mod.MatrixArkError("the backend fell over")))

    def test_a_message_containing_not_found_is_not_enough(self):
        """Classification is by type, not by words in the message."""
        self.assertEqual(500, gateway._classify_backend_error(
            adapter_mod.MatrixArkError("shard not found on the datanode")))

    def test_backpressure_still_maps_to_429(self):
        class MatrixArkBackpressureError(adapter_mod.MatrixArkError):
            pass

        self.assertEqual(429, gateway._classify_backend_error(
            MatrixArkBackpressureError("busy")))


if __name__ == "__main__":
    unittest.main()

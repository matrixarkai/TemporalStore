# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A scan the backend REFUSED is not a reason to read every record there is.

`_scan_records_of_types` returns `None` for two different things, and the difference decides
whether the full read that follows can work at all:

* no scanner on this client -- the question could not be put. The full read is the whole answer and
  is unrelated to the backend's health.
* the scan FAILED. With no Python hot cache (off by design on a native backend) and no disk
  fallback store, `read_all()` goes back to the same client that just refused, so it is guaranteed
  to fail too. `_read_all_after_scoped_scan` is where that is decided: it counts the fallback and
  re-raises the scan's own error instead of reading everything to find out.

Six readers inside `matrixark_mcp_temporal_adapters` ask it. The two that ask the same scan from
outside that module did not -- `_commit_records_of_types` on the live session-commit path, and
`_embedding_pass_records` on the embedding pass. Measured on one refused scan, same adapter class,
same client, `read_all` going back to it: the six readers attempted 0 whole-store reads and raised
`backend refused the scan`; the commit path attempted 1 and raised `backend refused the full read`,
naming the wrong operation, and counted nowhere. `session_commit` calls it up to twice.

The failure this guards is quiet in both directions. Reading everything against a dead backend is
invisible from outside -- a scan path that has stopped working looks exactly like one that was
never taken, and the cost shows up only as a store that got slower. And re-raising where the full
read COULD have answered would turn a served request into an error, which is why
`_full_read_can_answer_offline` treats "unsure" as "yes" and why the tests below pin the
could-not-ask case as unchanged.
"""

import ast
import os
import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

import matrixark_mcp_temporal_adapters as adapters  # noqa: E402  (adapters first: import cycle)
import matrixark_mcp_local_adapter as local_adapter  # noqa: E402

WANTED = ["context_batch_commit", "context_session_boundary"]


class _RefusingClient:
    """A backend that refuses every scan."""

    def __init__(self):
        self.scan_calls = 0
        self.read_calls = 0

    def matrixark_scan_candidates(self, **kwargs):
        self.scan_calls += 1
        raise ConnectionRefusedError("backend refused the scan")


class _SilentClient:
    """A client with no scanner at all: the question cannot be put, and nothing has failed."""

    def __init__(self):
        self.read_calls = 0


def _adapter(client, *, read_all_works=False):
    """A real backend adapter object, without standing up a backend.

    `object.__new__` on purpose: the methods under test are the real ones, and the four attributes
    the scan reads are set here. A class that grows a fifth requirement fails this loudly rather
    than quietly testing something else.
    """
    adapter = object.__new__(adapters.MatrixArkTemporalStoreRustAdapter)
    adapter._client = client
    adapter._count_key = "count"
    adapter._record_hash_key = "hash"
    adapter._shard_size = 1024
    adapter._records_cache = None
    adapter.python_hot_cache_enabled = lambda: False

    def read_all(_self=adapter):
        _self._client.read_calls += 1
        if read_all_works:
            return [{"record_type": "context_batch_commit", "scope": {}, "source_event_ids": [1]}]
        # what a full read does with nothing offline to serve it: back to the same client
        raise ConnectionRefusedError("backend refused the full read")

    adapter.read_all = read_all
    adapter.EMBEDDING_PASS_RECORD_TYPES = ("context_embedding", "context_event")
    return adapter


HELPER = "_read_all_after_scoped_scan"


def _reaches_the_helper(node: ast.AST) -> bool:
    """Does this function actually REACH the helper -- as an attribute or through `getattr`?

    Prose does not count. A revert that leaves the explanation behind still reads as a mention.
    """
    for inner in ast.walk(node):
        if isinstance(inner, ast.Attribute) and inner.attr == HELPER:
            return True
        if (isinstance(inner, ast.Call) and isinstance(inner.func, ast.Name)
                and inner.func.id == "getattr"
                and any(isinstance(arg, ast.Constant) and arg.value == HELPER
                        for arg in inner.args)):
            return True
    return False


class _Fresh:
    """Reset the process-wide fallback counter and the thread-local scan error around a case."""

    def __enter__(self):
        adapters._FULL_READ_FALLBACKS.clear()
        adapters._SCAN_STATE.last_error = None
        return self

    def __exit__(self, *exc):
        adapters._FULL_READ_FALLBACKS.clear()
        adapters._SCAN_STATE.last_error = None
        return False


class ARefusedScanIsNotAReasonToReadEverythingTest(unittest.TestCase):

    # -- the refused scan -----------------------------------------------------------------------

    def _refused(self, call, label):
        with _Fresh():
            client = _RefusingClient()
            adapter = _adapter(client)
            with self.assertRaises(ConnectionRefusedError) as raised:
                call(adapter)
            self.assertEqual(1, client.scan_calls,
                             "the scan was never attempted, so nothing about a refusal is proven")
            self.assertEqual(
                0, client.read_calls,
                f"{label} read the whole store against a backend that had just refused it")
            self.assertIn("refused the scan", str(raised.exception),
                          "the error names the full read rather than the scan that actually failed")
            self.assertEqual({label: 1}, dict(adapters._FULL_READ_FALLBACKS),
                             "the fallback was not counted under the name of the reader that took it")

    def test_the_commit_read_does_not_read_everything_after_a_refused_scan(self):
        self._refused(lambda a: a._commit_records_of_types(list(WANTED)),
                      "_commit_records_of_types")

    def test_the_embedding_pass_does_not_read_everything_after_a_refused_scan(self):
        self._refused(lambda a: a._embedding_pass_records(), "_embedding_pass_records")

    def test_the_six_readers_in_the_adapter_module_already_behaved_this_way(self):
        """The control. If this stops holding, the two above are matching the wrong thing."""
        with _Fresh():
            client = _RefusingClient()
            adapter = _adapter(client)
            self.assertIsNone(adapter._scan_records_of_types(["context_batch_commit"]),
                              "the scan did not fail, so the control proves nothing")
            self.assertIsNotNone(getattr(adapters._SCAN_STATE, "last_error", None),
                                 "a refused scan recorded no error, so nothing can act on it")
            with self.assertRaises(ConnectionRefusedError):
                adapter._read_all_after_scoped_scan()
            self.assertEqual(0, client.read_calls)

    # -- the case that must NOT change ------------------------------------------------------------

    def _could_not_ask(self, call, label):
        """No scanner on the client: nothing failed, and the full read is the whole answer."""
        with _Fresh():
            client = _SilentClient()
            adapter = _adapter(client, read_all_works=True)
            records = call(adapter)
            self.assertEqual(1, client.read_calls,
                             f"{label} did not fall back to the full read when it could not ask")
            self.assertTrue(records, "the full read returned nothing, so nothing is proven")
            self.assertIsNone(getattr(adapters._SCAN_STATE, "last_error", None),
                              "a question that could not be put must not look like a failure")

    def test_a_scan_that_cannot_be_asked_still_reads_everything_on_the_commit_path(self):
        self._could_not_ask(lambda a: a._commit_records_of_types(list(WANTED)),
                            "_commit_records_of_types")

    def test_a_scan_that_cannot_be_asked_still_reads_everything_on_the_embedding_pass(self):
        self._could_not_ask(lambda a: a._embedding_pass_records(), "_embedding_pass_records")

    def test_the_plain_local_adapter_has_no_scanner_and_no_helper(self):
        """Why both call sites ask for the helper instead of assuming it. The plain local adapter
        has neither, never reaches the refused-scan branch, and keeps the plain full read."""
        self.assertFalse(hasattr(local_adapter.MatrixArkLocalAdapter, "_scan_records_of_types"))
        self.assertFalse(hasattr(local_adapter.MatrixArkLocalAdapter, "_read_all_after_scoped_scan"))

    # -- nobody else gets to skip it ---------------------------------------------------------------

    def test_every_reader_that_asks_the_scan_and_falls_back_routes_through_the_helper(self):
        """Derived from the tree, not from a list kept here.

        A reader is in scope when it mentions `_scan_records_of_types` AND calls `self.read_all()`
        -- the two together are what "ask the scan, otherwise read everything" looks like, however
        the scanner is bound. The ones that merely return `None` to their own caller are not in
        scope and are reported so the selection can be read.

        "Routes through the helper" is decided on the syntax tree, not on the text. Reverting a
        call site while leaving the sentence that explains it in the docstring is exactly what a
        revert looks like, and a substring check passed on both of the two real reverts.
        """
        asks, skips = [], []
        files = sorted(path for path in TOOLS.glob("*.py") if not path.name.startswith("test_"))
        for path in files:
            source = path.read_text(encoding="utf-8", errors="replace")
            if "_scan_records_of_types" not in source:
                continue
            lines = source.splitlines()
            for node in ast.walk(ast.parse(source)):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if node.name == "_scan_records_of_types":
                    continue
                body = "\n".join(lines[node.lineno - 1:getattr(node, "end_lineno", node.lineno)])
                if "_scan_records_of_types" not in body:
                    continue
                reads_all = any(
                    isinstance(inner, ast.Call)
                    and isinstance(inner.func, ast.Attribute) and inner.func.attr == "read_all"
                    and isinstance(inner.func.value, ast.Name) and inner.func.value.id == "self"
                    for inner in ast.walk(node)
                )
                where = f"{path.name}:{node.lineno} {node.name}"
                if not reads_all:
                    skips.append(where)
                elif _reaches_the_helper(node):
                    asks.append(where)
                else:
                    self.fail(f"{where} falls back to a full read after a scan that could not "
                              f"answer, without asking whether that read can work. Route it "
                              f"through _read_all_after_scoped_scan.")
        self.assertGreaterEqual(
            len(asks), 8,
            "found only %d readers that ask the scan and fall back to a full read (%s); the "
            "selection stopped matching, so this passed by looking at nothing. Not in scope: %s"
            % (len(asks), asks, skips))
        self.assertGreaterEqual(len(files), 200, "the module listing collapsed")


if __name__ == "__main__":
    unittest.main()

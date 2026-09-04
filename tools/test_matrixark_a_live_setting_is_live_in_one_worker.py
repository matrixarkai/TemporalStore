#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A live setting is live in the worker that was written to, and no other.

``applies: live`` means the value is read from ``os.environ`` inside the function that needs it,
per call::

    matrixark_mcp_embeddings.py:151  provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", ...)

``update()`` writes ``os.environ`` and persists the file, so the worker that served the POST is
correct at once. The gateway documents ``--workers 4`` and supports it wherever the workers share a
store — and those other workers never see the write. They read their own environment, which still
holds what ``apply_boot`` gave them at startup, until they are restarted.

So a customer changes their embedding provider and one request in four uses it. Retrieval quality
diverges by worker with nothing in any log saying why, and the page said *"Saved and live now."*

This does not make the other workers pick it up — that wants a deliberate refresh path and its own
review. It stops the page claiming something true of one process out of several.

The worker count is lifted out of the split-store warning, which already reads it from the command
line and ``WEB_CONCURRENCY``, so there is one answer to the question rather than two that can
drift. That warning's own behaviour is asserted here too, because the refactor could have changed
it silently.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import unittest

import matrixark_v1_gateway as gw

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "worker_reach_harness.js")
PAGE = os.path.join(PORTAL, "setup_portal.html")


class TheWorkerCountIsReadOnceTest(unittest.TestCase):

    def test_nothing_said_means_one(self) -> None:
        """The default has to be 1, not 0 or unknown: every caveat below hangs off "more than
        one", and a zero would either silence it everywhere or fire it everywhere."""
        self.assertEqual(1, gw._worker_count([], {}))

    def test_the_command_line_forms(self) -> None:
        self.assertEqual(4, gw._worker_count(["--workers", "4"], {}))
        self.assertEqual(3, gw._worker_count(["--workers=3"], {}))

    def test_the_environment_form(self) -> None:
        self.assertEqual(2, gw._worker_count([], {"WEB_CONCURRENCY": "2"}))

    def test_the_command_line_wins(self) -> None:
        self.assertEqual(4, gw._worker_count(["--workers", "4"], {"WEB_CONCURRENCY": "9"}))

    def test_nonsense_is_one_rather_than_a_crash(self) -> None:
        self.assertEqual(1, gw._worker_count(["--workers", "many"], {}))
        self.assertEqual(1, gw._worker_count([], {"WEB_CONCURRENCY": "lots"}))


class TheSplitStoreWarningStillBehavesTest(unittest.TestCase):
    """It gave up its own copy of the counting; it must not have given up anything else."""

    def test_it_fires_on_several_workers_with_an_embedded_store(self) -> None:
        self.assertIsNotNone(gw._single_writer_warning(["--workers", "4"], {}))

    def test_it_stays_quiet_on_one_worker(self) -> None:
        self.assertIsNone(gw._single_writer_warning(["--workers", "1"], {}))

    def test_it_stays_quiet_when_the_workers_share_a_store(self) -> None:
        self.assertIsNone(
            gw._single_writer_warning(["--workers", "4"], {"TS_STORAGE_BACKEND": "shared"}))

    def test_it_still_names_the_count(self) -> None:
        self.assertIn("--workers 4", gw._single_writer_warning(["--workers", "4"], {}))


@unittest.skipUnless(shutil.which("node"), "node is not installed; the page JS cannot be run")
class ThePageSaysHowFarTheWriteReachedTest(unittest.TestCase):

    def _run(self, workers):
        return subprocess.run(["node", HARNESS, PAGE, str(workers)],
                              capture_output=True, text=True, timeout=180)

    def test_a_save_still_reports_itself(self) -> None:
        proc = self._run(4)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertIn("ok   it says the write was saved", proc.stdout)

    def test_several_workers_are_described(self) -> None:
        out = self._run(4).stdout
        self.assertIn("ok   it says this worker has it", out)
        self.assertIn("ok   it counts the others", out)
        self.assertIn("ok   it says what makes them agree", out)

    def test_one_worker_hears_nothing_about_workers(self) -> None:
        """A deployment with a single worker has no caveat, and inventing one teaches the reader
        to skip the sentence on the deployments that do."""
        self.assertIn("ok   a single worker is not told about other workers",
                      self._run(1).stdout)


if __name__ == "__main__":
    unittest.main()

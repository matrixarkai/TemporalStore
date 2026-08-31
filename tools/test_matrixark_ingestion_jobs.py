# SPDX-License-Identifier: Apache-2.0
"""Ingestion job path safety and progress accounting.

The path tests carry the weight here. This endpoint ingests server-side files the caller names, so
without a boundary it is a file-disclosure endpoint: "ingest /etc" would be a valid request. Every
resolved path must sit inside ``MATRIXARK_INGESTION_ROOT``, traversal and symlinks included, and an
unset root must refuse rather than default to the filesystem root.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_ingestion_jobs as jobs  # noqa: E402


class PathSafetyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = os.environ.get("MATRIXARK_INGESTION_ROOT")
        self.root = tempfile.mkdtemp(prefix="ingest-root-")
        self.outside = tempfile.mkdtemp(prefix="ingest-outside-")
        for name in ("a.md", "b.json"):
            with open(os.path.join(self.root, name), "w", encoding="utf-8") as handle:
                handle.write("# doc")
        with open(os.path.join(self.outside, "secret.md"), "w", encoding="utf-8") as handle:
            handle.write("# secret")
        os.environ["MATRIXARK_INGESTION_ROOT"] = self.root

    def tearDown(self) -> None:
        if self._saved is None:
            os.environ.pop("MATRIXARK_INGESTION_ROOT", None)
        else:
            os.environ["MATRIXARK_INGESTION_ROOT"] = self._saved

    def test_documents_inside_the_root_resolve(self) -> None:
        found = jobs.resolve_request_paths(directory=self.root)
        self.assertEqual(sorted(os.path.basename(p) for p in found), ["a.md", "b.json"])

    def test_a_path_outside_the_root_is_refused(self) -> None:
        with self.assertRaises(jobs.PathOutsideRoot):
            jobs.resolve_request_paths(paths=[os.path.join(self.outside, "secret.md")])

    def test_traversal_out_of_the_root_is_refused(self) -> None:
        with self.assertRaises(jobs.PathOutsideRoot):
            jobs.resolve_request_paths(directory=os.path.join(self.root, "..", "..", "etc"))

    def test_a_symlink_pointing_outside_the_root_is_refused(self) -> None:
        link = os.path.join(self.root, "escape.md")
        try:
            os.symlink(os.path.join(self.outside, "secret.md"), link)
        except (OSError, NotImplementedError):  # pragma: no cover - platforms without symlinks
            self.skipTest("symlinks unavailable")
        with self.assertRaises(jobs.PathOutsideRoot):
            jobs.resolve_request_paths(paths=[link])

    def test_an_unset_root_refuses_rather_than_defaulting_to_the_filesystem(self) -> None:
        os.environ.pop("MATRIXARK_INGESTION_ROOT", None)
        with self.assertRaises(jobs.IngestionRootNotConfigured):
            jobs.resolve_request_paths(paths=["/etc/passwd"])

    def test_duplicates_are_removed_and_order_preserved(self) -> None:
        a = os.path.join(self.root, "a.md")
        found = jobs.resolve_request_paths(paths=[a, a, os.path.join(self.root, "b.json")])
        self.assertEqual([os.path.basename(p) for p in found], ["a.md", "b.json"])


class ProgressTest(unittest.TestCase):
    def test_a_fresh_job_reports_everything_remaining(self) -> None:
        job = jobs.Job("j1", ["/x/a.md", "/x/b.md"], {"user_id": "u"})
        snap = job.snapshot()
        self.assertEqual((snap["total"], snap["done"], snap["remaining"]), (2, 0, 2))
        self.assertEqual(snap["state"], "queued")

    def test_counters_and_eta_move_with_progress(self) -> None:
        job = jobs.Job("j2", ["/x/a.md", "/x/b.md", "/x/c.md", "/x/d.md"], {})
        job.done, job.failed, job.bytes = 2, 1, 4096
        snap = job.snapshot()
        self.assertEqual((snap["done"], snap["failed"], snap["remaining"]), (2, 1, 1))
        self.assertEqual(snap["bytes"], 4096)

    def test_prometheus_text_is_valid_exposition_format(self) -> None:
        text = jobs.prometheus_text()
        lines = [l for l in text.split("\n") if l and not l.startswith("#")]
        self.assertTrue(lines, "expected at least one sample line")
        for line in lines:
            name, _, value = line.partition(" ")
            self.assertTrue(name.startswith("matrixark_ingestion_"), name)
            float(value)  # every sample must parse as a number
        # HELP/TYPE must accompany each metric family.
        self.assertIn("# HELP matrixark_ingestion_documents_done", text)
        self.assertIn("# TYPE matrixark_ingestion_documents_done counter", text)

    def test_the_registry_is_bounded(self) -> None:
        registry = jobs.JobRegistry()
        for index in range(jobs.MAX_RETAINED_JOBS + 10):
            job = jobs.Job("job%d" % index, [], {})
            with registry._lock:  # submit() would start threads; exercise retention directly
                registry._jobs[job.id] = job
                registry._order.insert(0, job.id)
                while len(registry._order) > jobs.MAX_RETAINED_JOBS:
                    registry._jobs.pop(registry._order.pop(), None)
        self.assertEqual(len(registry.list()), jobs.MAX_RETAINED_JOBS)


if __name__ == "__main__":
    unittest.main()

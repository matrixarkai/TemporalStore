#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Ingestion jobs: what failed, whether it is worth retrying, and retrying only that.

A thousand-document import that fails three times leaves an operator with a choice, and the
tempting one is to re-run the whole directory -- ingest is a keyed upsert, so it is safe, and it
takes as long the second time and ends with the same three failures. Retrying only the failures
worth retrying is the difference; classifying them is what makes it more than a shortcut.
"""
from __future__ import annotations

import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_ingestion_jobs as jobs  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg, drive  # noqa: E402

ADMIN = {"Authorization": "Bearer k-acme"}


class FailureClassificationTest(unittest.TestCase):
    def test_a_4xx_is_not_worth_retrying(self) -> None:
        # post_document already gives up immediately on a 4xx: the request itself is wrong, so the
        # retry sends the same wrong request.
        for detail in ("http 400", "http 401", "http 403", "http 413", "HTTP 422"):
            with self.subTest(detail=detail):
                self.assertFalse(jobs.classify_failure(detail))

    def test_a_timeout_or_a_5xx_is(self) -> None:
        for detail in ("http 500", "http 502", "http 503", "TimeoutError: timed out",
                       "ConnectionRefusedError: [Errno 111]", "http 429"):
            with self.subTest(detail=detail):
                self.assertTrue(jobs.classify_failure(detail))

    def test_a_file_that_would_not_open_is_not(self) -> None:
        self.assertFalse(jobs.classify_failure("read failed: [Errno 2] No such file"))

    def test_an_unparseable_detail_errs_towards_retrying(self) -> None:
        # Refusing to retry on a message nobody anticipated strands work; retrying one document
        # that will fail again costs a request.
        self.assertTrue(jobs.classify_failure(""))
        self.assertTrue(jobs.classify_failure("http not-a-number"))


class _StubJob(jobs.Job):
    """A job whose documents resolve from a script instead of an HTTP call."""

    def __init__(self, results):
        super().__init__("stub", [p for p, _ in results], {})
        self._results = dict(results)

    def run(self) -> None:
        # Goes through Job.record, so what the tests below measure is the product's own bookkeeping
        # rather than a second copy of it in the stub.
        self.state = "running"
        for path in self.paths:
            ok, detail = self._results[path]
            self.record(path, ok, 1.0, 0, detail)
        self.state = "failed" if self.failed and not self.done else "completed"
        self.finished_at = time.time()


class FailedPathsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.job = _StubJob([
            ("/docs/a.md", (True, "http 200")),
            ("/docs/b.md", (False, "http 400")),      # malformed: not worth retrying
            ("/docs/c.md", (False, "http 503")),      # the deployment had a bad minute
            ("/docs/d.md", (False, "TimeoutError")),  # ditto
        ])
        self.job.run()

    def test_only_the_retryable_failures_by_default(self) -> None:
        self.assertEqual(["/docs/c.md", "/docs/d.md"], self.job.failed_paths())

    def test_everything_that_failed_when_asked(self) -> None:
        self.assertEqual(["/docs/b.md", "/docs/c.md", "/docs/d.md"],
                         self.job.failed_paths(only_retryable=False))

    def test_the_snapshot_separates_the_two_counts(self) -> None:
        snap = self.job.snapshot()
        self.assertEqual(3, snap["failed"])
        self.assertEqual(3, snap["failure_count"])
        self.assertEqual(2, snap["retryable_failures"])
        self.assertFalse(snap["failures_truncated"])

    def test_the_recent_ring_shows_what_it_has_been_chewing_through(self) -> None:
        recent = self.job.snapshot()["recent"]
        self.assertEqual(4, len(recent))
        self.assertEqual("/docs/a.md", recent[0]["path"])
        self.assertTrue(recent[0]["ok"])

    def test_the_recent_ring_is_bounded(self) -> None:
        job = _StubJob([("/docs/%d.md" % i, (True, "http 200"))
                        for i in range(jobs.RECENT_KEPT + 10)])
        job.run()
        self.assertEqual(jobs.RECENT_KEPT, len(job.snapshot()["recent"]))
        # It keeps the NEWEST, which is the half that says where the import has got to.
        self.assertEqual("/docs/%d.md" % (jobs.RECENT_KEPT + 9),
                         job.snapshot()["recent"][-1]["path"])


class RetryRouteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = gw.make_v1_app(_FakeServer(), _cfg())
        self._saved = dict(jobs.REGISTRY._jobs), list(jobs.REGISTRY._order)  # noqa: SLF001
        jobs.REGISTRY._jobs = {}      # noqa: SLF001
        jobs.REGISTRY._order = []     # noqa: SLF001

    def tearDown(self) -> None:
        jobs.REGISTRY._jobs, jobs.REGISTRY._order = self._saved  # noqa: SLF001

    def _register(self, job) -> None:
        jobs.REGISTRY._jobs[job.id] = job     # noqa: SLF001
        jobs.REGISTRY._order.insert(0, job.id)  # noqa: SLF001

    def test_retry_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="POST",
                             path="/v1/admin/ingestion/jobs/stub/retry", body={})
        self.assertEqual(401, status)

    def test_an_unknown_job_is_a_404(self) -> None:
        status, _, body = drive(self.app, method="POST",
                                path="/v1/admin/ingestion/jobs/nope/retry",
                                headers=ADMIN, body={})
        self.assertEqual(404, status)
        self.assertEqual("unknown_job", json.loads(body)["error"])

    def test_a_job_with_no_retryable_failures_is_refused_with_the_reason(self) -> None:
        job = _StubJob([("/docs/b.md", (False, "http 400"))])
        job.run()
        self._register(job)
        status, _, body = drive(self.app, method="POST",
                                path="/v1/admin/ingestion/jobs/%s/retry" % job.id,
                                headers=ADMIN, body={})
        self.assertEqual(400, status)
        payload = json.loads(body)
        self.assertEqual("nothing_to_retry", payload["error"])
        self.assertIn("only_retryable=false", payload["detail"])

    def test_a_running_job_is_a_409_rather_than_a_duplicate_submission(self) -> None:
        job = _StubJob([("/docs/a.md", (True, "http 200"))])
        job.state = "running"
        self._register(job)
        status, _, body = drive(self.app, method="POST",
                                path="/v1/admin/ingestion/jobs/%s/retry" % job.id,
                                headers=ADMIN, body={})
        self.assertEqual(409, status)
        self.assertEqual("job_still_running", json.loads(body)["error"])

    def test_a_retry_submits_only_the_failures_and_links_both_jobs(self) -> None:
        job = _StubJob([
            ("/docs/a.md", (True, "http 200")),
            ("/docs/b.md", (False, "http 400")),
            ("/docs/c.md", (False, "http 503")),
        ])
        job.run()
        self._register(job)
        submitted = {}

        def capture(paths, options):
            submitted["paths"] = list(paths)
            submitted["options"] = dict(options)
            child = _StubJob([(p, (True, "http 200")) for p in paths])
            child.id = "child"
            child.retry_of = options.get("retry_of")
            self._register(child)
            return child

        original = jobs.REGISTRY.submit
        jobs.REGISTRY.submit = capture  # type: ignore[assignment]
        try:
            status, _, body = drive(self.app, method="POST",
                                    path="/v1/admin/ingestion/jobs/%s/retry" % job.id,
                                    headers=ADMIN, body={})
        finally:
            jobs.REGISTRY.submit = original  # type: ignore[assignment]

        self.assertEqual(202, status)
        # Only the retryable failure: not the success, and not the 4xx that will fail identically.
        self.assertEqual(["/docs/c.md"], submitted["paths"])
        self.assertEqual(job.id, submitted["options"]["retry_of"])
        self.assertEqual(job.id, json.loads(body)["retry_of"])
        self.assertEqual("child", job.snapshot()["retried_by"])

    def test_only_retryable_false_resubmits_everything_that_failed(self) -> None:
        job = _StubJob([
            ("/docs/b.md", (False, "http 400")),
            ("/docs/c.md", (False, "http 503")),
        ])
        job.run()
        self._register(job)
        submitted = {}

        def capture(paths, options):
            submitted["paths"] = list(paths)
            child = _StubJob([(p, (True, "http 200")) for p in paths])
            child.id = "child2"
            self._register(child)
            return child

        original = jobs.REGISTRY.submit
        jobs.REGISTRY.submit = capture  # type: ignore[assignment]
        try:
            status, _, _ = drive(self.app, method="POST",
                                 path="/v1/admin/ingestion/jobs/%s/retry" % job.id,
                                 headers=ADMIN, body={"only_retryable": False})
        finally:
            jobs.REGISTRY.submit = original  # type: ignore[assignment]
        self.assertEqual(202, status)
        self.assertEqual(["/docs/b.md", "/docs/c.md"], submitted["paths"])


class ImportProgressTest(unittest.TestCase):
    """What the landing page says about imports.

    A running import and a pile of failures waiting for a retry are both states a customer would
    otherwise only find by opening the Ingestion page, which is not where anyone starts.
    """

    def setUp(self) -> None:
        self.app = gw.make_v1_app(_FakeServer(), _cfg())
        self._saved = dict(jobs.REGISTRY._jobs), list(jobs.REGISTRY._order)  # noqa: SLF001
        jobs.REGISTRY._jobs = {}      # noqa: SLF001
        jobs.REGISTRY._order = []     # noqa: SLF001

    def tearDown(self) -> None:
        jobs.REGISTRY._jobs, jobs.REGISTRY._order = self._saved  # noqa: SLF001

    def _add(self, job) -> None:
        jobs.REGISTRY._jobs[job.id] = job       # noqa: SLF001
        jobs.REGISTRY._order.insert(0, job.id)  # noqa: SLF001

    def test_an_empty_registry_reports_nothing_running(self) -> None:
        summary = gw._import_progress()
        self.assertEqual(0, summary["running"])
        self.assertEqual([], summary["active"])
        self.assertEqual(0, summary["retryable"])

    def test_a_running_job_is_reported_with_its_position(self) -> None:
        job = _StubJob([("/docs/%d.md" % i, (True, "http 200")) for i in range(10)])
        job.id = "live"
        job.run()
        job.state = "running"       # left mid-flight
        job.finished_at = None
        job.current = "/docs/7.md"
        self._add(job)
        summary = gw._import_progress()
        self.assertEqual(1, summary["running"])
        self.assertEqual("live", summary["active"][0]["job_id"])
        self.assertEqual("/docs/7.md", summary["active"][0]["current"])

    def test_the_eta_across_two_imports_is_the_one_that_finishes_last(self) -> None:
        # Summing them would claim an import takes as long as all of them together, which is only
        # true if they run one after another -- and they do not.
        for name, eta in (("a", 30.0), ("b", 90.0)):
            job = _StubJob([("/docs/%s.md" % name, (True, "http 200"))])
            job.id = name
            job.run()
            job.state = "running"
            job.finished_at = None
            job.snapshot = (lambda e: (lambda: dict(job_id=job.id, state="running", total=10,
                                                    done=1, failed=0, remaining=9, eta_s=e,
                                                    current=None, retryable_failures=0)))(eta)
            self._add(job)
        self.assertEqual(90.0, gw._import_progress()["eta_s"])

    def test_failures_waiting_for_a_retry_are_counted_across_finished_jobs_too(self) -> None:
        # The whole point is the import that ENDED with work outstanding.
        job = _StubJob([("/docs/c.md", (False, "http 503")),
                        ("/docs/b.md", (False, "http 400"))])
        job.run()
        self._add(job)
        summary = gw._import_progress()
        self.assertEqual(0, summary["running"])
        self.assertEqual(2, summary["documents_failed"])
        self.assertEqual(1, summary["retryable"])

    def test_the_overview_carries_the_summary_and_raises_a_check(self) -> None:
        job = _StubJob([("/docs/c.md", (False, "TimeoutError"))])
        job.run()
        self._add(job)
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/overview",
                              headers={"Authorization": "Bearer k-acme"})
        payload = json.loads(body)
        self.assertEqual(1, payload["imports"]["retryable"])
        check = {c["id"]: c for c in payload["checks"]}["import_retries"]
        self.assertEqual("warn", check["status"])
        self.assertTrue(check["how"])

    def test_no_check_when_there_is_nothing_waiting(self) -> None:
        # A checklist that lists a clean state as an item trains people to stop reading it.
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/overview",
                              headers={"Authorization": "Bearer k-acme"})
        self.assertNotIn("import_retries",
                         {c["id"] for c in json.loads(body)["checks"]})

    def test_a_registry_that_cannot_answer_does_not_take_the_page_down(self) -> None:
        broken = jobs.REGISTRY.list

        def explode():
            raise RuntimeError("registry unavailable")

        jobs.REGISTRY.list = explode  # type: ignore[assignment]
        try:
            summary = gw._import_progress()
        finally:
            jobs.REGISTRY.list = broken  # type: ignore[assignment]
        self.assertEqual(0, summary["running"])


class RetryMetricsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = dict(jobs.REGISTRY._jobs), list(jobs.REGISTRY._order)  # noqa: SLF001
        jobs.REGISTRY._jobs = {}      # noqa: SLF001
        jobs.REGISTRY._order = []     # noqa: SLF001

    def tearDown(self) -> None:
        jobs.REGISTRY._jobs, jobs.REGISTRY._order = self._saved  # noqa: SLF001

    def test_work_waiting_for_a_retry_is_visible_on_a_dashboard(self) -> None:
        # Non-zero after an import means somebody has to press retry; without the gauge that state
        # is only visible to whoever happens to open the page.
        job = _StubJob([("/docs/c.md", (False, "http 503")),
                        ("/docs/b.md", (False, "http 400"))])
        job.run()
        jobs.REGISTRY._jobs[job.id] = job       # noqa: SLF001
        jobs.REGISTRY._order.insert(0, job.id)  # noqa: SLF001
        text = jobs.prometheus_text()
        self.assertIn("matrixark_ingestion_documents_retryable 1", text)
        self.assertIn("matrixark_ingestion_documents_failed 2", text)


if __name__ == "__main__":
    unittest.main()

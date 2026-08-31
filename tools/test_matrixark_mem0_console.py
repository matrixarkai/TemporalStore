#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The mem0 console and the record batch.

The console's forms are generated from MEM0_OPERATIONS, so the table is the thing worth testing:
an operation naming a route that does not exist is a button that 404s, and a memory route no
operation reaches is a capability nothing in the portal can exercise.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_batch_ingest as batch  # noqa: E402
import matrixark_gateway_metrics as metricsmod  # noqa: E402
import matrixark_ingestion_jobs as jobs  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg, drive  # noqa: E402

ADMIN = {"Authorization": "Bearer k-acme"}
HERE = os.path.dirname(os.path.abspath(__file__))

FIELD_PLACES = {"body", "scope", "query", "path", "message"}
FIELD_KINDS = {"text", "textarea", "number", "bool", "json"}


def _read_text(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


class OperationTableTest(unittest.TestCase):
    def test_every_operation_names_a_route_that_exists(self) -> None:
        documented = {(r["method"], r["path"]) for r in gw.ROUTE_DOCS}
        for op in gw.MEM0_OPERATIONS:
            with self.subTest(op=op["id"]):
                self.assertIn((op["method"], op["path"]), documented,
                              "the console would call a route nothing documents")

    def test_every_memory_route_is_reachable_from_the_console(self) -> None:
        # The other direction. A memory route no operation reaches is one a customer cannot try
        # from the portal at all -- and the portal is where they look first.
        served = {r["path"] for r in gw.ROUTE_DOCS if r["group"] == "Memory"}
        reached = {op["path"] for op in gw.MEM0_OPERATIONS}
        missing = served - reached
        # /v1/ingest_file takes a raw body and a set of headers, which is a file upload, not a form
        # this console can build; it has its own pane on the same page.
        self.assertEqual({"/v1/ingest_file"}, missing | {"/v1/ingest_file"},
                         "memory routes the console cannot reach: %s" % sorted(missing))

    def test_the_scope_each_operation_declares_matches_the_route(self) -> None:
        # The console prints this next to the button. A scope that disagrees with what the gateway
        # enforces sends a customer to issue the wrong key and read a 403 that names a third thing.
        by_route = {(r["method"], r["path"]): r.get("scope") for r in gw.ROUTE_DOCS}
        for op in gw.MEM0_OPERATIONS:
            with self.subTest(op=op["id"]):
                self.assertEqual(by_route[(op["method"], op["path"])], op["scope"])

    def test_operation_ids_are_unique(self) -> None:
        ids = [op["id"] for op in gw.MEM0_OPERATIONS]
        self.assertEqual(sorted(set(ids)), sorted(ids))

    def test_every_field_is_well_formed(self) -> None:
        for op in gw.MEM0_OPERATIONS:
            for field in op["fields"]:
                with self.subTest(op=op["id"], field=field["name"]):
                    self.assertIn(field["in"], FIELD_PLACES)
                    self.assertIn(field["kind"], FIELD_KINDS)
                    self.assertTrue(field["label"].strip())

    def test_a_path_placeholder_always_has_a_field_to_fill_it(self) -> None:
        # Otherwise the console builds a URL containing a literal {id} and the gateway answers a
        # 404 that reads like the memory is missing.
        for op in gw.MEM0_OPERATIONS:
            if "{" not in op["path"]:
                continue
            names = {f["name"] for f in op["fields"] if f["in"] == "path"}
            for part in op["path"].split("{")[1:]:
                with self.subTest(op=op["id"]):
                    self.assertIn(part.split("}")[0], names)

    def test_every_destructive_operation_is_marked_and_every_forget_route_is_destructive(
            self) -> None:
        # The marking drives the confirm box. An unmarked delete is one click.
        forget_routes = {r["path"] for r in gw.ROUTE_DOCS if r.get("scope") == "context:forget"}
        for op in gw.MEM0_OPERATIONS:
            with self.subTest(op=op["id"]):
                self.assertEqual(op["path"] in forget_routes, bool(op["destructive"]),
                                 "destructive marking disagrees with the scope the route gates on")
        self.assertTrue(any(op["destructive"] for op in gw.MEM0_OPERATIONS))

    def test_a_get_operation_puts_nothing_in_a_body(self) -> None:
        # A GET with a body is silently ignored by the gateway, so a field placed there would look
        # filled in and do nothing.
        for op in gw.MEM0_OPERATIONS:
            if op["method"] != "GET":
                continue
            with self.subTest(op=op["id"]):
                self.assertFalse(op["needs_scope"])
                for field in op["fields"]:
                    self.assertIn(field["in"], ("query", "path"))


class ConsolePageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = gw.make_v1_app(_FakeServer(), _cfg())

    def test_the_explore_page_carries_the_console_and_every_operation(self) -> None:
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/explore")
        text = body.decode("utf-8")
        self.assertIn('id="ops"', text)
        self.assertIn('data-pane="api"', text)
        self.assertIn('data-pane="batch"', text)
        for op in gw.MEM0_OPERATIONS:
            with self.subTest(op=op["id"]):
                self.assertIn('"id": "%s"' % op["id"], text)

    def test_the_copyable_curl_names_the_key_rather_than_carrying_it(self) -> None:
        # This lands on a clipboard and often in a ticket.
        source = _read_text(os.path.join(HERE, "portal", "build_portal_pages.py"))
        start = source.index('if (ev.target.id === "opCurl")')
        block = source[start:source.index('if (ev.target.id === "opRun")', start)]
        self.assertIn("$MATRIXARK_API_KEY", block)
        self.assertNotIn('$("key").value', block)


class RecordBatchRouteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = gw.make_v1_app(_FakeServer(), _cfg())
        metricsmod.METRICS.__init__()  # type: ignore[misc]
        self._submitted = []
        self._original = jobs.REGISTRY.submit

        def _capture(items, options):
            self._submitted.append((list(items), dict(options)))
            job = jobs.Job("stub-batch", items, options)
            return job

        jobs.REGISTRY.submit = _capture  # type: ignore[assignment]

    def tearDown(self) -> None:
        jobs.REGISTRY.submit = self._original  # type: ignore[assignment]

    def post(self, payload, headers=ADMIN):
        status, _h, body = drive(self.app, method="POST",
                                 path="/v1/admin/ingestion/records",
                                 headers=headers, body=payload)
        try:
            return status, json.loads(body.decode("utf-8"))
        except ValueError:
            return status, {}

    def test_it_needs_a_key(self) -> None:
        status, _ = self.post({"records": [{"text": "hello"}]}, headers=None)
        self.assertEqual(401, status)
        self.assertEqual([], self._submitted)

    def test_an_empty_batch_is_refused_rather_than_started(self) -> None:
        # A job over nothing completes instantly and reads as a successful import.
        for payload in ({}, {"records": []}, {"records": "nope"}):
            with self.subTest(payload=payload):
                status, body = self.post(payload)
                self.assertEqual(400, status)
                self.assertEqual("no_records", body["error"])
        self.assertEqual([], self._submitted)

    def test_blank_records_are_dropped_and_counted_not_queued_to_fail(self) -> None:
        # A blank record fails identically every time, so queueing it would fill the failure list
        # with entries no retry can ever clear -- and a retryable-looking failure that never
        # succeeds is worse than one that was never attempted.
        status, body = self.post({"records": [{"text": "real"}, {"text": "   "}, {"text": ""}]})
        self.assertEqual(202, status)
        self.assertEqual(2, body["skipped"])
        self.assertEqual(1, body["total"])
        items, _options = self._submitted[0]
        self.assertEqual(1, len(items))

    def test_a_batch_of_nothing_but_blanks_is_refused(self) -> None:
        status, body = self.post({"records": [{"text": ""}, {"text": "  "}]})
        self.assertEqual(400, status)
        self.assertEqual("no_usable_records", body["error"])
        self.assertEqual([], self._submitted)

    def test_too_many_records_is_refused_with_the_limit_named(self) -> None:
        status, body = self.post({"records": [{"text": "x"}] * (gw.MAX_BATCH_RECORDS + 1)})
        self.assertEqual(400, status)
        self.assertEqual("too_many_records", body["error"])
        self.assertIn(str(gw.MAX_BATCH_RECORDS), body["detail"])

    def test_a_preview_ingests_nothing(self) -> None:
        status, body = self.post({"records": [{"text": "one"}, {"text": "two"}], "preview": True})
        self.assertEqual(200, status)
        self.assertEqual("preview", body["status"])
        self.assertEqual(2, body["total"])
        self.assertEqual([], self._submitted, "a preview started a job")

    def test_a_per_record_user_overrides_the_batch_user(self) -> None:
        # A pasted dump usually already says whose memory each line is. Filing them all under one
        # subject would be silent and unrecoverable without re-ingesting.
        status, _body = self.post({
            "records": [{"text": "mine"}, {"text": "hers", "user_id": "alice"}],
            "user_id": "default",
        })
        self.assertEqual(202, status)
        items, options = self._submitted[0]
        self.assertEqual("default", options["user_id"])
        users = [i["record"]["user_id"] for i in items]
        self.assertEqual(["default", "alice"], users)

    def test_metadata_and_identity_key_survive_to_the_job(self) -> None:
        status, _body = self.post({"records": [
            {"text": "renewal moved", "metadata": {"source": "crm"}, "identity_key": "renewal"},
        ]})
        self.assertEqual(202, status)
        items, _options = self._submitted[0]
        self.assertEqual({"source": "crm"}, items[0]["record"]["metadata"])
        self.assertEqual("renewal", items[0]["record"]["identity_key"])

    def test_the_route_is_labelled_in_the_metrics(self) -> None:
        # Without a template the path would be labelled by its literal self, which is fine here but
        # is the mechanism that lets an unbounded path explode the label set.
        self.post({"records": [{"text": "one"}]})
        self.assertEqual("/v1/admin/ingestion/records",
                         metricsmod.route_label("/v1/admin/ingestion/records"))
        self.assertIn("/v1/admin/ingestion/records",
                      "\n".join(metricsmod.METRICS.prometheus_lines()))


class RecordJobTest(unittest.TestCase):
    """The job itself, with the network stubbed."""

    def setUp(self) -> None:
        self._original = batch.post_record
        self.sent = []

        def _fake(base_url, record, *, user_id, api_key, timeout_s):
            self.sent.append((base_url, dict(record), user_id))
            text = str(record.get("text") or "")
            return ("boom" not in text), ("http 503" if "boom" in text else "http 200")

        batch.post_record = _fake  # type: ignore[assignment]

    def tearDown(self) -> None:
        batch.post_record = self._original  # type: ignore[assignment]

    def test_a_record_job_runs_every_record_and_counts_them(self) -> None:
        job = jobs.Job("j1", jobs.record_items([
            {"text": "one"}, {"text": "boom"}, {"text": "three"},
        ]), {"base_url": "http://x", "user_id": "default"})
        job.run()
        self.assertEqual(3, job.total)
        self.assertEqual(2, job.done)
        self.assertEqual(1, job.failed)
        self.assertEqual("records", job.snapshot()["source"])

    def test_a_records_job_has_no_paths_and_says_so(self) -> None:
        # Anything reading `paths` on a record job would otherwise see an empty list and conclude
        # the job was empty.
        job = jobs.Job("j2", jobs.record_items([{"text": "one"}]), {})
        self.assertEqual([], job.paths)
        self.assertEqual(1, job.total)

    def test_a_retry_of_a_record_job_re_sends_the_records(self) -> None:
        # The failure list keys by label, and a label cannot be turned back into a record. Without
        # the item carrying its own record, a retry would report success over nothing.
        job = jobs.Job("j3", jobs.record_items([{"text": "boom"}, {"text": "fine"}]),
                       {"base_url": "http://x"})
        job.run()
        self.assertEqual(1, job.failed)
        items = job.failed_items()
        self.assertEqual(1, len(items))
        self.assertEqual("boom", items[0]["record"]["text"])

    def test_failed_paths_stays_empty_for_a_record_job(self) -> None:
        # It answers "which files to re-read", and there are none. Returning labels there would
        # have the path retry try to open a file named "record 1: boom".
        job = jobs.Job("j4", jobs.record_items([{"text": "boom"}]), {})
        job.run()
        self.assertEqual([], job.failed_paths())
        self.assertEqual(1, len(job.failed_items()))

    def test_a_mixed_job_retries_each_kind_as_itself(self) -> None:
        original_doc = batch.post_document

        def _fake_doc(base_url, path, *, user_id, api_key, timeout_s, finalize=True):
            return False, "http 503"

        batch.post_document = _fake_doc  # type: ignore[assignment]
        try:
            job = jobs.Job("j5",
                           jobs.path_items(["/docs/a.md"]) + jobs.record_items([{"text": "boom"}]),
                           {})
            job.run()
        finally:
            batch.post_document = original_doc  # type: ignore[assignment]
        self.assertEqual(2, job.failed)
        kinds = sorted(i["kind"] for i in job.failed_items())
        self.assertEqual(["path", "record"], kinds)
        self.assertEqual(["/docs/a.md"], job.failed_paths())


class RecordBodyTest(unittest.TestCase):
    def test_a_record_is_sent_as_a_finalized_waited_turn(self) -> None:
        # Without both, the gateway acks the record as a deferred event and the batch reports a
        # success for something that is not retrievable yet -- which reads as data loss.
        body = batch.record_body({"text": "hello"}, "default")
        self.assertTrue(body["wait"])
        self.assertTrue(body["finalize"])
        self.assertEqual([{"role": "user", "content": "hello"}], body["messages"])

    def test_the_scope_carries_the_per_record_identity(self) -> None:
        body = batch.record_body(
            {"text": "hi", "user_id": "alice", "agent_id": "a1", "session_id": "s1"}, "default")
        self.assertEqual({"user_id": "alice", "agent_id": "a1", "session_id": "s1"}, body["scope"])
        self.assertEqual("alice", body["user_id"])

    def test_an_absent_agent_is_omitted_rather_than_sent_empty(self) -> None:
        # An empty string is a value: it would scope the memory to an agent named "".
        body = batch.record_body({"text": "hi", "agent_id": ""}, "default")
        self.assertEqual({"user_id": "default"}, body["scope"])

    def test_content_is_accepted_as_well_as_text(self) -> None:
        self.assertEqual([{"role": "user", "content": "hi"}],
                         batch.record_body({"content": "hi"}, "default")["messages"])

    def test_an_empty_record_is_refused_without_a_request(self) -> None:
        called = []
        original = batch.post_ingest_body
        batch.post_ingest_body = lambda *a, **k: called.append(1) or (True, "")  # type: ignore
        try:
            ok, detail = batch.post_record("http://x", {"text": "  "},
                                           user_id="u", api_key="", timeout_s=1.0)
        finally:
            batch.post_ingest_body = original  # type: ignore[assignment]
        self.assertFalse(ok)
        self.assertEqual("empty record", detail)
        self.assertEqual([], called)

    def test_an_empty_record_failure_is_not_retryable(self) -> None:
        # It would fail identically forever, and a retryable failure that never succeeds is how a
        # retry button becomes something nobody trusts.
        self.assertFalse(jobs.classify_failure("empty record"))


if __name__ == "__main__":
    unittest.main()

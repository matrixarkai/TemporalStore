#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The deployment chooser over HTTP: what it offers, what it refuses, and what it claims to know.

The claim that needs guarding is the narrow one. The portal can prove MatrixObject is compiled in --
a live backend of `matrixobject` says so -- but it can never prove the opposite, because auto picks
another backend for plenty of reasons on a build that has the feature. Reporting "not available"
from that silence would turn a missing datanode into a false statement about the build, printed
next to a storage choice. So absence is never asserted, and that is what these tests hold in place.
"""
from __future__ import annotations

import json
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import (  # noqa: E402
    _FakeResponse, _FakeServer, _cfg, _factory_for, drive,
)

ADMIN = {"Authorization": "Bearer k-acme"}

_METRICS = (
    b"# HELP temporalstore_storage_backend Selected distributed storage backend (1 = active).\n"
    b"# TYPE temporalstore_storage_backend gauge\n"
    b'temporalstore_storage_backend{backend="matrixobject",replication="shared_store"} 1\n'
    b"temporalstore_data_node_runtime_queue_depth 0\n"
)


def _app(response=None):
    cfg = _cfg(blob_connection_factory=_factory_for(response or _FakeResponse(200, _METRICS)))
    return gw.make_v1_app(_FakeServer(), cfg)


class DeploymentCatalogueTest(unittest.TestCase):

    def test_it_needs_an_admin_key(self) -> None:
        status, _h, _b = drive(_app(), method="GET", path="/v1/admin/deployment")
        self.assertEqual(401, status)

    def test_it_offers_every_shape_with_storage_for_each(self) -> None:
        status, _h, body = drive(_app(), method="GET", path="/v1/admin/deployment", headers=ADMIN)
        self.assertEqual(200, status)
        doc = json.loads(body)
        shapes = {s["id"]: s for s in doc["catalogue"]["shapes"]}
        self.assertEqual({"onebox", "raft", "shared"}, set(shapes))
        for name, shape in shapes.items():
            with self.subTest(shape=name):
                self.assertTrue(shape["storage"], "no storage choices offered")
                self.assertTrue(shape["when"].strip(), "no guidance on when to pick it")
        # Shared storage defaults to the object store; the disk shapes never offer it.
        self.assertIn("matrixobject", [s["id"] for s in shapes["shared"]["storage"]])
        self.assertNotIn("matrixobject", [s["id"] for s in shapes["onebox"]["storage"]])

    def test_a_live_matrixobject_backend_confirms_the_feature(self) -> None:
        _st, _h, body = drive(_app(), method="GET", path="/v1/admin/deployment", headers=ADMIN)
        doc = json.loads(body)
        self.assertEqual("matrixobject", doc["live"]["backend"])
        self.assertEqual("shared_store", doc["live"]["replication"])
        self.assertTrue(doc["catalogue"]["matrixobject_confirmed"])

    def test_another_backend_does_not_claim_the_feature_is_absent(self) -> None:
        # A raft backend is not evidence that MatrixObject is uncompiled -- auto reaches raft for
        # several reasons on a build that has it. Confirmation goes false; nothing asserts absence.
        raft = _FakeResponse(200, b'temporalstore_storage_backend{backend="raft",'
                                  b'replication="raft"} 1\n')
        _st, _h, body = drive(_app(raft), method="GET", path="/v1/admin/deployment", headers=ADMIN)
        doc = json.loads(body)
        self.assertEqual("raft", doc["live"]["backend"])
        self.assertFalse(doc["catalogue"]["matrixobject_confirmed"])
        self.assertTrue(doc["catalogue"]["matrixobject_available"],
                        "absence was asserted from silence")

    def test_an_unreachable_datanode_is_reported_not_fatal(self) -> None:
        # Every way this can fail has to land on "could not determine", because the alternative is
        # a 500 on the page that tells an operator what their deployment is doing.
        #
        # The third fixture is the one that carries weight. A 503 with an EMPTY body returns None
        # whether or not the status is checked -- there are no lines to parse either way -- so it
        # cannot tell a working status check from a missing one. A 503 that still carries a
        # parseable metric line can, and it is the realistic shape: a datanode in a state where its
        # own numbers should not be believed can still serve bytes that scan cleanly.
        fixtures = (
            _FakeResponse(503, b""),
            _FakeResponse(200, b"nothing useful\n"),
            _FakeResponse(503, b'temporalstore_storage_backend{backend="raft",'
                               b'replication="raft"} 1\n'),
        )
        for index, response in enumerate(fixtures):
            with self.subTest(fixture=index, status=response.status):
                status, _h, body = drive(_app(response), method="GET",
                                         path="/v1/admin/deployment", headers=ADMIN)
                self.assertEqual(200, status)
                doc = json.loads(body)
                self.assertIsNone(doc["live"],
                                  "a failed scrape was reported as a live backend")
                self.assertIn("temporalstore_storage_backend", doc["live_detail"])

    def test_a_connection_that_raises_is_survived(self) -> None:
        def explode(_cfg_arg):
            raise OSError("connection refused")
        app = gw.make_v1_app(_FakeServer(), _cfg(blob_connection_factory=explode))
        status, _h, body = drive(app, method="GET", path="/v1/admin/deployment", headers=ADMIN)
        self.assertEqual(200, status)
        self.assertIsNone(json.loads(body)["live"])


class DeploymentPlanRouteTest(unittest.TestCase):

    def _plan(self, payload, app=None):
        status, _h, body = drive(app or _app(), method="POST",
                                 path="/v1/admin/deployment/plan", body=payload, headers=ADMIN)
        return status, json.loads(body)

    def test_it_needs_an_admin_key(self) -> None:
        status, _h, _b = drive(_app(), method="POST", path="/v1/admin/deployment/plan",
                               body={"shape": "onebox"})
        self.assertEqual(401, status)

    def test_a_onebox_plan_comes_back_with_an_env_file(self) -> None:
        status, plan = self._plan({"shape": "onebox", "storage": "ebs"})
        self.assertEqual(200, status)
        self.assertTrue(plan["ok"], plan["blocking"])
        self.assertEqual("1", plan["env"]["TS_STANDALONE"])
        self.assertIn("TS_PAGE_STORE_DIR=", plan["env_file"])
        self.assertIn("TS_STANDALONE=1", plan["env_file"])

    def test_an_even_raft_count_is_refused_with_the_reason(self) -> None:
        status, plan = self._plan({"shape": "raft", "storage": "ebs", "nodes": 4})
        self.assertEqual(200, status)
        self.assertFalse(plan["ok"])
        self.assertTrue(any("odd" in b for b in plan["blocking"]), plan["blocking"])

    def test_shared_storage_without_a_directory_is_refused(self) -> None:
        status, plan = self._plan({"shape": "shared", "storage": "path", "nodes": 3})
        self.assertEqual(200, status)
        self.assertFalse(plan["ok"])
        self.assertTrue(any("TS_SHARED_STORE_DIR" in b for b in plan["blocking"]))

    def test_a_key_name_is_carried_but_never_a_value(self) -> None:
        status, plan = self._plan({"shape": "onebox", "storage": "ebs",
                                   "key_envs": ["DEEPSEEK_API_KEY"]})
        self.assertEqual(200, status)
        self.assertNotIn("DEEPSEEK_API_KEY=", plan["env_file"])
        self.assertNotIn("DEEPSEEK_API_KEY", plan["env"])

    def test_a_malformed_body_is_a_400_not_a_500(self) -> None:
        status, _h, body = drive(_app(), method="POST", path="/v1/admin/deployment/plan",
                                 raw=b"{not json", headers=ADMIN)
        self.assertEqual(400, status)
        self.assertEqual("invalid_json", json.loads(body)["error"])

    def test_an_unknown_shape_is_refused_rather_than_guessed(self) -> None:
        status, plan = self._plan({"shape": "wishful"})
        self.assertEqual(200, status)
        self.assertFalse(plan["ok"])
        self.assertEqual({}, plan["env"])


class _PageHarness:
    """Drives the built Setup page against real route bodies.

    A mixin rather than a base test class: subclassing a TestCase to reuse one helper also inherits
    and re-runs every one of its test methods.
    """

    def setUp(self) -> None:
        import shutil
        if not shutil.which("node"):
            self.skipTest("node is not installed")
        self.app = _app()

    def _routes(self, plan_payload) -> dict:
        _st, _h, config = drive(self.app, method="GET", path="/v1/admin/config", headers=ADMIN)
        _st, _h, deployment = drive(self.app, method="GET", path="/v1/admin/deployment",
                                    headers=ADMIN)
        _st, _h, plan = drive(self.app, method="POST", path="/v1/admin/deployment/plan",
                              body=plan_payload, headers=ADMIN)
        return {
            "config": json.loads(config),
            "deployment": json.loads(deployment),
            "plan": json.loads(plan),
        }

    def _run(self, plan_payload) -> dict:
        import subprocess
        import tempfile
        page = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "portal", "setup_portal.html")
        harness = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "portal", "deployment_chooser_harness.js")
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(self._routes(plan_payload), handle)
            fixture = handle.name
        try:
            proc = subprocess.run(["node", harness, page, fixture],
                                  capture_output=True, text=True, timeout=60)
        finally:
            os.unlink(fixture)
        self.assertEqual(0, proc.returncode, proc.stderr)
        return json.loads(proc.stdout)


class ChooserRendersTest(_PageHarness, unittest.TestCase):
    """The page, run against the real route bodies.

    A request and its outcome differ here on purpose, and in page source the two are the same shape
    of string concatenation. Rendering the resolved backend and rendering the requested one look
    identical to a grep, so the only way to tell them apart is to run the page and read what landed
    in the DOM.
    """

    def test_every_shape_is_offered_and_the_plan_is_previewed(self) -> None:
        result = self._run({"shape": "onebox", "storage": "ebs"})
        self.assertEqual([], result["errors"], "the page's scripts threw")
        self.assertEqual(3, result["shapeOptions"])
        self.assertIn("TS_STANDALONE=1", result["envFile"])
        # The preview happens without the customer pressing anything -- checked against the
        # snapshot taken before the harness dispatched any change, because a post made in response
        # to the harness's own interaction proves nothing about what a customer sees on arrival.
        self.assertTrue(
            any("/v1/admin/deployment/plan" in url for url in result["postedOnLoad"]),
            "no plan was previewed until something was touched")
        self.assertIn("Resolves to", result["verdictOnLoad"])

    def test_the_page_shows_what_the_plan_resolves_to_not_what_was_asked(self) -> None:
        result = self._run({"shape": "onebox", "storage": "ebs"})
        self.assertIn("Resolves to", result["verdict"])
        # The live backend read from the engine is stated, not inferred from the form.
        self.assertIn("matrixobject", result["live"])

    def test_a_blocked_plan_is_shown_as_blocked(self) -> None:
        result = self._run({"shape": "shared", "storage": "path", "nodes": 3})
        self.assertIn("Blocked", result["verdict"])
        self.assertIn("TS_SHARED_STORE_DIR", result["verdict"])

    def test_the_shared_directory_field_follows_the_storage_choice(self) -> None:
        # It is required for a shared filesystem and meaningless for the object store, and the
        # difference is a live DOM change no source read can confirm.
        result = self._run({"shape": "onebox", "storage": "ebs"})
        self.assertIn("MatrixObject", result["storageAfterShared"])
        self.assertTrue(result["sharedFieldShownForPath"],
                        "a shared filesystem was offered with nowhere to put the directory")
        self.assertTrue(result["sharedFieldHiddenForObject"],
                        "a directory field was left showing for the object store")


class LaunchArtifactRouteTest(unittest.TestCase):
    """What comes back over HTTP, since that is what a customer copies."""

    def _plan(self, payload):
        _st, _h, body = drive(_app(), method="POST", path="/v1/admin/deployment/plan",
                              body=payload, headers=ADMIN)
        return json.loads(body)

    def test_a_launchable_plan_carries_its_script_and_its_teardown(self) -> None:
        plan = self._plan({"shape": "onebox", "storage": "ebs", "region": "eu-west-1"})
        self.assertTrue(plan["ok"])
        self.assertIn("#!/bin/bash", plan["cloud_init"])
        self.assertIn("run-instances", plan["commands"]["launch"])
        self.assertIn("terminate-instances", plan["commands"]["teardown"])
        self.assertIn("eu-west-1", plan["commands"]["launch"])

    def test_a_blocked_plan_carries_no_launch_script(self) -> None:
        # Handing over a script for a configuration already known not to produce the requested
        # deployment is how the blocking message gets stepped over.
        plan = self._plan({"shape": "raft", "storage": "ebs", "nodes": 4})
        self.assertFalse(plan["ok"])
        self.assertNotIn("cloud_init", plan)
        self.assertNotIn("commands", plan)

    def test_no_key_value_crosses_the_wire_in_a_script(self) -> None:
        plan = self._plan({"shape": "onebox", "storage": "ebs",
                           "key_envs": ["DEEPSEEK_API_KEY"]})
        self.assertIn("# DEEPSEEK_API_KEY=", plan["cloud_init"])
        self.assertNotIn("\nDEEPSEEK_API_KEY=", plan["cloud_init"])


class LaunchArtifactRendersTest(_PageHarness, unittest.TestCase):
    """The page's copy of the same guarantee, read out of the DOM after running it."""

    def test_the_page_shows_the_script_and_the_teardown(self) -> None:
        result = self._run({"shape": "onebox", "storage": "ebs"})
        self.assertIn("#!/bin/bash", result["userData"])
        self.assertIn("run-instances", result["commands"])
        self.assertIn("terminate-instances", result["commands"],
                      "the page offers a launch with no way to undo it")

    def test_the_page_never_renders_a_key_value(self) -> None:
        result = self._run({"shape": "onebox", "storage": "ebs",
                            "key_envs": ["DEEPSEEK_API_KEY"]})
        self.assertIn("# DEEPSEEK_API_KEY=", result["userData"])
        self.assertNotIn("\nDEEPSEEK_API_KEY=", result["userData"])


class ThisFileDefinesEachClassOnceTest(unittest.TestCase):
    """A class defined twice silently shadows the first, and nothing anywhere reports it.

    Four copies of one test class accumulated here from a re-run patch script. Python binds the
    name to the last definition, so unittest collected 26 of the 34 test methods present and eight
    could never fail -- while the file imported cleanly, the suite passed, and the count looked
    entirely plausible. That is the same shape of vacuous test the rest of this file exists to rule
    out, so it is worth one assertion.
    """

    def test_no_class_is_defined_twice(self) -> None:
        import collections
        with open(os.path.abspath(__file__), encoding="utf-8") as handle:
            names = re.findall(r"^class ([A-Za-z_][A-Za-z0-9_]*)", handle.read(), re.M)
        self.assertTrue(names, "found no class definitions -- this check is looking in the wrong "
                               "place and would pass no matter what")
        repeated = sorted(n for n, count in collections.Counter(names).items() if count > 1)
        self.assertEqual([], repeated,
                         "these classes are defined more than once, so the earlier definitions are "
                         "dead code that can never fail: %s" % repeated)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Choosing a model: the catalogue, endpoint discovery, and the guard on changing an encoder.

The failure this covers is not a crash. A model name that is merely wrong makes extraction fall back
to the local rules and embedding fall back to hash vectors -- both answer 200, so the deployment
looks healthy while storing much less than it was asked to. Everything here is about making the
choice visible before it is made.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402
import matrixark_gateway_metrics as metricsmod  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg, drive  # noqa: E402

ADMIN = {"Authorization": "Bearer k-acme"}
HERE = os.path.dirname(os.path.abspath(__file__))


def _read_text(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


class _ModelTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _FakeServer()
        self.app = gw.make_v1_app(self.server, _cfg())
        self._saved_env = dict(os.environ)
        self._saved_boot = dict(cfgmod._BOOT_ENV)
        for name in list(os.environ):
            if name.startswith(("MATRIXARK_", "DEEPSEEK_", "OPENAI_", "LOCAL_ENCODER_")):
                del os.environ[name]
        cfgmod._BOOT_ENV.clear()
        self._dir = tempfile.TemporaryDirectory()
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(self._dir.name, "runtime.json")
        metricsmod.METRICS.__init__()  # type: ignore[misc]

    def tearDown(self) -> None:
        self._dir.cleanup()
        os.environ.clear()
        os.environ.update(self._saved_env)
        cfgmod._BOOT_ENV.clear()
        cfgmod._BOOT_ENV.update(self._saved_boot)

    def get(self, path: str, headers=ADMIN):
        status, _h, body = drive(self.app, method="GET", path=path, headers=headers)
        try:
            return status, json.loads(body.decode("utf-8"))
        except ValueError:
            return status, {}


class ModelsRouteTest(_ModelTest):
    def test_it_needs_a_key(self) -> None:
        status, _ = self.get("/v1/admin/models?target=extraction&probe=0", headers=None)
        self.assertEqual(401, status)

    def test_an_unknown_target_is_refused_rather_than_answered_with_the_wrong_list(self) -> None:
        # Answering an unrecognised target with the extraction catalogue would show a customer a
        # list of chat models under the heading they asked about.
        status, body = self.get("/v1/admin/models?target=rerank&probe=0")
        self.assertEqual(400, status)
        self.assertIn("extraction", body.get("detail", ""))

    def test_it_defaults_to_extraction_rather_than_erroring_without_a_target(self) -> None:
        status, body = self.get("/v1/admin/models?probe=0")
        self.assertEqual(200, status)
        self.assertEqual("extraction", body["target"])

    def test_probing_is_off_unless_asked_for(self) -> None:
        # Discovery calls somebody else's service. Doing it on every page load would turn opening
        # the portal into traffic against the customer's model provider.
        status, body = self.get("/v1/admin/models?target=extraction&probe=0")
        self.assertEqual(200, status)
        self.assertFalse(body["discovered"]["available"])
        self.assertEqual("not_probed", body["discovered"]["reason"])

    def test_the_catalogue_is_returned_for_each_target(self) -> None:
        for target in ("extraction", "embedding"):
            with self.subTest(target=target):
                status, body = self.get("/v1/admin/models?target=%s&probe=0" % target)
                self.assertEqual(200, status)
                self.assertTrue(body["catalogue"])
                for entry in body["catalogue"]:
                    self.assertTrue(entry["model"].strip())
                    self.assertTrue(entry["note"].strip())

    def test_the_measured_table_reaches_the_page(self) -> None:
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/setup")
        text = body.decode("utf-8")
        self.assertIn("measuredTable", text)
        self.assertIn("hit@1", text)

    def test_every_embedding_in_the_catalogue_states_its_width(self) -> None:
        # Width is the one property that decides whether a switch is survivable, and it is the one
        # a customer cannot look up from inside the portal. A catalogue entry without it invites
        # exactly the same-width swap that fails silently.
        status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        self.assertEqual(200, status)
        for entry in body["catalogue"]:
            with self.subTest(model=entry["model"]):
                self.assertIsInstance(entry.get("dim"), int)
                self.assertGreater(entry["dim"], 0)

    def test_every_same_width_pair_in_the_catalogue_names_the_other(self) -> None:
        # The trap is two DIFFERENT encoders of the SAME width -- nothing in the stack raises an
        # error on that swap. Written as prose this only ever warned about the pair somebody
        # remembered: the note called out the two 384-dim MiniLMs and said nothing about bge-m3 and
        # voyage-3 both being 1024. Derived from the widths, every collision is covered, including
        # ones added later.
        status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        self.assertEqual(200, status)
        widths: dict = {}
        for entry in body["catalogue"]:
            widths.setdefault(entry["dim"], []).append(entry["model"])
        collisions = {dim: names for dim, names in widths.items() if len(names) > 1}
        self.assertTrue(collisions, "no same-width pair in the catalogue to warn about")
        for entry in body["catalogue"]:
            with self.subTest(model=entry["model"]):
                expected = sorted(name for name in widths[entry["dim"]]
                                  if name != entry["model"])
                self.assertEqual(expected, sorted(entry["same_width_as"]))

    def test_a_model_added_to_the_catalogue_annotates_itself(self) -> None:
        # The point of deriving it: nobody has to remember to update a second place.
        added = dict(gw._ENCODER_CATALOG[0])
        added["id"] = "some-new-encoder"
        gw._ENCODER_CATALOG.append(added)
        try:
            rows = {row["model"]: row for row in gw.embedding_picker_catalogue()}
        finally:
            gw._ENCODER_CATALOG.pop()
        twin = str(gw._ENCODER_CATALOG[0]["id"])
        self.assertIn("some-new-encoder", rows)
        self.assertIn(twin, rows["some-new-encoder"]["same_width_as"])
        self.assertIn("some-new-encoder", rows[twin]["same_width_as"])

    def test_the_catalogue_returned_is_a_copy(self) -> None:
        # It is annotated on the way out; handing back the module-level list would let one request's
        # annotation accumulate onto the next.
        first = gw.embedding_picker_catalogue()
        first[0]["same_width_as"] = ["tampered"]
        second = gw.embedding_picker_catalogue()
        self.assertNotEqual(["tampered"], second[0]["same_width_as"])
        self.assertNotIn("same_width_as", gw._ENCODER_CATALOG[0])

    def test_the_picker_serves_the_MEASURED_catalogue_not_a_second_list(self) -> None:
        # Two lists of the same thing do not stay agreed. The one this replaced omitted the entire
        # e5 family -- the models that measured best -- and recommended the encoder the measurement
        # puts fifteen points of hit@1 behind e5-small at the same size. One list, or the picker
        # argues against the evidence beside it.
        served = {row["model"] for row in gw.embedding_picker_catalogue()}
        measured = {str(row["id"]) for row in gw.encoder_catalog()}
        self.assertEqual(measured, served)
        with self.assertRaises(ValueError):
            cfgmod.model_catalogue("embedding")

    def test_every_encoder_carries_the_numbers_a_choice_is_made_on(self) -> None:
        # Asserted on what the ROUTE serves, because that is what the picker reads. An earlier
        # version checked the function instead, and a mutation that stripped the measurements from
        # the route's answer left every test green.
        status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        self.assertEqual(200, status)
        self.assertTrue(body["catalogue"])
        for row in body["catalogue"]:
            with self.subTest(model=row["model"]):
                for field in ("hit_at_1", "hit_at_5", "texts_per_s", "vectors_mb_per_doc"):
                    self.assertIsInstance(row[field], (int, float),
                                          "%s is missing from what the route serves" % field)
                self.assertGreater(row["hit_at_1"], 0)

    def test_the_route_serves_the_measured_rows_unchanged(self) -> None:
        # The route may annotate; it must not invent or drop. Comparing the served rows against the
        # measurement itself is what makes "the picker shows the measurement" checkable.
        _status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        served = {row["model"]: row for row in body["catalogue"]}
        for measured in gw.encoder_catalog():
            model = str(measured["id"])
            with self.subTest(model=model):
                self.assertIn(model, served)
                for field in ("hit_at_1", "hit_at_5", "texts_per_s", "vectors_mb_per_doc"):
                    self.assertEqual(measured[field], served[model][field])
                self.assertEqual(measured["dims"], served[model]["dim"])

    def test_the_picker_shows_the_collision_where_the_choice_is_made(self) -> None:
        _st, _h, page = drive(self.app, method="GET", path="/v1/admin/setup")
        self.assertIn("same width as", page.decode("utf-8"))

    def test_extraction_does_not_carry_the_embedding_warning(self) -> None:
        # Showing the encoder warning where it does not apply is how a warning stops being read.
        status, body = self.get("/v1/admin/models?target=extraction&probe=0")
        self.assertEqual(200, status)
        self.assertNotIn("change_warning", body)
        self.assertNotIn("in_store", body)

    def test_embedding_carries_the_warning_and_what_the_store_holds(self) -> None:
        status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        self.assertEqual(200, status)
        self.assertIn("change_warning", body)
        self.assertIn("in_store", body)

    def test_the_warning_names_the_same_width_trap(self) -> None:
        # A warning that only says "re-encode afterwards" leaves a customer believing a width
        # mismatch would have stopped them. It would not: both MiniLMs are 384.
        status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        warning = body["change_warning"].lower()
        self.assertIn("same width", warning)
        self.assertIn("384", warning)
        self.assertIn("re-encode", warning)

    def test_it_reports_what_is_configured_now(self) -> None:
        os.environ["MATRIXARK_EXTRACTION_MODEL"] = "deepseek-chat"
        status, body = self.get("/v1/admin/models?target=extraction&probe=0")
        self.assertEqual("deepseek-chat", body["current"])

    def test_the_current_model_is_read_per_call_not_captured_at_boot(self) -> None:
        # The picker's whole value is showing what is in force. A value captured when the process
        # started would keep showing the old model after a save, which reads as a save that did not
        # take.
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = "first"
        _status, first = self.get("/v1/admin/models?target=embedding&probe=0")
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = "second"
        _status, second = self.get("/v1/admin/models?target=embedding&probe=0")
        self.assertEqual("first", first["current"])
        self.assertEqual("second", second["current"])


class StoredEncoderTest(_ModelTest):
    def test_a_backend_that_cannot_answer_reports_unknown_not_empty(self) -> None:
        # "Nothing is stored" makes a destructive change look free. Unknown must stay unknown.
        def _boom(name, args):
            raise RuntimeError("backend down")

        self.server.call_tool = _boom  # type: ignore[assignment]
        status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        self.assertEqual(200, status)
        self.assertFalse(body["in_store"]["known"])
        self.assertTrue(body["in_store"]["detail"].strip())

    def test_it_passes_through_what_the_backend_reports(self) -> None:
        captured = {}

        def _tool(name, args):
            captured["name"] = name
            return {"total": 12, "models": [{"model": "bge-m3", "count": 12}],
                    "dimensions": [{"dim": 1024, "count": 12}], "mixed_dimensions": False}

        self.server.call_tool = _tool  # type: ignore[assignment]
        _status, body = self.get("/v1/admin/models?target=embedding&probe=0")
        self.assertEqual("matrixark_embedding_status", captured["name"])
        self.assertTrue(body["in_store"]["known"])
        self.assertEqual(12, body["in_store"]["total"])
        self.assertEqual([{"model": "bge-m3", "count": 12}], body["in_store"]["models"])


class DiscoveryTest(_ModelTest):
    def test_no_base_url_says_so_rather_than_failing_a_request(self) -> None:
        result = cfgmod.discover_models("extraction")
        self.assertFalse(result["available"])
        self.assertEqual("no_base_url", result["reason"])

    def test_an_endpoint_that_does_not_implement_the_listing_is_not_an_error(self) -> None:
        # Plenty of OpenAI-compatible servers answer 404 to /models. That must read as "cannot ask",
        # not as "the endpoint is broken" -- the field still works as free text.
        import urllib.error

        os.environ["MATRIXARK_EXTRACTION_BASE_URL"] = "https://example.invalid/v1"
        original = cfgmod._get_json

        def _fail(url, headers, timeout):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)  # type: ignore[arg-type]

        cfgmod._get_json = _fail  # type: ignore[assignment]
        try:
            result = cfgmod.discover_models("extraction")
        finally:
            cfgmod._get_json = original  # type: ignore[assignment]
        self.assertFalse(result["available"])
        self.assertEqual("http_404", result["reason"])
        self.assertIn("free text", result["detail"])

    def test_it_asks_the_configured_base_url_and_returns_sorted_unique_names(self) -> None:
        os.environ["MATRIXARK_EXTRACTION_BASE_URL"] = "https://example.invalid/v1/"
        seen = {}
        original = cfgmod._get_json

        def _ok(url, headers, timeout):
            seen["url"] = url
            return 200, {"data": [{"id": "b"}, {"id": "a"}, {"id": "a"}]}

        cfgmod._get_json = _ok  # type: ignore[assignment]
        try:
            result = cfgmod.discover_models("extraction")
        finally:
            cfgmod._get_json = original  # type: ignore[assignment]
        # The trailing slash must not produce //models -- some gateways 404 on it.
        self.assertEqual("https://example.invalid/v1/models", seen["url"])
        self.assertEqual(["a", "b"], result["models"])
        self.assertEqual(2, result["count"])

    def test_it_sends_the_key_as_a_header_never_in_the_url(self) -> None:
        os.environ["MATRIXARK_EXTRACTION_BASE_URL"] = "https://example.invalid/v1"
        os.environ["DEEPSEEK_API_KEY"] = "sk-secret"
        os.environ["MATRIXARK_EXTRACTION_API_KEY_ENV"] = "DEEPSEEK_API_KEY"
        seen = {}
        original = cfgmod._get_json

        def _ok(url, headers, timeout):
            seen["url"] = url
            seen["headers"] = headers
            return 200, {"data": []}

        cfgmod._get_json = _ok  # type: ignore[assignment]
        try:
            cfgmod.discover_models("extraction")
        finally:
            cfgmod._get_json = original  # type: ignore[assignment]
        self.assertNotIn("sk-secret", seen["url"])
        self.assertEqual("Bearer sk-secret", seen["headers"]["Authorization"])

    def test_a_body_that_is_not_a_model_list_does_not_become_an_empty_list(self) -> None:
        # An empty list reads as "this endpoint serves nothing", which is a different and wrong
        # conclusion from "that answer was not a model list".
        os.environ["MATRIXARK_EXTRACTION_BASE_URL"] = "https://example.invalid/v1"
        original = cfgmod._get_json
        cfgmod._get_json = lambda url, headers, timeout: (200, ["not", "a", "dict"])  # type: ignore
        try:
            result = cfgmod.discover_models("extraction")
        finally:
            cfgmod._get_json = original  # type: ignore[assignment]
        self.assertFalse(result["available"])
        self.assertEqual("unexpected_body", result["reason"])


class PickerPageTest(_ModelTest):
    def test_the_setup_page_offers_the_picker_and_wires_it_to_the_real_fields(self) -> None:
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/setup")
        text = body.decode("utf-8")
        self.assertIn('id="models"', text)
        self.assertIn('id="probeModels"', text)
        self.assertIn("/v1/admin/models?target=", text)
        # The picker must set the SAME settings the form saves; a separate write path would be a
        # second thing to keep correct and a second entry in the change log.
        self.assertIn('key: "extraction.model"', text)
        self.assertIn('key: "embedding.model"', text)

    def test_the_warning_is_computed_from_the_pending_selection(self) -> None:
        # The risk has to be visible while the encoder is being chosen. Computing it only from the
        # value already written into the form told a customer about a decision they had made.
        source = _read_text(os.path.join(HERE, "portal", "build_portal_pages.py"))
        start = source.index("var pendingPick = {}")
        self.assertIn("pendingPick", source[source.index("function renderModels()"):
                                            source.index("function pickedModel")])
        handler = source[source.index('$("models").addEventListener("change"'):]
        handler = handler[:handler.index('$("models").addEventListener("click"')]
        self.assertIn("pendingPick[target] = pickedModel(target)", handler)
        self.assertIn("renderModels()", handler)
        self.assertGreater(start, 0)

    def test_choosing_type_it_yourself_does_not_re_render_the_box_being_typed_in(self) -> None:
        # A re-render replaces the text input, so doing it on the way into free-text entry throws
        # away the focus and anything already typed.
        source = _read_text(os.path.join(HERE, "portal", "build_portal_pages.py"))
        handler = source[source.index('$("models").addEventListener("change"'):]
        handler = handler[:handler.index("pendingPick[target] = pickedModel(target)")]
        self.assertIn('=== "__other__"', handler)
        self.assertIn("return;", handler)

    def test_the_risk_check_uses_the_same_name_rule_as_the_guard(self) -> None:
        # The backend stopped treating a repository prefix as a different model. The page's own
        # check still compared exactly, so re-spelling the encoder a store already holds raised
        # "changing this strands what is already stored" -- which strands nothing, and teaches the
        # reader to click past the warning on the change that would.
        source = _read_text(os.path.join(HERE, "portal", "build_portal_pages.py"))
        start = source.index("function embeddingRisk(")
        block = source[start:source.index("function renderModels()", start)]
        self.assertIn("sameModel(", block)
        self.assertNotIn("models[0] === chosen", block)
        self.assertNotIn('chosen === (d.current', block)

    def test_the_name_rule_is_declared_before_the_checks_that_use_it(self) -> None:
        # Declarations hoist, so this is about the page reading in the order it runs.
        source = _read_text(os.path.join(HERE, "portal", "build_portal_pages.py"))
        self.assertLess(source.index("function sameModel("),
                        source.index("function embeddingRisk("))

    def test_the_picker_does_not_save_on_its_own(self) -> None:
        # Choosing a model has to land in the pending-changes set, not go straight to the server:
        # an encoder change saved on click is one nobody reviewed.
        source = _read_text(os.path.join(HERE, "portal", "build_portal_pages.py"))
        start = source.index('$("models").addEventListener("click"')
        block = source[start:start + 1800]
        self.assertIn("edits[entry.key] = value", block)
        self.assertIn("markDirty()", block)
        self.assertNotIn('method: "POST"', block)


if __name__ == "__main__":
    unittest.main()

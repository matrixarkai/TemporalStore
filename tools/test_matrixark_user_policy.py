#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Per-user dynamic config: what may vary per user, and what must not.

A store is shared inside a tenant. A per-user override that changes how one request's results are
selected touches nobody else. A per-user override that changes what gets WRITTEN leaves one store
holding records of two shapes -- and unlike a setting, that does not go back when the setting does.

So the layer a knob belongs to is part of the knob, the default is the restrictive one, and these
tests hold both halves: that a read-path override actually takes effect, and that a write-path one
is refused however it arrives.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_tenant_policy as tp  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg, drive  # noqa: E402

ACME_ALICE = {"tenant_id": "acme", "user_id": "alice"}
ACME_BOB = {"tenant_id": "acme", "user_id": "bob"}
GLOBEX_ALICE = {"tenant_id": "globex", "user_id": "alice"}


class _PolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        tp.clear_tenant_policy_cache()
        self._saved = {name: os.environ.pop(knob.env)
                       for name, knob in tp.KNOBS.items() if knob.env in os.environ}
        # Pinned at a file that does not exist, so apply_boot() has nothing to seed.
        # make_v1_app() calls it, and on a configured box the stored document carries
        # real values -- which would land in the environment after this isolation ran
        # and be read as though the test had set them.
        self._config_dir = tempfile.TemporaryDirectory()
        self._saved_config = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE")
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(
            self._config_dir.name, "runtime.json")

    def tearDown(self) -> None:
        tp.clear_tenant_policy_cache()
        if self._saved_config is None:
            os.environ.pop("MATRIXARK_RUNTIME_CONFIG_FILE", None)
        else:
            os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = self._saved_config
        self._config_dir.cleanup()
        for name, value in self._saved.items():
            os.environ[tp.KNOBS[name].env] = value


class LayerClassificationTest(_PolicyTest):
    def test_every_knob_declares_a_layer(self) -> None:
        for name, knob in tp.KNOBS.items():
            with self.subTest(knob=name):
                self.assertIn(knob.layer, ("read", "write"))

    def test_a_new_knob_is_tenant_only_until_someone_says_otherwise(self) -> None:
        # The default has to be the restrictive one. A knob added later without anyone thinking
        # about the shared store must not become per-user settable by omission.
        knob = tp.Knob("invented", "bool", "MATRIXARK_INVENTED", True, "d")
        self.assertEqual("write", knob.layer)

    def test_an_unknown_layer_is_refused_at_construction(self) -> None:
        with self.assertRaises(ValueError):
            tp.Knob("invented", "bool", "MATRIXARK_INVENTED", True, "d", layer="sometimes")

    def test_the_read_path_list_names_only_real_knobs(self) -> None:
        # A rename that left this list behind would silently drop a knob back to tenant-only, and
        # the customer's setting would stop applying with nothing to say why.
        for name in tp.READ_PATH_KNOBS:
            with self.subTest(knob=name):
                self.assertIn(name, tp.KNOBS)
                self.assertEqual("read", tp.KNOBS[name].layer)

    def test_the_knobs_that_write_are_not_settable_per_user(self) -> None:
        # Named individually rather than counted: each of these leaves a permanent mark on a store
        # the whole tenant reads.
        for name in ("generate_embeddings", "write_secondary_index", "max_event_text_chars",
                     "max_summary_text_chars", "summary_levels", "extract_segments",
                     "generate_l1_summaries", "store_event_summary_text"):
            with self.subTest(knob=name):
                self.assertEqual("write", tp.KNOBS[name].layer)

    def test_recall_reinforcement_is_tenant_only_because_retrieval_writes(self) -> None:
        # The one that reads like a retrieval knob and is a writer: measured, five searches
        # produced 572 protection markers. Per-user divergence changes what survives pruning for
        # everyone in the tenant.
        self.assertEqual("write", tp.KNOBS["recall_reinforcement"].layer)
        self.assertIn("writer", tp.KNOBS["recall_reinforcement"].description)


class ResolutionOrderTest(_PolicyTest):
    def test_a_user_override_wins_over_the_tenant(self) -> None:
        tp.set_tenant_policy("acme", {"top_k_per_layer": 100})
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
        self.assertEqual(12, tp.resolve("top_k_per_layer", ACME_ALICE))

    def test_another_user_in_the_same_tenant_is_unaffected(self) -> None:
        tp.set_tenant_policy("acme", {"top_k_per_layer": 100})
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
        self.assertEqual(100, tp.resolve("top_k_per_layer", ACME_BOB))

    def test_the_same_user_name_in_another_tenant_is_a_different_person(self) -> None:
        # The identity is the PAIR. Keyed on the user id alone, one customer's settings would be
        # served to a same-named user at another company.
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
        self.assertEqual(tp.KNOBS["top_k_per_layer"].default,
                         tp.resolve("top_k_per_layer", GLOBEX_ALICE))

    def test_the_tenant_still_answers_where_the_user_said_nothing(self) -> None:
        tp.set_tenant_policy("acme", {"top_k_per_layer": 100, "max_selected_refs": 7})
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
        self.assertEqual(7, tp.resolve("max_selected_refs", ACME_ALICE))

    def test_the_environment_still_answers_below_both(self) -> None:
        os.environ[tp.KNOBS["top_k_per_layer"].env] = "55"
        try:
            self.assertEqual(55, tp.resolve("top_k_per_layer", ACME_ALICE))
            tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
            self.assertEqual(12, tp.resolve("top_k_per_layer", ACME_ALICE))
        finally:
            os.environ.pop(tp.KNOBS["top_k_per_layer"].env, None)

    def test_a_scope_with_no_user_resolves_at_the_tenant(self) -> None:
        tp.set_tenant_policy("acme", {"top_k_per_layer": 100})
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
        self.assertEqual(100, tp.resolve("top_k_per_layer", {"tenant_id": "acme"}))

    def test_it_takes_effect_without_a_restart(self) -> None:
        # The whole point of the layer: a customer changes a setting in the portal and the next
        # request uses it.
        before = tp.resolve("max_selected_refs", ACME_ALICE)
        tp.set_user_policy("acme", "alice", {"max_selected_refs": before + 3})
        self.assertEqual(before + 3, tp.resolve("max_selected_refs", ACME_ALICE))


class WritePathRefusalTest(_PolicyTest):
    def test_a_write_path_knob_is_dropped_from_a_user_policy(self) -> None:
        kept = tp.set_user_policy("acme", "alice", {"generate_embeddings": False})
        self.assertEqual({}, kept)
        self.assertTrue(tp.resolve("generate_embeddings", ACME_ALICE))

    def test_the_rest_of_the_set_survives_one_refused_key(self) -> None:
        # A refusal must not discard a customer's other, legitimate changes.
        kept = tp.set_user_policy("acme", "alice",
                                  {"generate_embeddings": False, "max_selected_refs": 5})
        self.assertEqual({"max_selected_refs": 5}, kept)
        self.assertEqual(5, tp.resolve("max_selected_refs", ACME_ALICE))

    def test_a_durable_record_cannot_smuggle_one_in(self) -> None:
        # The refusal has to sit at the layer, not at the API: a record written by an older build,
        # or by hand, is read back on load and must be filtered the same way.
        count = tp.register_user_policy_records([{
            "record_type": tp.USER_POLICY_RECORD_TYPE,
            "tenant_id": "acme", "user_id": "alice",
            "policy": {"generate_embeddings": False, "top_k_per_layer": 9},
        }])
        self.assertEqual(1, count)
        self.assertTrue(tp.resolve("generate_embeddings", ACME_ALICE))
        self.assertEqual(9, tp.resolve("top_k_per_layer", ACME_ALICE))

    def test_the_record_form_is_filtered_before_it_is_stored(self) -> None:
        record = tp.user_policy_record("acme", "alice",
                                       {"generate_embeddings": False, "top_k_per_layer": 9})
        self.assertEqual({"top_k_per_layer": 9}, record["policy"])
        self.assertEqual(tp.USER_POLICY_RECORD_TYPE, record["record_type"])

    def test_a_tenant_may_still_set_a_write_path_knob(self) -> None:
        # The restriction is per-USER. A tenant deciding for its whole store is the level where
        # that decision is coherent.
        tp.set_tenant_policy("acme", {"generate_embeddings": False})
        self.assertFalse(tp.resolve("generate_embeddings", ACME_ALICE))


class IdentityTest(_PolicyTest):
    def test_a_record_missing_half_an_identity_is_not_stored(self) -> None:
        # Half an identity is not an identity; stored under a partial key it would answer for
        # somebody else.
        self.assertEqual(0, tp.register_user_policy_records([
            {"record_type": tp.USER_POLICY_RECORD_TYPE, "tenant_id": "acme",
             "policy": {"top_k_per_layer": 9}},
            {"record_type": tp.USER_POLICY_RECORD_TYPE, "user_id": "alice",
             "policy": {"top_k_per_layer": 9}},
        ]))

    def test_setting_a_policy_without_both_halves_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            tp.set_user_policy("", "alice", {"top_k_per_layer": 9})
        with self.assertRaises(ValueError):
            tp.set_user_policy("acme", "", {"top_k_per_layer": 9})

    def test_the_user_is_read_from_a_scope_key_as_well_as_a_dict(self) -> None:
        # A served record carries hashes in a scope_key, not the human ids the portal set.
        self.assertEqual("u9", tp.user_of("t=t1|u=u9|s=s2"))
        self.assertEqual("alice", tp.user_of({"user_id": "alice"}))
        self.assertEqual("", tp.user_of({"tenant_id": "acme"}))
        self.assertEqual("", tp.user_of(None))

    def test_the_key_carries_both_halves(self) -> None:
        self.assertNotEqual(tp.user_key("acme", "alice"), tp.user_key("globex", "alice"))
        self.assertNotEqual(tp.user_key("acme", "alice"), tp.user_key("acme", "bob"))

    def test_clearing_the_cache_clears_the_user_layer_too(self) -> None:
        # A half-cleared state resolves from one layer that was reset and one that was not.
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
        tp.clear_tenant_policy_cache()
        self.assertEqual(tp.KNOBS["top_k_per_layer"].default,
                         tp.resolve("top_k_per_layer", ACME_ALICE))


class ProvenanceTest(_PolicyTest):
    def test_it_names_the_layer_each_value_came_from(self) -> None:
        tp.set_tenant_policy("acme", {"max_selected_refs": 7})
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 12})
        described = tp.describe_effective_policy(ACME_ALICE)
        self.assertEqual("user", described["knobs"]["top_k_per_layer"]["source"])
        self.assertEqual("tenant", described["knobs"]["max_selected_refs"]["source"])
        self.assertEqual("default", described["knobs"]["generate_embeddings"]["source"])

    def test_it_says_which_knobs_a_user_may_set(self) -> None:
        # So the portal can offer the ones that work and explain the ones it cannot.
        described = tp.describe_effective_policy(ACME_ALICE)
        self.assertTrue(described["knobs"]["top_k_per_layer"]["settable_per_user"])
        self.assertFalse(described["knobs"]["generate_embeddings"]["settable_per_user"])

    def test_it_reports_the_identity_it_resolved_for(self) -> None:
        described = tp.describe_effective_policy(ACME_ALICE)
        self.assertEqual("acme", described["tenant"])
        self.assertEqual("alice", described["user"])


class RegistryTest(_PolicyTest):
    def test_a_knob_defined_twice_is_refused(self) -> None:
        # It was a dict comprehension, which keeps the last. Three knobs were defined twice and
        # their earlier definitions were dead -- a different default sitting in the file that
        # nothing read.
        with self.assertRaises(ValueError):
            tp._registry(
                tp.Knob("dup", "bool", "MATRIXARK_DUP", True, "first"),
                tp.Knob("dup", "bool", "MATRIXARK_DUP2", False, "second"),
            )

    def test_the_shadowed_defaults_are_the_ones_that_were_in_force(self) -> None:
        # Removing the dead definitions must not have changed behaviour: the live values were
        # always the later ones.
        self.assertEqual(240, tp.KNOBS["top_k_per_layer"].default)
        self.assertEqual(20480, tp.KNOBS["max_global_candidates"].default)
        self.assertEqual(10000, tp.KNOBS["max_selected_refs"].default)


class PolicyFileTest(_PolicyTest):
    """Per-user overrides persist in the same file as the tenant ones.

    One file, one loader, one cache. A second file with its own path and its own keep-last-good
    behaviour would drift from this one on exactly the edge cases this one already handles.
    """

    def write(self, document: dict) -> None:
        import json
        import tempfile

        path = os.path.join(tempfile.mkdtemp(), "policy.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(document, handle)
        os.environ["MATRIXARK_TENANT_POLICY_PATH"] = path
        tp.clear_tenant_policy_cache()
        self.addCleanup(os.environ.pop, "MATRIXARK_TENANT_POLICY_PATH", None)
        self.addCleanup(tp.clear_tenant_policy_cache)
        return path

    def test_a_user_section_is_read_from_the_file(self) -> None:
        self.write({"tenants": {"acme": {"top_k_per_layer": 100}},
                    "users": {"acme": {"alice": {"top_k_per_layer": 12}}}})
        self.assertEqual(12, tp.resolve("top_k_per_layer", ACME_ALICE))
        self.assertEqual(100, tp.resolve("top_k_per_layer", ACME_BOB))

    def test_a_write_path_knob_written_by_hand_is_still_refused(self) -> None:
        # The refusal has to sit at the layer, not at the API. Somebody editing the file directly
        # is exactly the case an API-level check would miss.
        self.write({"users": {"acme": {"alice": {"generate_embeddings": False,
                                                 "max_selected_refs": 4}}}})
        self.assertTrue(tp.resolve("generate_embeddings", ACME_ALICE))
        self.assertEqual(4, tp.resolve("max_selected_refs", ACME_ALICE))

    def test_a_runtime_change_wins_over_the_file(self) -> None:
        # Same precedence as the tenant layer: the file is the starting point, runtime wins.
        self.write({"users": {"acme": {"alice": {"top_k_per_layer": 12}}}})
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 5})
        self.assertEqual(5, tp.resolve("top_k_per_layer", ACME_ALICE))

    def test_a_users_section_that_is_not_an_object_is_skipped_not_fatal(self) -> None:
        # A malformed section must not take the tenant policy down with it.
        self.write({"tenants": {"acme": {"top_k_per_layer": 100}},
                    "users": {"acme": "not-an-object"}})
        self.assertEqual(100, tp.resolve("top_k_per_layer", ACME_ALICE))

    def test_a_file_with_no_users_section_still_loads(self) -> None:
        self.write({"tenants": {"acme": {"top_k_per_layer": 100}}})
        self.assertEqual(100, tp.resolve("top_k_per_layer", ACME_ALICE))

    def test_a_broken_edit_keeps_the_last_good_user_overrides(self) -> None:
        # The tenant layer already keeps its last good policy through a broken write; the user
        # overrides must not be the half that disappears.
        path = self.write({"users": {"acme": {"alice": {"top_k_per_layer": 12}}}})
        self.assertEqual(12, tp.resolve("top_k_per_layer", ACME_ALICE))
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{ this is not json")
        self.assertEqual(12, tp.resolve("top_k_per_layer", ACME_ALICE))


class PolicyRouteTest(_PolicyTest):
    """`/v1/admin/policy` — read and change one user's settings."""

    def setUp(self) -> None:
        super().setUp()
        self.app = gw.make_v1_app(_FakeServer(), _cfg())

    def get(self, query: str = "user_id=alice", headers=None):
        import json as _json

        status, _h, body = drive(self.app, method="GET", path="/v1/admin/policy?" + query,
                                 headers={"Authorization": "Bearer k-acme"} if headers is None
                                 else headers)
        try:
            return status, _json.loads(body.decode("utf-8"))
        except ValueError:
            return status, {}

    def post(self, payload, headers=None):
        import json as _json

        status, _h, body = drive(self.app, method="POST", path="/v1/admin/policy",
                                 headers={"Authorization": "Bearer k-acme"} if headers is None
                                 else headers,
                                 body=payload)
        try:
            return status, _json.loads(body.decode("utf-8"))
        except ValueError:
            return status, {}

    def test_it_needs_a_key(self) -> None:
        status, _ = self.get(headers=None if False else {})
        self.assertEqual(401, status)

    def test_it_reports_every_knob_with_where_its_value_came_from(self) -> None:
        status, body = self.get()
        self.assertEqual(200, status)
        self.assertEqual(len(tp.KNOBS), len(body["knobs"]))
        for name, knob in body["knobs"].items():
            with self.subTest(knob=name):
                self.assertIn(knob["source"], ("user", "tenant", "env", "default"))
                self.assertIn("settable_per_user", knob)

    def test_it_carries_the_knob_type_so_the_portal_renders_a_control(self) -> None:
        # A text box for a boolean is how a customer types "true" and gets a string.
        _status, body = self.get()
        self.assertEqual("int", body["knobs"]["top_k_per_layer"]["kind"])
        self.assertEqual("bool", body["knobs"]["generate_embeddings"]["kind"])
        self.assertTrue(body["knobs"]["top_k_per_layer"]["description"].strip())

    def test_it_lists_what_a_user_may_set(self) -> None:
        _status, body = self.get()
        self.assertEqual(sorted(tp.READ_PATH_KNOBS), body["settable_per_user"])

    def test_a_change_applies_and_is_reported_as_coming_from_the_user(self) -> None:
        status, body = self.post({"user_id": "alice", "settings": {"top_k_per_layer": 15}})
        self.assertEqual(200, status)
        self.assertEqual(["top_k_per_layer"], body["applied"])
        self.assertEqual(15, body["knobs"]["top_k_per_layer"]["value"])
        self.assertEqual("user", body["knobs"]["top_k_per_layer"]["source"])

    def test_a_write_path_setting_is_refused_and_named(self) -> None:
        # "Some settings were refused" leaves the customer to work out which.
        status, body = self.post({"user_id": "alice",
                                  "settings": {"top_k_per_layer": 15,
                                               "generate_embeddings": False}})
        self.assertEqual(200, status)
        self.assertEqual(["top_k_per_layer"], body["applied"])
        self.assertEqual(["generate_embeddings"], body["refused"])
        self.assertTrue(body["knobs"]["generate_embeddings"]["value"])

    def test_it_says_when_a_change_will_not_survive_a_restart(self) -> None:
        # Applied-but-not-saved must not read as saved. With no policy file configured there is
        # nowhere to write, and the customer has to be told rather than left to find out.
        _status, body = self.post({"user_id": "alice", "settings": {"top_k_per_layer": 15}})
        self.assertFalse(body["persisted"])
        self.assertIn("restart", body["persist_note"])

    def test_it_is_persisted_when_a_policy_file_is_configured(self) -> None:
        import json as _json
        import tempfile

        path = os.path.join(tempfile.mkdtemp(), "policy.json")
        with open(path, "w", encoding="utf-8") as handle:
            _json.dump({}, handle)
        os.environ["MATRIXARK_TENANT_POLICY_PATH"] = path
        self.addCleanup(os.environ.pop, "MATRIXARK_TENANT_POLICY_PATH", None)
        tp.clear_tenant_policy_cache()
        _status, body = self.post({"user_id": "alice", "settings": {"top_k_per_layer": 15}})
        self.assertTrue(body["persisted"])
        self.assertNotIn("persist_note", body)
        with open(path, encoding="utf-8") as handle:
            self.assertEqual(15, _json.load(handle)["users"]["acme"]["alice"]["top_k_per_layer"])

    def test_naming_no_user_is_refused(self) -> None:
        status, body = self.post({"settings": {"top_k_per_layer": 15}})
        self.assertEqual(400, status)
        self.assertEqual("no_user", body["error"])

    def test_an_empty_settings_object_is_refused(self) -> None:
        status, body = self.post({"user_id": "alice", "settings": {}})
        self.assertEqual(400, status)
        self.assertEqual("no_settings", body["error"])

    def test_the_tenant_comes_from_the_key_not_the_request(self) -> None:
        # Otherwise a caller reads or rewrites another tenant's settings by naming it.
        _status, body = self.post({"user_id": "alice", "tenant_id": "globex",
                                   "settings": {"top_k_per_layer": 15}})
        self.assertEqual("acme", body["tenant"])
        self.assertEqual(15, tp.resolve("top_k_per_layer",
                                        {"tenant_id": "acme", "user_id": "alice"}))
        self.assertEqual(tp.KNOBS["top_k_per_layer"].default,
                         tp.resolve("top_k_per_layer",
                                    {"tenant_id": "globex", "user_id": "alice"}))

    def test_it_explains_once_why_some_are_tenant_only(self) -> None:
        _status, body = self.get()
        self.assertIn("shared", body["why_some_are_tenant_only"])


class PolicyPageTest(unittest.TestCase):
    def test_the_setup_page_offers_the_per_user_section(self) -> None:
        app = gw.make_v1_app(_FakeServer(), _cfg())
        _st, _h, body = drive(app, method="GET", path="/v1/admin/setup")
        page = body.decode("utf-8")
        self.assertIn('id="policy"', page)
        self.assertIn('id="polUser"', page)
        self.assertIn("/v1/admin/policy", page)

    def test_a_knob_this_level_cannot_set_is_shown_and_says_where_to_set_it(self) -> None:
        # A customer looking for a setting has to find it and learn where it lives, rather than
        # conclude it does not exist. Asserted on the behaviour, not on one phrasing of it: the
        # control is replaced by text that names the other level.
        source = _read_builder()
        block = source[source.index("function policyControl("):
                       source.index("function renderPolicy(")]
        self.assertIn("if (!settableHere(name))", block)
        self.assertIn("tenant", block.split("if (!settableHere(name))")[1][:400])

    def test_it_sends_only_what_changed(self) -> None:
        # Sending the whole set would write a user override for every knob, and a later tenant
        # change would then never reach that user.
        source = _read_builder()
        block = source[source.index("function savePolicy("):
                       source.index('$("polLoad").addEventListener')]
        self.assertIn("!== String(knob.value)", block)


def _read_builder() -> str:
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "portal", "build_portal_pages.py"), encoding="utf-8") as handle:
        return handle.read()


class OneRegistryTest(_PolicyTest):
    """The per-user registry must not exist twice.

    This file is reachable as `matrixark_tenant_policy` and as `tools.matrixark_tenant_policy`, and
    Python treats those as two modules with their own module-level state. The tenant layer had
    exactly this bug -- half the writes went to the registry the reader was not looking at, and
    nothing said so. The per-user dict is new state in the same module, so it inherits the fix; this
    holds it to that rather than assuming.
    """

    def test_a_user_policy_is_visible_through_either_import_name(self) -> None:
        import importlib

        try:
            dotted = importlib.import_module("tools.matrixark_tenant_policy")
        except Exception:  # pragma: no cover - the package form is not always importable
            self.skipTest("the package import name is not available here")
        self.assertIs(tp, dotted, "the module exists twice, so its registries do too")
        tp.set_user_policy("acme", "alice", {"top_k_per_layer": 17})
        self.assertEqual(17, dotted.resolve("top_k_per_layer", ACME_ALICE))


class TenantLevelTest(_PolicyTest):
    """Setting for the whole tenant -- the layer nothing could reach from the portal."""

    def setUp(self) -> None:
        super().setUp()
        self.app = gw.make_v1_app(_FakeServer(), _cfg())

    def post(self, payload):
        import json as _json

        status, _h, body = drive(self.app, method="POST", path="/v1/admin/policy",
                                 headers={"Authorization": "Bearer k-acme"}, body=payload)
        try:
            return status, _json.loads(body.decode("utf-8"))
        except ValueError:
            return status, {}

    def test_a_tenant_may_set_a_write_path_knob(self) -> None:
        # A tenant owns its whole store, so deciding there is a decision somebody can make. The
        # same setting is refused for one user because two users writing one store under different
        # rules leave records of two shapes behind.
        status, body = self.post({"level": "tenant",
                                  "settings": {"generate_embeddings": False}})
        self.assertEqual(200, status)
        self.assertEqual(["generate_embeddings"], body["applied"])
        self.assertEqual([], body["refused"])
        self.assertFalse(tp.resolve("generate_embeddings", ACME_ALICE))

    def test_a_tenant_change_reaches_every_user_who_has_not_overridden_it(self) -> None:
        self.post({"level": "tenant", "settings": {"top_k_per_layer": 50}})
        self.assertEqual(50, tp.resolve("top_k_per_layer", ACME_ALICE))
        self.assertEqual(50, tp.resolve("top_k_per_layer", ACME_BOB))

    def test_a_user_override_still_wins_after_a_tenant_change(self) -> None:
        self.post({"level": "user", "user_id": "alice", "settings": {"top_k_per_layer": 9}})
        self.post({"level": "tenant", "settings": {"top_k_per_layer": 50}})
        self.assertEqual(9, tp.resolve("top_k_per_layer", ACME_ALICE))
        self.assertEqual(50, tp.resolve("top_k_per_layer", ACME_BOB))

    def test_a_tenant_change_needs_no_user(self) -> None:
        status, _body = self.post({"level": "tenant", "settings": {"top_k_per_layer": 50}})
        self.assertEqual(200, status)

    def test_an_unknown_level_is_refused(self) -> None:
        status, body = self.post({"level": "everyone", "settings": {"top_k_per_layer": 50}})
        self.assertEqual(400, status)
        self.assertEqual("bad_level", body["error"])

    def test_changing_a_write_path_knob_says_what_it_does_not_undo(self) -> None:
        # A setting can be put back; the records written under the old one keep their shape. Said
        # on the change that has that property, not on every change.
        _status, body = self.post({"level": "tenant",
                                   "settings": {"generate_embeddings": False}})
        self.assertIn("already_written_note", body)
        self.assertIn("re-ingesting", body["already_written_note"])

    def test_a_read_path_only_change_does_not_carry_that_note(self) -> None:
        # It would be false, and a note that appears on everything is one nobody reads.
        _status, body = self.post({"level": "tenant", "settings": {"top_k_per_layer": 50}})
        self.assertNotIn("already_written_note", body)

    def test_the_view_says_what_each_level_can_set(self) -> None:
        import json as _json

        status, _h, raw = drive(self.app, method="GET",
                                path="/v1/admin/policy?user_id=alice",
                                headers={"Authorization": "Bearer k-acme"})
        self.assertEqual(200, status)
        body = _json.loads(raw.decode("utf-8"))
        self.assertEqual(sorted(tp.KNOBS), body["settable_per_tenant"])
        self.assertEqual(sorted(tp.READ_PATH_KNOBS), body["settable_per_user"])
        self.assertLess(len(body["settable_per_user"]), len(body["settable_per_tenant"]))

    def test_a_tenant_change_is_persisted(self) -> None:
        import json as _json
        import tempfile

        path = os.path.join(tempfile.mkdtemp(), "policy.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{}")
        os.environ["MATRIXARK_TENANT_POLICY_PATH"] = path
        self.addCleanup(os.environ.pop, "MATRIXARK_TENANT_POLICY_PATH", None)
        tp.clear_tenant_policy_cache()
        _status, body = self.post({"level": "tenant", "settings": {"top_k_per_layer": 50}})
        self.assertTrue(body["persisted"])
        with open(path, encoding="utf-8") as handle:
            self.assertEqual(50, _json.load(handle)["tenants"]["acme"]["top_k_per_layer"])

    def test_both_levels_share_one_file_without_overwriting_each_other(self) -> None:
        import json as _json
        import tempfile

        path = os.path.join(tempfile.mkdtemp(), "policy.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{}")
        os.environ["MATRIXARK_TENANT_POLICY_PATH"] = path
        self.addCleanup(os.environ.pop, "MATRIXARK_TENANT_POLICY_PATH", None)
        tp.clear_tenant_policy_cache()
        self.post({"level": "tenant", "settings": {"top_k_per_layer": 50}})
        self.post({"level": "user", "user_id": "alice", "settings": {"max_selected_refs": 4}})
        with open(path, encoding="utf-8") as handle:
            document = _json.load(handle)
        self.assertEqual(50, document["tenants"]["acme"]["top_k_per_layer"])
        self.assertEqual(4, document["users"]["acme"]["alice"]["max_selected_refs"])


class PolicyPagePolishTest(unittest.TestCase):
    """Two things the page got wrong that only showed by driving it."""

    def test_the_save_button_names_the_person_typed_not_the_one_last_loaded(self) -> None:
        # Switching level does not reload, so the view still remembers whoever was loaded before --
        # and this is the label on the button that commits the change.
        source = _read_builder()
        block = source[source.index("var typed = "):source.index("$(\"policy\").innerHTML = (rows")]
        self.assertIn('$("polUser").value.trim()', block)
        self.assertIn("typed ||", block)

    def test_the_user_field_is_not_disabled_at_tenant_level(self) -> None:
        # It was disabled by a handler that only ran on change, so on first load it looked editable
        # and was ignored. It stays usable instead: at tenant level it chooses whose effective
        # values are shown, which is worth seeing while deciding a change for everyone.
        source = _read_builder()
        self.assertNotIn('$("polUser").disabled', source)
        self.assertIn("polLevelHint", source)

    def test_the_placeholder_says_what_the_field_is_for_at_each_level(self) -> None:
        source = _read_builder()
        block = source[source.index("function polLevelHint("):
                       source.index('$("polLevel").addEventListener')]
        self.assertIn("optional", block)


class PolicyPageLevelTest(unittest.TestCase):
    def test_the_page_offers_both_levels(self) -> None:
        app = gw.make_v1_app(_FakeServer(), _cfg())
        _st, _h, body = drive(app, method="GET", path="/v1/admin/setup")
        page = body.decode("utf-8")
        self.assertIn('id="polLevel"', page)
        self.assertIn("Everyone in this tenant", page)
        self.assertIn("One user", page)

    def test_the_page_asks_the_backend_which_knobs_this_level_can_set(self) -> None:
        # Keeping its own copy of the list is how the two drift and a customer is offered a
        # control that the backend then refuses.
        source = _read_builder()
        block = source[source.index("function settableHere("):
                       source.index("function loadPolicy(")]
        self.assertIn("settable_per_tenant", block)
        self.assertIn("settable_per_user", block)


class OverrideBadgeTest(_PolicyTest):
    """A deployment setting says when a tenant or user setting can win over it.

    It was said once, as a note on the `behaviour` group. The policy knobs are spread across TWO
    settings groups, so the three in `skills` said nothing -- and those three are exactly the ones
    a single user can set. The note also predated per-user overrides, so it named one of the two
    layers that can win.
    """

    def snapshot(self) -> dict:
        import matrixark_gateway_config as cfg

        return cfg.snapshot()

    def fields(self) -> dict:
        out = {}
        for _group, entries in (self.snapshot().get("groups") or {}).items():
            for entry in entries:
                out[entry["key"]] = entry
        return out

    def test_every_policy_knob_setting_says_a_tenant_can_override_it(self) -> None:
        import matrixark_gateway_config as cfg

        knob_envs = {k.env for k in tp.KNOBS.values()}
        fields = self.fields()
        checked = 0
        for setting in cfg.SETTINGS:
            if setting.env not in knob_envs:
                continue
            checked += 1
            with self.subTest(setting=setting.key):
                self.assertIn("tenant", fields[setting.key]["overridable_by"])
        self.assertGreater(checked, 20, "almost nothing was checked, so this passed vacuously")

    def test_a_read_path_knob_also_says_a_user_can(self) -> None:
        fields = self.fields()
        self.assertEqual(["tenant", "user"], fields["skills.chunks_per_skill"]["overridable_by"])

    def test_a_write_path_knob_says_tenant_only(self) -> None:
        import matrixark_gateway_config as cfg

        write_env = tp.KNOBS["generate_embeddings"].env
        key = next(s.key for s in cfg.SETTINGS if s.env == write_env)
        self.assertEqual(["tenant"], self.fields()[key]["overridable_by"])

    def test_a_setting_that_is_not_a_policy_knob_claims_nothing(self) -> None:
        # Most of the 79 exist only at the deployment level, and a badge on those would be false.
        self.assertEqual([], self.fields()["skills.discovery"]["overridable_by"])

    def test_the_badge_reaches_both_groups_the_knobs_live_in(self) -> None:
        # The bug in the group note, stated as a test: a knob in `skills` must carry it too.
        groups_with_badge = set()
        for group, entries in (self.snapshot().get("groups") or {}).items():
            for entry in entries:
                if entry.get("overridable_by"):
                    groups_with_badge.add(group)
        self.assertIn("skills", groups_with_badge)
        self.assertIn("behaviour", groups_with_badge)

    def test_it_is_derived_from_the_registry_not_written_per_group(self) -> None:
        # So a knob that moves group keeps its badge, and one added later gets it.
        source = _read_config_source()
        self.assertIn("def _override_layers_by_env", source)
        block = source[source.index("def _override_layers_by_env"):source.index("def snapshot(")]
        self.assertIn("policy.KNOBS.values()", block)

    def test_the_page_renders_it(self) -> None:
        app = gw.make_v1_app(_FakeServer(), _cfg())
        _st, _h, body = drive(app, method="GET", path="/v1/admin/setup")
        self.assertIn("can override", body.decode("utf-8"))


def _read_config_source() -> str:
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "matrixark_gateway_config.py"), encoding="utf-8") as handle:
        return handle.read()


if __name__ == "__main__":
    unittest.main()

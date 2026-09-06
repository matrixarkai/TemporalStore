#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The customer portal's HTTP surface: setup/catalog pages, config writes, catalog reads, metrics.

Reuses the ASGI harness from the main gateway suite so these exercise the same app construction the
rest of the routes are tested through.
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


class _PortalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _FakeServer()
        self._saved_env = dict(os.environ)
        self._saved_boot = dict(cfgmod._BOOT_ENV)
        # Derived from the registry rather than written down. The list here named four prefixes;
        # the registry declares two, MATRIXARK_ for 98 settings and TS_ for 19, and TS_ was not
        # one of the four. Those nineteen survived the isolation, so on a box that exports them --
        # or one whose stored config seeds them through apply_boot -- this suite asserted against
        # the machine it ran on. A written list drifts from the registry; a derived one cannot.
        declared = {setting.env for setting in cfgmod.SETTINGS if setting.env}
        for name in list(os.environ):
            if name in declared or name.startswith(("MATRIXARK_", "DEEPSEEK_", "OPENAI_",
                                                    "LOCAL_ENCODER_")):
                del os.environ[name]
        cfgmod._BOOT_ENV.clear()
        self._dir = tempfile.TemporaryDirectory()
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(self._dir.name, "runtime.json")
        # After the isolation, not before. make_v1_app() calls apply_boot(), which seeds the
        # stored document into the environment; building the app first meant seeding from
        # whatever configuration the machine happened to have.
        self.app = gw.make_v1_app(self.server, _cfg())
        # The metric registry is process-wide; start each test from zero so counts are assertable.
        metricsmod.METRICS.__init__()  # type: ignore[misc]

    def tearDown(self) -> None:
        self._dir.cleanup()
        os.environ.clear()
        os.environ.update(self._saved_env)
        cfgmod._BOOT_ENV.clear()
        cfgmod._BOOT_ENV.update(self._saved_boot)


PORTAL_PAGES = ("/v1/admin", "/v1/admin/setup", "/v1/admin/catalog", "/v1/admin/explore",
                "/v1/admin/ingestion", "/v1/admin/portal")


class PortalPagesTest(_PortalTest):
    def test_the_pages_are_served_without_auth_and_carry_the_shared_nav(self) -> None:
        # Same posture as the key portal: fetching the page needs nothing because every action on it
        # calls an admin-gated endpoint, so the page is inert without a key.
        for path, marker in (("/v1/admin", "MatrixArk"),
                             ("/v1/admin/setup", "Setup"),
                             ("/v1/admin/catalog", "Skills"),
                             ("/v1/admin/explore", "Explore")):
            with self.subTest(path=path):
                status, headers, body = drive(self.app, method="GET", path=path)
                self.assertEqual(200, status)
                self.assertTrue(headers["content-type"].startswith("text/html"))
                text = body.decode("utf-8")
                self.assertIn(marker, text)
                self.assertIn('class="portalnav"', text)

    def test_every_page_links_to_every_other_page(self) -> None:
        # The nav is generated into each page separately, so adding a page has to update the others.
        # Without this, the pages that existed first keep a nav that does not mention the new one --
        # which is exactly what happened, and is invisible unless you open an older page.
        for path in PORTAL_PAGES:
            _st, _h, body = drive(self.app, method="GET", path=path)
            text = body.decode("utf-8")
            for link in PORTAL_PAGES:
                with self.subTest(page=path, link=link):
                    self.assertIn('href="%s"' % link, text)

    def test_the_current_page_is_marked_in_its_own_nav(self) -> None:
        for path in PORTAL_PAGES:
            with self.subTest(path=path):
                _st, _h, body = drive(self.app, method="GET", path=path)
                self.assertIn('href="%s" aria-current="page"' % path, body.decode("utf-8"))


class OverviewTest(_PortalTest):
    def test_the_readiness_report_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="GET", path="/v1/admin/overview")
        self.assertEqual(401, status)

    def test_a_bare_deployment_reports_the_model_work_as_still_to_do(self) -> None:
        status, _, body = drive(self.app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        self.assertEqual(200, status)
        payload = json.loads(body)
        checks = {c["id"]: c for c in payload["checks"]}
        self.assertEqual("todo", checks["extraction"]["status"])
        self.assertEqual("todo", checks["embedding"]["status"])
        self.assertEqual("todo", checks["ingestion_root"]["status"])
        self.assertFalse(payload["ready"])
        self.assertEqual(payload["total"], len(payload["checks"]))
        # Every item points somewhere a customer can act, or explains itself without one.
        for check in payload["checks"]:
            self.assertTrue(check["detail"].strip(), check["id"])

    def test_a_configured_deployment_reports_ready(self) -> None:
        os.environ.update({
            "MATRIXARK_EXTRACTION_PROVIDER": "openai_compatible",
            "MATRIXARK_EXTRACTION_BASE_URL": "https://api.deepseek.com/v1",
            "MATRIXARK_EXTRACTION_MODEL": "deepseek-chat",
            "MATRIXARK_EXTRACTION_API_KEY_ENV": "DEEPSEEK_API_KEY",
            "DEEPSEEK_API_KEY": "present",
            "MATRIXARK_EMBEDDING_PROVIDER": "openai_compatible",
            "MATRIXARK_EMBEDDING_API_BASE": "http://127.0.0.1:8400/v1",
            "MATRIXARK_EMBEDDING_API_KEY_ENV": "LOCAL_ENCODER_KEY",
            "LOCAL_ENCODER_KEY": "placeholder",
            "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS": "1",
            "MATRIXARK_INGESTION_ROOT": "/srv/docs",
        })
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        payload = json.loads(body)
        checks = {c["id"]: c for c in payload["checks"]}
        self.assertEqual("ok", checks["extraction"]["status"])
        self.assertEqual("ok", checks["embedding"]["status"])
        self.assertEqual("ok", checks["fail_closed"]["status"])
        self.assertEqual("ok", checks["ingestion_root"]["status"])
        self.assertEqual("ok", checks["config_warnings"]["status"])

    def test_a_backend_that_cannot_answer_still_returns_the_config_half(self) -> None:
        # The moment an operator most needs the checklist is when the backend is the broken thing;
        # a listing failure must not take the whole report down.
        def explode(name, args):
            raise RuntimeError("backend down")

        self.server.call_tool = explode  # type: ignore[assignment]
        status, _, body = drive(self.app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        self.assertEqual(200, status)
        payload = json.loads(body)
        self.assertIsNone(payload["counts"]["skills"])
        self.assertIn("extraction", {c["id"] for c in payload["checks"]})

    def test_the_three_listings_run_together(self) -> None:
        """Each listing walks the record log, so in sequence the page waits for the sum.

        Timed rather than asserted structurally: what matters is the wall clock a customer waits,
        and a refactor that quietly reintroduces `await` in a loop would keep any structural check
        passing. The margin is wide enough that a loaded box cannot fail it -- three sequential
        60 ms calls take 180 ms, and this allows 150 ms.
        """
        import time as _time

        def slow(_name, _args):
            _time.sleep(0.06)
            return {"status": "ok", "count": 0}

        self.server.call_tool = slow  # type: ignore[assignment]
        started = _time.time()
        status, _hdrs, _body = drive(self.app, method="GET", path="/v1/admin/overview",
                                     headers=ADMIN)
        elapsed = _time.time() - started
        self.assertEqual(200, status)
        self.assertLess(elapsed, 0.15,
                        "three 60ms listings took %.0f ms -- they are running one at a time"
                        % (elapsed * 1000))

    def test_the_blocking_items_say_how_to_fix_them(self) -> None:
        # "No model is configured" says what is wrong and leaves the customer to work out where
        # the setting lives, what the endpoint must look like, and which variable the key goes in.
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        checks = {c["id"]: c for c in json.loads(body)["checks"]}
        for check_id in ("extraction", "embedding", "ingestion_root"):
            with self.subTest(check=check_id):
                self.assertNotEqual("ok", checks[check_id]["status"])
                steps = checks[check_id]["how"]
                self.assertTrue(steps, "%s is unfinished and says nothing about how" % check_id)
                for step in steps:
                    self.assertTrue(step.strip())

    def test_an_unauthenticated_deployment_says_how_to_close_it(self) -> None:
        # The default fixture enforces auth, so this needs its own app: the whole point of the
        # item is the deployment that is answering anonymous requests.
        app = gw.make_v1_app(self.server, _cfg(require_auth=False))
        _st, _h, body = drive(app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        auth_check = {c["id"]: c for c in json.loads(body)["checks"]}["auth"]
        self.assertEqual("warn", auth_check["status"])
        joined = " ".join(auth_check["how"])
        self.assertIn("MATRIXARK_REQUIRE_AUTH=1", joined)
        # Turning auth on without keys locks the operator out along with everyone else.
        self.assertIn("Issue keys first", joined)

    def test_a_finished_item_carries_no_steps(self) -> None:
        # The useful thing about a green item is that there is nothing to do; a list of steps under
        # one reads as work still outstanding.
        os.environ.update({
            "MATRIXARK_EXTRACTION_PROVIDER": "openai_compatible",
            "MATRIXARK_EXTRACTION_BASE_URL": "https://api.deepseek.com/v1",
            "MATRIXARK_EXTRACTION_MODEL": "deepseek-chat",
            "MATRIXARK_EXTRACTION_API_KEY_ENV": "DEEPSEEK_API_KEY",
            "DEEPSEEK_API_KEY": "present",
            "MATRIXARK_EMBEDDING_PROVIDER": "openai_compatible",
            "MATRIXARK_EMBEDDING_API_BASE": "http://127.0.0.1:8400/v1",
            "MATRIXARK_EMBEDDING_API_KEY_ENV": "LOCAL_ENCODER_KEY",
            "LOCAL_ENCODER_KEY": "placeholder",
            "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS": "1",
            "MATRIXARK_INGESTION_ROOT": "/srv/docs",
        })
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        for check in json.loads(body)["checks"]:
            if check["status"] == "ok":
                with self.subTest(check=check["id"]):
                    self.assertEqual([], check["how"])

    def test_the_warnings_item_carries_the_warnings_themselves(self) -> None:
        # Its steps ARE the warnings: restating them in other words would be a second copy to
        # drift from.
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        payload = json.loads(body)
        checks = {c["id"]: c for c in payload["checks"]}
        self.assertEqual(payload["config"]["warnings"], checks["config_warnings"]["how"])

    def test_the_report_never_carries_a_secret(self) -> None:
        secret = "sk-overview-must-not-leak"
        os.environ["MATRIXARK_EXTRACTION_API_KEY_ENV"] = "DEEPSEEK_API_KEY"
        os.environ["DEEPSEEK_API_KEY"] = secret
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        self.assertNotIn(secret.encode(), body)


class UsersReadTest(_PortalTest):
    def test_get_users_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="GET", path="/v1/users")
        self.assertEqual(401, status)
        self.assertEqual([], self.server.calls)

    def test_get_users_reaches_the_listing_tool_with_a_clamped_limit(self) -> None:
        status, _, _ = drive(self.app, method="GET",
                             path="/v1/users?user_id=alice&limit=99999", headers=ADMIN)
        self.assertEqual(200, status)
        name, args = self.server.calls[0]
        self.assertEqual("matrixark_list_users", name)
        self.assertEqual("alice", args["scope"]["user_id"])
        self.assertEqual(500, args["limit"])


class SkillUpdateTest(_PortalTest):
    def test_disabling_a_skill_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="POST", path="/v1/skills/update",
                             body={"skill_hash": 111, "status": "disabled"})
        self.assertEqual(401, status)
        self.assertEqual([], self.server.calls)

    def test_a_missing_skill_hash_is_a_400(self) -> None:
        status, _, body = drive(self.app, method="POST", path="/v1/skills/update",
                                headers=ADMIN, body={"status": "disabled"})
        self.assertEqual(400, status)
        self.assertEqual("bad_request", json.loads(body)["error"])

    def test_the_update_reaches_the_backend_with_only_the_fields_it_understands(self) -> None:
        status, _, _ = drive(self.app, method="POST", path="/v1/skills/update", headers=ADMIN,
                             body={"skill_hash": 111, "status": "disabled",
                                   "precedence": "low", "sneaky": "ignored"})
        self.assertEqual(200, status)
        name, args = self.server.calls[0]
        self.assertEqual("matrixark_update_skill", name)
        self.assertEqual(111, args["skill_hash"])
        self.assertEqual("disabled", args["status"])
        self.assertNotIn("sneaky", args)


class ConfigWriteTest(_PortalTest):
    def test_a_write_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="POST", path="/v1/admin/config",
                             body={"settings": {"extraction.model": "deepseek-chat"}})
        self.assertEqual(401, status)
        self.assertNotIn("MATRIXARK_EXTRACTION_MODEL", os.environ)

    def test_a_write_applies_persists_and_reports_restart_requirements(self) -> None:
        status, _, body = drive(
            self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
            body={"settings": {"extraction.provider": "openai_compatible",
                               "extraction.base_url": "https://api.deepseek.com/v1",
                               "extraction.model": "deepseek-chat",
                               "extraction.api_key_env": "DEEPSEEK_API_KEY",
                               "extraction.api_key": "sk-live-secret",
                               "embedding.api_base": "http://127.0.0.1:8400/v1"}})
        self.assertEqual(200, status)
        payload = json.loads(body)
        self.assertEqual("ok", payload["status"])
        # The key is live on the next call (the request path reads the variable per call); the
        # endpoint and model are captured at import, so they are persisted but need a restart.
        self.assertIn("extraction.base_url", payload["restart_required"])
        self.assertIn("extraction.model", payload["restart_required"])
        self.assertNotIn("extraction.api_key", payload["restart_required"])
        self.assertEqual("sk-live-secret", os.environ["DEEPSEEK_API_KEY"])
        self.assertEqual("deepseek-chat", cfgmod.load()["values"]["extraction.model"])

    def test_no_response_on_this_surface_ever_echoes_a_secret(self) -> None:
        secret = "sk-this-value-must-never-appear"
        _, _, write_body = drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
                                 body={"settings": {"extraction.api_key_env": "DEEPSEEK_API_KEY",
                                                    "extraction.api_key": secret}})
        _, _, read_body = drive(self.app, method="GET", path="/v1/admin/config", headers=ADMIN)
        _, _, metrics_body = drive(self.app, method="GET", path="/v1/metrics")
        for label, payload in (("write", write_body), ("read", read_body),
                               ("metrics", metrics_body)):
            with self.subTest(response=label):
                self.assertNotIn(secret.encode(), payload)

    def test_an_unknown_setting_is_a_400_not_a_500(self) -> None:
        status, _, body = drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
                                body={"settings": {"PATH": "/tmp"}})
        self.assertEqual(400, status)
        self.assertEqual("unknown_setting", json.loads(body)["error"])

    def test_an_invalid_value_is_a_400(self) -> None:
        status, _, body = drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
                                body={"settings": {"extraction.max_tokens": "lots"}})
        self.assertEqual(400, status)
        self.assertEqual("invalid_value", json.loads(body)["error"])

    def test_the_read_carries_the_writable_registry(self) -> None:
        status, _, body = drive(self.app, method="GET", path="/v1/admin/config", headers=ADMIN)
        self.assertEqual(200, status)
        payload = json.loads(body)
        self.assertIn("extraction", payload["settings"]["groups"])
        self.assertIn("deepseek", payload["settings"]["presets"])
        # The pre-existing read-side contract is unchanged.
        self.assertIn("warnings", payload)
        self.assertIn("provider", payload["extraction"])

    def test_a_preset_configures_the_deployment(self) -> None:
        status, _, body = drive(self.app, method="POST", path="/v1/admin/config/preset",
                                headers=ADMIN, body={"preset": "deepseek"})
        self.assertEqual(200, status)
        self.assertEqual("https://api.deepseek.com/v1", os.environ["MATRIXARK_EXTRACTION_BASE_URL"])
        self.assertEqual("deepseek", json.loads(body)["preset"])

    def test_an_unknown_preset_is_a_400(self) -> None:
        status, _, body = drive(self.app, method="POST", path="/v1/admin/config/preset",
                                headers=ADMIN, body={"preset": "nope"})
        self.assertEqual(400, status)
        self.assertEqual("unknown_preset", json.loads(body)["error"])

    def test_the_probe_reports_a_deterministic_deployment_without_dialling(self) -> None:
        status, _, body = drive(self.app, method="POST", path="/v1/admin/config/test",
                                headers=ADMIN, body={})
        self.assertEqual(200, status)
        payload = json.loads(body)
        self.assertFalse(payload["all_ok"])
        self.assertTrue(all(r.get("skipped") for r in payload["results"]))

    def test_the_probe_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="POST", path="/v1/admin/config/test", body={})
        self.assertEqual(401, status)


class CatalogRoutesTest(_PortalTest):
    def test_the_catalog_reads_need_a_key(self) -> None:
        for path in ("/v1/skills", "/v1/resources"):
            with self.subTest(path=path):
                status, _, _ = drive(self.app, method="GET", path=path)
                self.assertEqual(401, status)
                self.assertEqual([], self.server.calls)

    def test_skills_and_resources_reach_the_backend_listing_tools(self) -> None:
        status, _, _ = drive(self.app, method="GET", path="/v1/skills?user_id=alice",
                             headers=ADMIN)
        self.assertEqual(200, status)
        status, _, _ = drive(self.app, method="GET",
                             path="/v1/resources?user_id=alice&resource_type=md", headers=ADMIN)
        self.assertEqual(200, status)
        names = [name for name, _args in self.server.calls]
        self.assertEqual(["matrixark_list_skills", "matrixark_list_resources"], names)
        skills_args = self.server.calls[0][1]
        self.assertEqual("alice", skills_args["scope"]["user_id"])
        self.assertEqual("md", self.server.calls[1][1]["resource_type"])

    def test_a_client_cannot_ask_for_an_unbounded_listing(self) -> None:
        # A caller-supplied limit reaches a full-store scan in the adapter; clamp it at the edge.
        drive(self.app, method="GET", path="/v1/skills?limit=100000", headers=ADMIN)
        self.assertEqual(500, self.server.calls[0][1]["limit"])

    def test_include_disabled_is_off_unless_asked_for(self) -> None:
        drive(self.app, method="GET", path="/v1/skills", headers=ADMIN)
        self.assertNotIn("include_disabled", self.server.calls[0][1])
        drive(self.app, method="GET", path="/v1/skills?include_disabled=1", headers=ADMIN)
        self.assertTrue(self.server.calls[1][1]["include_disabled"])


class ConfigExportTest(_PortalTest):
    def test_export_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="GET", path="/v1/admin/config/export")
        self.assertEqual(401, status)

    def test_the_export_round_trips_through_the_write_endpoint(self) -> None:
        # The point of the export is "make that deployment match this one", so what comes out has
        # to be exactly what the write endpoint takes in.
        drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
              body={"settings": {"embedding.provider": "openai_compatible",
                                 "embedding.api_base": "http://127.0.0.1:8400/v1",
                                 "embedding.model": "minilm",
                                 "retrieval.min_score": "0.35"}})
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/config/export",
                              headers=ADMIN)
        exported = json.loads(body)
        self.assertEqual("0.35", exported["settings"]["retrieval.min_score"])

        # Apply it to a "different deployment": a clean config file and a clean environment.
        for name in list(os.environ):
            if name.startswith("MATRIXARK_"):
                del os.environ[name]
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(self._dir.name, "other.json")
        status, _, _ = drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
                             body={"settings": exported["settings"]})
        self.assertEqual(200, status)
        self.assertEqual("minilm", os.environ["MATRIXARK_EMBEDDING_MODEL"])
        self.assertEqual("0.35", os.environ["MATRIXARK_RETRIEVAL_MIN_SCORE"])

    def test_a_secret_is_omitted_rather_than_blanked(self) -> None:
        # A blank would be a WRITE that clears the target's working key, so an import that "just
        # applied everything" would break the deployment it was meant to configure.
        secret = "sk-must-not-travel"
        drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
              body={"settings": {"extraction.api_key_env": "DEEPSEEK_API_KEY",
                                 "extraction.api_key": secret}})
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/config/export",
                              headers=ADMIN)
        exported = json.loads(body)
        self.assertNotIn(secret, body.decode("utf-8"))
        self.assertNotIn("extraction.api_key", exported["settings"])
        self.assertIn("extraction.api_key", exported["secrets_omitted"])

    def test_defaults_are_left_out_unless_asked_for(self) -> None:
        _st, _h, body = drive(self.app, method="GET", path="/v1/admin/config/export",
                              headers=ADMIN)
        self.assertEqual({}, json.loads(body)["settings"])
        _st, _h, body = drive(self.app, method="GET",
                              path="/v1/admin/config/export?include_defaults=1", headers=ADMIN)
        self.assertGreater(len(json.loads(body)["settings"]), 50)


class ConfigChangeMetricsTest(_PortalTest):
    def test_a_write_moves_the_change_timestamp(self) -> None:
        _st, _h, body = drive(self.app, method="GET", path="/v1/metrics")
        self.assertIn("matrixark_gateway_config_changed_timestamp_seconds 0",
                      body.decode("utf-8"))
        drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
              body={"settings": {"retrieval.min_score": "0.4"}})
        _st, _h, body = drive(self.app, method="GET", path="/v1/metrics")
        text = body.decode("utf-8")
        line = [ln for ln in text.splitlines()
                if ln.startswith("matrixark_gateway_config_changed_timestamp_seconds ")][0]
        self.assertGreater(float(line.rsplit(" ", 1)[1]), 1_600_000_000)
        self.assertIn("matrixark_gateway_settings_overridden 1", text)

    def test_the_change_metrics_never_carry_a_value(self) -> None:
        secret = "sk-metrics-must-not-leak"
        drive(self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
              body={"settings": {"extraction.api_key": secret,
                                 "extraction.model": "deepseek-chat"}})
        _st, _h, body = drive(self.app, method="GET", path="/v1/metrics")
        text = body.decode("utf-8")
        self.assertNotIn(secret, text)
        self.assertNotIn("deepseek-chat", text)


class ScopeCatalogTest(_PortalTest):
    def test_the_catalogue_needs_a_key(self) -> None:
        status, _, _ = drive(self.app, method="GET", path="/v1/admin/scopes")
        self.assertEqual(401, status)

    def test_it_describes_every_scope_the_backend_gates_on(self) -> None:
        # A scope the backend enforces and the catalogue does not describe is one a customer cannot
        # discover: the create-key form would never offer it, and a key issued without it fails
        # with a 403 that names a string appearing nowhere in the portal.
        from matrixark_mcp_core import MATRIXARK_TOOL_SCOPES
        enforced = {s for scopes in MATRIXARK_TOOL_SCOPES.values() for s in scopes}
        described = {entry["scope"] for entry in gw.SCOPE_CATALOG}
        self.assertEqual(set(), enforced - described,
                         "scopes the backend enforces but the portal never explains")

    def test_it_describes_nothing_the_backend_does_not_know(self) -> None:
        # The other direction: a scope offered here that the backend never checks is one a customer
        # can grant to no effect, and would then trust.
        from matrixark_mcp_core import MATRIXARK_TOOL_SCOPES
        enforced = {s for scopes in MATRIXARK_TOOL_SCOPES.values() for s in scopes}
        described = {entry["scope"] for entry in gw.SCOPE_CATALOG}
        self.assertEqual(set(), described - enforced,
                         "scopes the portal offers that nothing gates on")

    def test_every_preset_grants_only_real_scopes(self) -> None:
        described = {entry["scope"] for entry in gw.SCOPE_CATALOG}
        for preset in gw.SCOPE_PRESETS:
            with self.subTest(preset=preset["id"]):
                self.assertTrue(set(preset["scopes"]).issubset(described))
                self.assertTrue(preset["detail"].strip())

    def test_the_serving_presets_cannot_delete(self) -> None:
        # An agent key that can reset a tenant's memory is a support incident waiting to happen;
        # deletion is a deliberate choice, not something a starting point hands out.
        for preset in gw.SCOPE_PRESETS:
            if preset["id"] in ("agent", "read_only", "ingest"):
                with self.subTest(preset=preset["id"]):
                    self.assertNotIn("context:forget", preset["scopes"])

    def test_the_catalogue_is_served(self) -> None:
        status, _, body = drive(self.app, method="GET", path="/v1/admin/scopes", headers=ADMIN)
        self.assertEqual(200, status)
        payload = json.loads(body)
        self.assertTrue(payload["scopes"])
        self.assertEqual({"agent", "read_only", "ingest", "admin"},
                         {p["id"] for p in payload["presets"]})


class MetricsRouteTest(_PortalTest):
    def test_edge_traffic_shows_up_in_the_scrape(self) -> None:
        drive(self.app, method="POST", path="/v1/ingest", headers=ADMIN, body={"records": [1]})
        drive(self.app, method="POST", path="/v1/ingest", body={"records": [1]})  # 401
        status, headers, body = drive(self.app, method="GET", path="/v1/metrics")
        self.assertEqual(200, status)
        self.assertTrue(headers["content-type"].startswith("text/plain"))
        text = body.decode("utf-8")
        self.assertIn('matrixark_gateway_requests_total{route="/v1/ingest",method="POST",'
                      'status="202"} 1', text)
        self.assertIn('matrixark_gateway_requests_total{route="/v1/ingest",method="POST",'
                      'status="401"} 1', text)
        self.assertIn('matrixark_gateway_request_duration_seconds_count{route="/v1/ingest"} 2', text)

    def test_the_scrape_still_needs_no_credentials_and_carries_no_identity(self) -> None:
        drive(self.app, method="POST", path="/v1/ingest", headers=ADMIN, body={"records": [1]})
        _, _, body = drive(self.app, method="GET", path="/v1/metrics")
        text = body.decode("utf-8")
        self.assertNotIn("k-acme", text)
        self.assertNotIn("acme", text)

    def test_config_health_is_exported_as_alertable_gauges(self) -> None:
        _, _, body = drive(self.app, method="GET", path="/v1/metrics")
        text = body.decode("utf-8")
        self.assertIn("matrixark_gateway_embedding_semantic 0", text)
        self.assertIn("matrixark_gateway_extraction_model_active 0", text)
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "openai_compatible"
        _, _, body = drive(self.app, method="GET", path="/v1/metrics")
        self.assertIn("matrixark_gateway_embedding_semantic 1", body.decode("utf-8"))

    def test_the_ingestion_counters_are_still_present(self) -> None:
        _, _, body = drive(self.app, method="GET", path="/v1/metrics")
        self.assertIn("matrixark_ingestion_jobs_total", body.decode("utf-8"))

    def test_a_path_a_client_invents_does_not_create_a_series(self) -> None:
        drive(self.app, method="GET", path="/v1/" + "x" * 40, headers=ADMIN)
        _, _, body = drive(self.app, method="GET", path="/v1/metrics")
        text = body.decode("utf-8")
        self.assertNotIn("x" * 40, text)
        self.assertIn('route="other"', text)


class AdminWritesNeedAScopeThatMayWriteTest(_PortalTest):
    """`admin:audit` is "Read the audit log" in the catalogue this gateway serves, and it was
    authorising configuration writes and ingestion.

    Every refusal below is paired with the same key doing something it is entitled to do, because
    a key that is refused everything would satisfy the refusals on its own.
    """

    AUDIT = "testkey_audit_only"        # "Read the audit log", and nothing else
    MANAGE = "testkey_manages_keys"     # admin:api_key -- what the portal's own actions carry
    LEGACY = "testkey_legacy_plain"     # no scopes at all: unrestricted, by documented design

    def setUp(self) -> None:
        super().setUp()
        hashed = {
            gw._secret_hash(self.AUDIT): {
                "tenant_id": "t", "account_id": "acct", "scopes": ["admin:audit"]},
            gw._secret_hash(self.MANAGE): {
                "tenant_id": "t", "account_id": "acct", "scopes": ["admin:api_key"]},
            gw._secret_hash(self.LEGACY): {"tenant_id": "t", "account_id": "acct"},
        }
        self.app = gw.make_v1_app(
            self.server, gw.GatewayConfig.from_env({"enforced": True, "hashed_api_keys": hashed}))

    def _as(self, key):
        return {"Authorization": "Bearer " + key}

    def _post(self, key, path, body):
        return drive(self.app, method="POST", path=path, headers=self._as(key), body=body)

    SETTINGS = {"settings": {"extraction.model": "deepseek-chat"}}

    # ---- the control: the reading key is a working key ---------------------------------------

    def test_an_audit_key_still_reads_what_it_is_for(self) -> None:
        status, _, _ = drive(self.app, method="GET", path="/v1/admin/api_key_usage",
                             headers=self._as(self.AUDIT))
        self.assertEqual(200, status)

    # ---- what it must not do ------------------------------------------------------------------

    def test_an_audit_key_cannot_rewrite_the_configuration(self) -> None:
        status, _, body = self._post(self.AUDIT, "/v1/admin/config", self.SETTINGS)
        self.assertEqual(403, status)
        payload = json.loads(body)
        self.assertEqual("insufficient_scope", payload["error"])
        self.assertEqual(["admin:api_key"], payload["required"])
        self.assertNotIn("MATRIXARK_EXTRACTION_MODEL", os.environ)

    def test_an_audit_key_cannot_apply_a_preset(self) -> None:
        status, _, _ = self._post(self.AUDIT, "/v1/admin/config/preset", {"preset": "deepseek"})
        self.assertEqual(403, status)

    def test_an_audit_key_cannot_start_an_import(self) -> None:
        status, _, _ = self._post(self.AUDIT, "/v1/admin/ingestion/jobs", {"paths": ["."]})
        self.assertEqual(403, status)

    def test_an_audit_key_cannot_submit_records(self) -> None:
        status, _, _ = self._post(self.AUDIT, "/v1/admin/ingestion/records",
                                  {"records": [{"text": "hello"}]})
        self.assertEqual(403, status)

    # ---- what a managing key may still do -----------------------------------------------------

    def test_a_managing_key_may_rewrite_the_configuration(self) -> None:
        """Otherwise the checks above would pass just as well with the route bricked."""
        status, _, _ = self._post(self.MANAGE, "/v1/admin/config", self.SETTINGS)
        self.assertNotEqual(403, status)

    # ---- the two POSTs that change nothing ----------------------------------------------------

    def test_a_reading_key_may_still_ask_what_a_plan_would_do(self) -> None:
        """It composes a plan without touching this process; needing a write scope to look would
        be its own mistake."""
        status, _, _ = self._post(self.AUDIT, "/v1/admin/deployment/plan", {})
        self.assertNotEqual(403, status)

    def test_a_reading_key_may_still_probe_the_configured_endpoints(self) -> None:
        status, _, _ = self._post(self.AUDIT, "/v1/admin/config/test", {})
        self.assertNotEqual(403, status)

    # ---- the posture that is deliberately unchanged --------------------------------------------

    def test_a_legacy_unrestricted_key_is_unaffected(self) -> None:
        """A key with no scopes is unrestricted everywhere else at this edge, and narrowing it
        here would change what those deployments can do without anybody asking."""
        status, _, _ = self._post(self.LEGACY, "/v1/admin/config", self.SETTINGS)
        self.assertNotEqual(403, status)


class TheWriteSaysHowManyWorkersTest(_PortalTest):
    """A live setting is applied to the environment of the worker that served the write, and read
    per call from the environment of whichever worker serves the next request. The answer carries
    the count so the page can say how far the write reached instead of claiming "live now"."""

    def test_the_response_carries_the_worker_count(self) -> None:
        status, _, body = drive(
            self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
            body={"settings": {"extraction.model": "deepseek-chat"}})
        self.assertEqual(200, status)
        self.assertIn("workers", json.loads(body))

    def test_it_is_a_number_and_never_zero(self) -> None:
        """The page branches on "more than one"; a zero would read as a single worker on a
        deployment running eight."""
        _s, _h, body = drive(
            self.app, method="POST", path="/v1/admin/config", headers=ADMIN,
            body={"settings": {"extraction.model": "deepseek-chat"}})
        workers = json.loads(body)["workers"]
        self.assertIsInstance(workers, int)
        self.assertGreaterEqual(workers, 1)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The write side of /v1/admin/config: a closed registry, write-only secrets, honest apply.

The three properties that make this endpoint safe to hand a customer, each pinned here:

* only registered keys can be written (it is not a general environment-variable setter),
* a secret VALUE never comes back out of a read, and
* a write that cannot take effect until a restart says so, because several of these variables are
  captured into module constants at import time and a write reported as live when it is not is how
  a deployment ends up "configured for DeepSeek" while ingest still runs the local rules.
"""
from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402


class _Sandbox(unittest.TestCase):
    """Each test gets its own config file and a clean MATRIXARK_* environment."""

    def setUp(self) -> None:
        self._saved_env = dict(os.environ)
        self._saved_boot = dict(cfgmod._BOOT_ENV)
        for name in list(os.environ):
            if name.startswith(("MATRIXARK_", "DEEPSEEK_", "OPENAI_", "LOCAL_ENCODER_")):
                del os.environ[name]
        cfgmod._BOOT_ENV.clear()
        self._dir = tempfile.TemporaryDirectory()
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(self._dir.name, "runtime.json")

    def tearDown(self) -> None:
        self._dir.cleanup()
        os.environ.clear()
        os.environ.update(self._saved_env)
        cfgmod._BOOT_ENV.clear()
        cfgmod._BOOT_ENV.update(self._saved_boot)


class ClosedRegistryTest(_Sandbox):
    def test_an_unregistered_key_is_refused(self) -> None:
        # The whole safety argument for exposing this to a customer is that it cannot set arbitrary
        # environment variables -- most of all not PATH or LD_PRELOAD.
        for bad in ("PATH", "LD_PRELOAD", "TS_STORAGE_BACKEND", "extraction.nonsense"):
            with self.subTest(key=bad):
                with self.assertRaises(cfgmod.UnknownSetting):
                    cfgmod.update({bad: "x"})

    def test_a_non_object_body_is_refused(self) -> None:
        with self.assertRaises(cfgmod.InvalidValue):
            cfgmod.update(["extraction.model", "deepseek-chat"])  # type: ignore[arg-type]

    def test_values_are_validated_at_the_boundary(self) -> None:
        with self.assertRaises(cfgmod.InvalidValue):
            cfgmod.update({"extraction.max_tokens": "lots"})
        with self.assertRaises(cfgmod.InvalidValue):
            cfgmod.update({"skills.shared_skill_budget_ratio": "half"})
        with self.assertRaises(cfgmod.InvalidValue):
            cfgmod.update({"extraction.provider": "telepathy"})

    def test_a_rejected_write_changes_nothing(self) -> None:
        cfgmod.update({"extraction.model": "deepseek-chat"})
        with self.assertRaises(cfgmod.InvalidValue):
            cfgmod.update({"extraction.model": "other", "extraction.max_tokens": "lots"})
        self.assertEqual("deepseek-chat", os.environ["MATRIXARK_EXTRACTION_MODEL"])


class SecretHandlingTest(_Sandbox):
    def test_a_secret_never_comes_back_out_of_a_read(self) -> None:
        secret = "sk-this-value-must-never-appear"
        cfgmod.update({"extraction.api_key_env": "DEEPSEEK_API_KEY",
                       "extraction.api_key": secret})
        snapshot = cfgmod.snapshot()
        self.assertNotIn(secret, json.dumps(snapshot))
        field = [f for f in snapshot["groups"]["extraction"] if f["key"] == "extraction.api_key"][0]
        self.assertTrue(field["configured"])
        self.assertIsNone(field["value"])

    def test_the_key_lands_in_the_variable_named_in_the_same_write(self) -> None:
        # A single call that sets both the key and the variable holding it must not write the key to
        # the OLD variable -- that is a key silently configured where nothing reads it.
        cfgmod.update({"extraction.api_key_env": "DEEPSEEK_API_KEY", "extraction.api_key": "sk-live"})
        self.assertEqual("sk-live", os.environ["DEEPSEEK_API_KEY"])
        self.assertNotIn("OPENAI_API_KEY", os.environ)

    def test_the_stored_file_is_owner_only(self) -> None:
        cfgmod.update({"extraction.api_key": "sk-live"})
        mode = stat.S_IMODE(os.stat(cfgmod.config_path()).st_mode)
        if os.name != "nt":  # Windows does not carry POSIX mode bits
            self.assertEqual(0o600, mode)

    def test_an_empty_write_clears_the_variable(self) -> None:
        cfgmod.update({"extraction.api_key": "sk-live"})
        self.assertEqual("sk-live", os.environ["OPENAI_API_KEY"])
        cfgmod.update({"extraction.api_key": ""})
        self.assertNotIn("OPENAI_API_KEY", os.environ)


class ApplySemanticsTest(_Sandbox):
    def test_live_settings_are_in_effect_and_restart_ones_say_so(self) -> None:
        result = cfgmod.update({
            "extraction.base_url": "https://api.deepseek.com/v1",   # module constant at import
            "embedding.api_base": "http://127.0.0.1:8400/v1",       # read per call
        })
        by_key = {entry["key"]: entry for entry in result["applied"]}
        self.assertFalse(by_key["extraction.base_url"]["in_effect"])
        self.assertTrue(by_key["embedding.api_base"]["in_effect"])
        self.assertEqual(["extraction.base_url"], result["restart_required"])

    def test_the_extraction_provider_alias_is_kept_in_step(self) -> None:
        # The provider modules read MATRIXARK_EXTRACTION_PROVIDER as the fallback name for
        # MATRIXARK_UNDERSTANDING_PROVIDER; writing only one of them half-applies the change.
        cfgmod.update({"extraction.provider": "openai_compatible"})
        self.assertEqual("openai_compatible", os.environ["MATRIXARK_UNDERSTANDING_PROVIDER"])
        self.assertEqual("openai_compatible", os.environ["MATRIXARK_EXTRACTION_PROVIDER"])

    def test_a_write_is_persisted_and_reloaded(self) -> None:
        cfgmod.update({"embedding.model": "paraphrase-multilingual-MiniLM-L12-v2"})
        document = cfgmod.load()
        self.assertEqual("paraphrase-multilingual-MiniLM-L12-v2",
                         document["values"]["embedding.model"])
        self.assertIsNotNone(document["updated_at"])

    def test_a_corrupt_file_is_treated_as_empty_not_fatal(self) -> None:
        with open(cfgmod.config_path(), "w", encoding="utf-8") as handle:
            handle.write("{not json")
        self.assertEqual({}, cfgmod.load()["values"])
        self.assertEqual([], cfgmod.apply_boot())


class HistoryTest(_Sandbox):
    def test_a_write_records_what_changed_and_from_what(self) -> None:
        cfgmod.update({"embedding.model": "minilm"}, actor="alice")
        cfgmod.update({"embedding.model": "bge-m3"}, actor="bob")
        entries = cfgmod.history()
        self.assertEqual(2, len(entries))
        newest = entries[0]
        self.assertEqual("bob", newest["by"])
        change = newest["changes"][0]
        self.assertEqual("embedding.model", change["key"])
        self.assertEqual("minilm", change["from"])
        self.assertEqual("bge-m3", change["to"])

    def test_the_previous_value_can_come_from_the_launcher_environment(self) -> None:
        # "What did it change from" has to answer even when the previous value was never written
        # through the portal -- otherwise the first change after a deployment reads as from-nothing.
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = "exported-model"
        cfgmod._BOOT_ENV["MATRIXARK_EMBEDDING_MODEL"] = "exported-model"
        cfgmod.update({"embedding.model": "minilm"})
        self.assertEqual("exported-model", cfgmod.history()[0]["changes"][0]["from"])

    def test_a_secret_is_recorded_by_key_and_never_by_value(self) -> None:
        secret = "sk-history-must-not-hold-this"
        cfgmod.update({"extraction.api_key": secret})
        entries = cfgmod.history()
        self.assertNotIn(secret, json.dumps(entries))
        change = entries[0]["changes"][0]
        self.assertEqual("extraction.api_key", change["key"])
        self.assertTrue(change["secret"])
        self.assertNotIn("to", change)
        # The FACT of a rotation is the useful part and is kept.
        self.assertEqual("set", change["action"])

    def test_a_reset_is_recorded_as_a_reset(self) -> None:
        cfgmod.update({"retrieval.min_score": "0.4"})
        cfgmod.update({"retrieval.min_score": None})
        self.assertEqual("reset", cfgmod.history()[0]["changes"][0]["action"])

    def test_the_log_is_bounded(self) -> None:
        # This file is read on every boot and every admin read; an unbounded log grows until
        # somebody notices, which is always later than they would like.
        for index in range(cfgmod.HISTORY_LIMIT + 15):
            cfgmod.update({"retrieval.min_score": "0.%d" % (index % 10 + 1)})
        stored = cfgmod.load()["history"]
        self.assertEqual(cfgmod.HISTORY_LIMIT, len(stored))

    def test_the_log_says_what_needed_a_restart(self) -> None:
        cfgmod.update({"extraction.base_url": "https://api.deepseek.com/v1",
                       "embedding.api_base": "http://127.0.0.1:8400/v1"})
        self.assertEqual(["extraction.base_url"], cfgmod.history()[0]["restart_required"])

    def test_the_snapshot_carries_the_log(self) -> None:
        cfgmod.update({"embedding.model": "minilm"}, actor="alice")
        self.assertEqual("alice", cfgmod.snapshot()["history"][0]["by"])


class BootPrecedenceTest(_Sandbox):
    def test_the_launcher_environment_still_wins_over_stored_config(self) -> None:
        cfgmod.update({"embedding.model": "stored-model"})
        # Simulate a restart: the operator exported the variable in the launcher.
        del os.environ["MATRIXARK_EMBEDDING_MODEL"]
        os.environ["MATRIXARK_EMBEDDING_MODEL"] = "launcher-model"
        cfgmod._BOOT_ENV["MATRIXARK_EMBEDDING_MODEL"] = "launcher-model"
        seeded = cfgmod.apply_boot()
        self.assertNotIn("MATRIXARK_EMBEDDING_MODEL", seeded)
        self.assertEqual("launcher-model", os.environ["MATRIXARK_EMBEDDING_MODEL"])

    def test_stored_config_seeds_a_variable_the_launcher_did_not_set(self) -> None:
        cfgmod.update({"embedding.model": "stored-model"})
        del os.environ["MATRIXARK_EMBEDDING_MODEL"]
        seeded = cfgmod.apply_boot()
        self.assertIn("MATRIXARK_EMBEDDING_MODEL", seeded)
        self.assertEqual("stored-model", os.environ["MATRIXARK_EMBEDDING_MODEL"])

    def test_the_snapshot_says_where_the_effective_value_came_from(self) -> None:
        cfgmod.update({"embedding.model": "stored-model"})
        fields = {f["key"]: f for f in cfgmod.snapshot()["groups"]["embedding"]}
        self.assertEqual("portal", fields["embedding.model"]["source"])
        os.environ["MATRIXARK_EMBEDDING_API_BASE"] = "http://exported/v1"
        cfgmod._BOOT_ENV["MATRIXARK_EMBEDDING_API_BASE"] = "http://exported/v1"
        fields = {f["key"]: f for f in cfgmod.snapshot()["groups"]["embedding"]}
        self.assertEqual("environment", fields["embedding.api_base"]["source"])
        self.assertEqual("default", fields["embedding.text_max_tokens"]["source"])


class PresetTest(_Sandbox):
    def test_the_deepseek_preset_configures_extraction_only(self) -> None:
        result = cfgmod.apply_preset("deepseek")
        self.assertEqual("https://api.deepseek.com/v1", os.environ["MATRIXARK_EXTRACTION_BASE_URL"])
        self.assertEqual("deepseek-chat", os.environ["MATRIXARK_EXTRACTION_MODEL"])
        self.assertEqual("DEEPSEEK_API_KEY", os.environ["MATRIXARK_EXTRACTION_API_KEY_ENV"])
        # DeepSeek has no embeddings API; the preset must not claim to configure one.
        self.assertNotIn("MATRIXARK_EMBEDDING_API_BASE", os.environ)
        self.assertIn("no embeddings API", result["note"])

    def test_no_preset_carries_a_secret(self) -> None:
        for name, preset in cfgmod.PRESETS.items():
            with self.subTest(preset=name):
                for key in preset["values"]:
                    self.assertFalse(cfgmod.SETTINGS_BY_KEY[key].secret)

    def test_an_unknown_preset_is_refused(self) -> None:
        with self.assertRaises(cfgmod.UnknownSetting):
            cfgmod.apply_preset("nope")


class ProbeTest(_Sandbox):
    def test_a_deterministic_provider_is_reported_not_dialled(self) -> None:
        # No network: the deterministic path calls nothing, so probing it must say so rather than
        # attempt a request against an unset endpoint.
        result = cfgmod.probe()
        targets = {r["target"]: r for r in result["results"]}
        self.assertTrue(targets["extraction"]["skipped"])
        self.assertTrue(targets["embedding"]["skipped"])
        self.assertFalse(result["all_ok"])

    def test_an_incomplete_configuration_is_reported_before_dialling(self) -> None:
        cfgmod.update({"extraction.provider": "openai_compatible"})
        targets = {r["target"]: r for r in cfgmod.probe(["extraction"])["results"]}
        self.assertEqual("incomplete_config", targets["extraction"]["error"])

    def test_an_unreachable_endpoint_is_an_error_not_an_exception(self) -> None:
        cfgmod.update({
            "extraction.provider": "openai_compatible",
            # Port 1 on the loopback refuses instantly; no live dependency, no timeout wait.
            "extraction.base_url": "http://127.0.0.1:1/v1",
            "extraction.model": "deepseek-chat",
        })
        result = cfgmod.probe(["extraction"], timeout=2.0)
        entry = result["results"][0]
        self.assertFalse(entry["ok"])
        self.assertIn("error", entry)
        self.assertIn("latency_ms", entry)


if __name__ == "__main__":
    unittest.main()

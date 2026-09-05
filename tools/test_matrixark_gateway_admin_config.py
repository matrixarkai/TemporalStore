# SPDX-License-Identifier: Apache-2.0
"""Gateway admin-config snapshot and the single-writer startup guard.

Two operator-facing safety surfaces:

* ``_model_config_snapshot`` powers ``GET /v1/admin/config``. It must never carry a key VALUE -- a
  key is configured by naming the env var that holds it, so the snapshot reports the name and a
  boolean. It must also warn about the deterministic fallbacks, which answer 200 and are otherwise
  indistinguishable from a healthy deployment.
* ``_single_writer_warning`` catches the multi-worker split: with the spawning backend each uvicorn
  worker owns its own embedded store, so >1 worker scatters a tenant's memory unless the workers are
  pointed at a shared store.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gateway  # noqa: E402


class ModelConfigSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = dict(os.environ)
        for name in list(os.environ):
            if name.startswith("MATRIXARK_"):
                del os.environ[name]

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._saved)

    def test_defaults_warn_about_both_deterministic_fallbacks(self) -> None:
        snapshot = gateway._model_config_snapshot()
        self.assertEqual(snapshot["extraction"]["provider"], "deterministic")
        self.assertEqual(snapshot["embedding"]["provider"], "deterministic")
        joined = " ".join(snapshot["warnings"])
        # Named as the customer sees them on the page: these are the labels of the two controls,
        # not the provider fields underneath.
        self.assertIn("Extraction provider is deterministic", joined)
        self.assertIn("Embedding provider is deterministic", joined)

    def test_key_value_is_never_included_only_its_env_var_name(self) -> None:
        secret = "sk-this-value-must-never-appear"
        os.environ["MATRIXARK_EXTRACTION_API_KEY_ENV"] = "DEEPSEEK_API_KEY"
        os.environ["DEEPSEEK_API_KEY"] = secret
        snapshot = gateway._model_config_snapshot()
        self.assertNotIn(secret, repr(snapshot))
        self.assertEqual(snapshot["extraction"]["api_key_env"], "DEEPSEEK_API_KEY")
        self.assertTrue(snapshot["extraction"]["api_key_configured"])

    def test_a_fully_configured_deployment_raises_no_warnings(self) -> None:
        os.environ.update(
            {
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
            }
        )
        self.assertEqual(gateway._model_config_snapshot()["warnings"], [])

    def test_an_embedding_base_url_without_v1_is_called_out(self) -> None:
        # <base>/embeddings never reaches an encoder serving /v1/embeddings, and the request still
        # succeeds with hash vectors -- invisible without this warning.
        os.environ.update(
            {
                "MATRIXARK_EMBEDDING_PROVIDER": "openai_compatible",
                "MATRIXARK_EMBEDDING_API_BASE": "http://127.0.0.1:8400",
                "MATRIXARK_EMBEDDING_API_KEY_ENV": "LOCAL_ENCODER_KEY",
                "LOCAL_ENCODER_KEY": "placeholder",
                "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS": "1",
            }
        )
        joined = " ".join(gateway._model_config_snapshot()["warnings"])
        self.assertIn("does not end in /v1", joined)

    def test_an_empty_embedding_key_is_called_out_even_for_a_local_encoder(self) -> None:
        os.environ.update(
            {
                "MATRIXARK_EMBEDDING_PROVIDER": "openai_compatible",
                "MATRIXARK_EMBEDDING_API_BASE": "http://127.0.0.1:8400/v1",
                "MATRIXARK_EMBEDDING_API_KEY_ENV": "LOCAL_ENCODER_KEY",
                "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS": "1",
            }
        )
        joined = " ".join(gateway._model_config_snapshot()["warnings"])
        self.assertIn("is empty", joined)

    def test_a_named_provider_with_an_empty_key_is_called_out(self) -> None:
        os.environ["MATRIXARK_EXTRACTION_PROVIDER"] = "openai_compatible"
        os.environ["MATRIXARK_EXTRACTION_API_KEY_ENV"] = "DEEPSEEK_API_KEY"
        os.environ.pop("DEEPSEEK_API_KEY", None)
        joined = " ".join(gateway._model_config_snapshot()["warnings"])
        # The control, because two of them write this variable and the variable alone does not say
        # which key is missing; and the variable too, because that is what an API reader sees.
        self.assertIn("Extraction API key is empty", joined)
        self.assertIn("DEEPSEEK_API_KEY", joined)

    def test_missing_require_model_embeddings_is_called_out(self) -> None:
        os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "openai_compatible"
        joined = " ".join(gateway._model_config_snapshot()["warnings"])
        self.assertIn("Fail instead of falling back is off", joined)


class SingleWriterGuardTest(unittest.TestCase):
    def test_one_worker_or_no_flag_is_silent(self) -> None:
        self.assertIsNone(gateway._single_writer_warning(["uvicorn", "--workers", "1"], {}))
        self.assertIsNone(gateway._single_writer_warning(["uvicorn"], {}))

    def test_multiple_workers_without_a_shared_store_warn(self) -> None:
        for argv, env in (
            (["uvicorn", "--workers", "4"], {}),
            (["uvicorn", "--workers=4"], {}),
            (["uvicorn"], {"WEB_CONCURRENCY": "4"}),
            (["uvicorn", "--workers", "4"], {"TS_META_ADDR": "standalone"}),
        ):
            with self.subTest(argv=argv, env=env):
                warning = gateway._single_writer_warning(argv, env)
                self.assertIsNotNone(warning)
                self.assertIn("INVISIBLE", warning)

    def test_a_shared_store_makes_multiple_workers_safe(self) -> None:
        for env in (
            {"TS_META_ADDR": "127.0.0.1:17801"},
            {"TS_STORAGE_BACKEND": "shared"},
            {"TS_SHARED_STORE_DIR": "/srv/shared"},
        ):
            with self.subTest(env=env):
                self.assertIsNone(gateway._single_writer_warning(["uvicorn", "--workers", "4"], env))

    def test_strict_mode_raises_instead_of_warning(self) -> None:
        with self.assertRaises(RuntimeError):
            gateway._enforce_single_writer(
                ["uvicorn", "--workers", "4"], {"MATRIXARK_STRICT_SINGLE_WRITER": "1"}
            )
        # Non-strict stays advisory so an existing deployment is not broken by an upgrade.
        gateway._enforce_single_writer(["uvicorn", "--workers", "4"], {})


if __name__ == "__main__":
    unittest.main()

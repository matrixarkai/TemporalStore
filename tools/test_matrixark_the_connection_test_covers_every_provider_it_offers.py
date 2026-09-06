#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal's connection test covers the providers the portal offers.

Two providers were reachable from the dropdown and not from the test.

``anthropic`` was probed as though it were OpenAI-compatible: ``{extraction base URL}/chat/completions``
with a bearer token. Anthropic reads its own base URL and model and authenticates with ``x-api-key``,
so the probe never reached it -- and, because the OpenAI base URL is empty on such a deployment,
answered "Set both the extraction base URL and model first", naming two fields that provider never
reads.

``local`` was worse, because it reported success. It is not a local server; it is the token-hash
fallback, and the encoder makes no HTTP call on it at all. The probe posted to the configured base
URL anyway, so a deployment running entirely on hash vectors could get a green connection test.

No test here reaches the network: the transport is replaced and every assertion is about the request
that would have been sent.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

ENCODER = "matrixark_mcp_embeddings.py"
CORE = "matrixark_mcp_core.py"

EXTRACTION_VARIABLES = ("MATRIXARK_UNDERSTANDING_PROVIDER", "MATRIXARK_EXTRACTION_PROVIDER",
                        "MATRIXARK_EXTRACTION_BASE_URL", "MATRIXARK_EXTRACTION_MODEL",
                        "MATRIXARK_ANTHROPIC_API_BASE", "MATRIXARK_ANTHROPIC_MODEL",
                        "MATRIXARK_ANTHROPIC_VERSION")
EMBEDDING_VARIABLES = ("MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_EMBEDDING_API_BASE",
                       "MATRIXARK_EMBED_BASE_URL", "MATRIXARK_EMBEDDING_MODEL")
KEY_VARIABLES = ("MATRIXARK_EXTRACTION_API_KEY_ENV", "MATRIXARK_EMBEDDING_API_KEY_ENV",
                 "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "VOYAGE_API_KEY")


def parse(filename: str) -> ast.Module:
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        return ast.parse(handle.read(), filename=filename)


def provider_membership_sets(filename: str) -> list:
    """Every ``provider in {...}`` set literal in a module, as the source writes them."""
    found = []
    for node in ast.walk(parse(filename)):
        if not isinstance(node, ast.Compare) or len(node.ops) != 1:
            continue
        if not isinstance(node.ops[0], ast.In):
            continue
        left, right = node.left, node.comparators[0]
        if not (isinstance(left, ast.Name) and left.id == "provider"):
            continue
        if isinstance(right, (ast.Set, ast.List, ast.Tuple)):
            members = [e.value for e in right.elts
                       if isinstance(e, ast.Constant) and isinstance(e.value, str)]
            if members:
                found.append(set(members))
    return found


def encoder_api_providers() -> set:
    """The encoder's own set, read from the module rather than restated."""
    for node in parse(ENCODER).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_API_EMBEDDING_PROVIDERS":
                    return {e.value for e in node.value.elts
                            if isinstance(e, ast.Constant) and isinstance(e.value, str)}
    raise AssertionError("_API_EMBEDDING_PROVIDERS not found in " + ENCODER)


class Recorder:
    """Stands in for the HTTP transport. Records the request and answers as the provider would."""

    def __init__(self, reply=None):
        self.calls = []
        self.reply = reply if reply is not None else {"model": "m", "choices": [
            {"message": {"content": "pong"}}]}

    def __call__(self, url, payload, headers, timeout):
        self.calls.append({"url": url, "payload": payload, "headers": headers})
        return 200, self.reply


class ProbeCase(unittest.TestCase):
    """Runs probe() with a chosen environment and no network."""

    def setUp(self) -> None:
        self._saved = {name: os.environ.get(name) for name in
                       EXTRACTION_VARIABLES + EMBEDDING_VARIABLES + KEY_VARIABLES}
        for name in self._saved:
            os.environ.pop(name, None)
        self._post_json = cfg._post_json
        self._load = cfg.load
        # Never read or write the deployment's real settings file from a test.
        cfg.load = lambda: {"values": {}}

    def tearDown(self) -> None:
        cfg._post_json = self._post_json
        cfg.load = self._load
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def key_variable(self, group: str) -> str:
        """Where a key for this group lands, asked of the registry with the provider already set.
        The probe's contract is that it reads the resolved variable, whichever that is; pinning a
        literal name here would be testing the routing, which has its own suite."""
        return cfg._env_name(cfg.SETTINGS_BY_KEY[group + ".api_key"], {})

    def probe(self, targets, reply=None, key=None, **environment) -> tuple:
        for name, value in environment.items():
            os.environ[name] = value
        if key is not None:
            os.environ[self.key_variable(targets[0])] = key
        recorder = Recorder(reply)
        cfg._post_json = recorder
        return cfg.probe(targets, 5.0), recorder


class TheSetsStillMatchTheProviderCodeTest(unittest.TestCase):
    """The probe decides whether a call happens at all from these. If they drift from the dispatch
    they mirror, the test is back to reporting on a path nobody runs."""

    def test_the_encoder_set_is_the_encoders(self) -> None:
        self.assertEqual(encoder_api_providers(), cfg._API_EMBEDDING_PROVIDERS)

    def test_local_is_not_one_of_them(self) -> None:
        """The whole point: 'local' names no endpoint, so the test must not pretend to reach one."""
        self.assertNotIn("local", encoder_api_providers())
        self.assertIn("local", cfg.SETTINGS_BY_KEY["embedding.provider"].choices)

    def test_both_extraction_sets_are_dispatch_sets_in_the_provider_code(self) -> None:
        sets = provider_membership_sets(CORE)
        self.assertIn(cfg._OPENAI_EXTRACTION_PROVIDERS, sets)
        self.assertIn(cfg._ANTHROPIC_EXTRACTION_PROVIDERS, sets)

    def test_every_offered_choice_is_classified(self) -> None:
        """A choice in neither the call sets nor the skip path would fall through to whichever
        branch happens to be last."""
        for choice in cfg.SETTINGS_BY_KEY["extraction.provider"].choices:
            with self.subTest(provider=choice):
                self.assertTrue(
                    choice in cfg._OPENAI_EXTRACTION_PROVIDERS
                    or choice in cfg._ANTHROPIC_EXTRACTION_PROVIDERS
                    or choice in {"deterministic", "rules", "local", ""})


class TheAnthropicProviderIsActuallyTestedTest(ProbeCase):

    def test_it_calls_the_messages_endpoint_not_chat_completions(self) -> None:
        result, recorder = self.probe(
            ["extraction"], reply={"model": "claude-sonnet-5",
                                   "content": [{"type": "text", "text": "pong"}]},
            MATRIXARK_UNDERSTANDING_PROVIDER="anthropic", key="k")
        self.assertEqual(1, len(recorder.calls))
        self.assertTrue(recorder.calls[0]["url"].endswith("/v1/messages"),
                        recorder.calls[0]["url"])
        self.assertTrue(result["all_ok"], result)

    def test_it_authenticates_the_way_anthropic_does(self) -> None:
        _, recorder = self.probe(["extraction"],
                                 reply={"content": [{"type": "text", "text": "pong"}]},
                                 MATRIXARK_UNDERSTANDING_PROVIDER="anthropic", key="k")
        headers = recorder.calls[0]["headers"]
        self.assertEqual("k", headers.get("x-api-key"))
        self.assertIn("anthropic-version", headers)
        self.assertNotIn("Authorization", headers)

    def test_it_uses_the_anthropic_base_url_and_model(self) -> None:
        _, recorder = self.probe(["extraction"],
                                 reply={"content": [{"type": "text", "text": "pong"}]},
                                 MATRIXARK_UNDERSTANDING_PROVIDER="anthropic",
                                 MATRIXARK_ANTHROPIC_API_BASE="https://anthropic.example",
                                 MATRIXARK_ANTHROPIC_MODEL="claude-test", key="k")
        self.assertEqual("https://anthropic.example/v1/messages", recorder.calls[0]["url"])
        self.assertEqual("claude-test", recorder.calls[0]["payload"]["model"])

    def test_it_no_longer_asks_for_fields_this_provider_never_reads(self) -> None:
        """The shipped behaviour: no OpenAI base URL, so it said 'Set both the extraction base URL
        and model first' -- advice that would not have changed anything."""
        result, _ = self.probe(["extraction"],
                               reply={"content": [{"type": "text", "text": "pong"}]},
                               MATRIXARK_UNDERSTANDING_PROVIDER="anthropic", key="k")
        self.assertNotEqual("incomplete_config", result["results"][0].get("error"))

    def test_a_missing_key_is_named_rather_than_posted_without(self) -> None:
        result, recorder = self.probe(["extraction"],
                                      MATRIXARK_UNDERSTANDING_PROVIDER="anthropic")
        self.assertEqual([], recorder.calls)
        entry = result["results"][0]
        self.assertEqual("no_api_key", entry["error"])
        self.assertIn(self.key_variable("extraction"), entry["detail"])

    def test_the_reply_is_summarised_from_content_blocks(self) -> None:
        result, _ = self.probe(["extraction"],
                               reply={"model": "claude-sonnet-5",
                                      "content": [{"type": "text", "text": "pong"}]},
                               MATRIXARK_UNDERSTANDING_PROVIDER="anthropic", key="k")
        self.assertEqual({"model": "claude-sonnet-5", "sample": "pong",
                          # The summary carries what was ASKED beside what answered, so a call
                          # that succeeded against a different model is not read as confirmation
                          # that the endpoint serves the configured one.
                          "requested_model": "claude-sonnet-5",
                          "model_matches_request": True},
                         result["results"][0]["response"])

    def test_a_reply_from_another_model_is_not_read_as_agreement(self) -> None:
        """The Anthropic branch specifically: three branches build a summary, and one left
        without the comparison would be a whole provider family silently unchecked."""
        result, _ = self.probe(["extraction"],
                               reply={"model": "claude-haiku-4-5",
                                      "content": [{"type": "text", "text": "pong"}]},
                               MATRIXARK_UNDERSTANDING_PROVIDER="anthropic", key="k")
        response = result["results"][0]["response"]
        self.assertEqual("claude-haiku-4-5", response["model"])
        self.assertEqual("claude-sonnet-5", response["requested_model"])
        self.assertIs(False, response["model_matches_request"])


class TheTestDoesNotPassOnAConfigurationThatMakesNoCallTest(ProbeCase):

    def test_local_does_not_probe_an_endpoint_the_encoder_never_calls(self) -> None:
        result, recorder = self.probe(["embedding"],
                                      MATRIXARK_EMBEDDING_PROVIDER="local",
                                      MATRIXARK_EMBEDDING_API_BASE="http://127.0.0.1:8400/v1")
        self.assertEqual([], recorder.calls, "the probe called an endpoint the encoder ignores")
        self.assertTrue(result["results"][0]["skipped"])
        self.assertFalse(result["all_ok"])

    def test_the_reason_says_what_is_actually_happening(self) -> None:
        result, _ = self.probe(["embedding"], MATRIXARK_EMBEDDING_PROVIDER="local",
                               MATRIXARK_EMBEDDING_API_BASE="http://127.0.0.1:8400/v1")
        self.assertIn("hash vectors", result["results"][0]["detail"])

    def test_a_real_api_provider_is_still_probed(self) -> None:
        """The floor: if nothing were probed any more, every assertion above would pass."""
        result, recorder = self.probe(
            ["embedding"], reply={"model": "voyage-3", "data": [{"embedding": [0.1, 0.2]}]},
            MATRIXARK_EMBEDDING_PROVIDER="voyage",
            MATRIXARK_EMBEDDING_API_BASE="https://api.voyageai.example/v1", key="k")
        self.assertEqual(1, len(recorder.calls))
        self.assertTrue(recorder.calls[0]["url"].endswith("/embeddings"))
        self.assertTrue(result["all_ok"], result)

    def test_an_openai_compatible_extraction_is_still_probed(self) -> None:
        result, recorder = self.probe(["extraction"],
                                      MATRIXARK_UNDERSTANDING_PROVIDER="openai_compatible",
                                      MATRIXARK_EXTRACTION_BASE_URL="https://api.example/v1",
                                      MATRIXARK_EXTRACTION_MODEL="gpt-4o-mini", key="k")
        self.assertEqual(1, len(recorder.calls))
        self.assertTrue(recorder.calls[0]["url"].endswith("/chat/completions"))
        self.assertEqual("Bearer k", recorder.calls[0]["headers"]["Authorization"])
        self.assertTrue(result["all_ok"], result)


class NoOfferedProviderIsLeftUnansweredTest(ProbeCase):
    """Whatever a customer picks, pressing the button has to say something true: either a result
    from a real call, or a reason it made none. Never a green tick without a call."""

    def test_every_embedding_choice_either_calls_or_explains(self) -> None:
        for choice in cfg.SETTINGS_BY_KEY["embedding.provider"].choices:
            with self.subTest(provider=choice):
                result, recorder = self.probe(
                    ["embedding"], reply={"model": "m", "data": [{"embedding": [0.1]}]},
                    MATRIXARK_EMBEDDING_PROVIDER=choice,
                    MATRIXARK_EMBEDDING_API_BASE="https://encoder.example/v1")
                entry = result["results"][0]
                if entry.get("skipped"):
                    self.assertEqual([], recorder.calls)
                    self.assertTrue(entry.get("detail"))
                else:
                    self.assertEqual(1, len(recorder.calls))

    def test_every_extraction_choice_either_calls_or_explains(self) -> None:
        for choice in cfg.SETTINGS_BY_KEY["extraction.provider"].choices:
            with self.subTest(provider=choice):
                result, recorder = self.probe(
                    ["extraction"], reply={"model": "m", "content": [{"type": "text", "text": "p"}],
                                           "choices": [{"message": {"content": "p"}}]},
                    MATRIXARK_UNDERSTANDING_PROVIDER=choice,
                    MATRIXARK_EXTRACTION_BASE_URL="https://api.example/v1",
                    MATRIXARK_EXTRACTION_MODEL="m", key="k")
                entry = result["results"][0]
                if entry.get("skipped"):
                    self.assertEqual([], recorder.calls)
                    self.assertTrue(entry.get("detail"))
                else:
                    self.assertEqual(1, len(recorder.calls))
                    self.assertTrue(entry["ok"])


if __name__ == "__main__":
    unittest.main()

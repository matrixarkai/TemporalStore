# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Conformance test: the published canonical ingest envelope schema and
``normalize_envelope`` describe the same contract.

For every VALID example payload we assert BOTH the JSON Schema
(``integrations/agent-hooks/shared/ingest_envelope_schema.json``) and
``tools/matrixark_mcp_core.normalize_envelope`` accept it. For every INVALID
payload we assert BOTH reject it (schema fails; ``normalize_envelope`` raises
``MatrixArkError``).

``normalize_envelope`` is the source of truth; the schema only DESCRIBES it.

Validator selection: if the ``jsonschema`` package is importable we use it
(draft 2020-12); otherwise we fall back to a minimal structural validator that
covers the constraints under test (kind enum, role enum, content-presence per
kind, scope-is-object, ingestion_time_ms positive int). The active validator is
printed once at load time and recorded in ``ACTIVE_VALIDATOR``.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
for _p in (_REPO, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:  # package path
    # matrixark_mcp_core re-exports MatrixArkError from matrixark_mcp_core_identity
    # (via `from .matrixark_mcp_core_identity import *`), which is the exact class
    # normalize_envelope raises. Import both from the same module so assertRaises
    # matches the raised type.
    from tools.matrixark_mcp_core import (
        normalize_envelope,
        normalize_message_role,
        MatrixArkError,
    )
except ImportError:
    from matrixark_mcp_core import (
        normalize_envelope,
        normalize_message_role,
        MatrixArkError,
    )

SCHEMA_PATH = os.path.join(
    _REPO, "integrations", "agent-hooks", "shared", "ingest_envelope_schema.json"
)
with open(SCHEMA_PATH, "r", encoding="utf-8") as _fh:
    SCHEMA = json.load(_fh)


# --------------------------------------------------------------------------- #
# Validator selection
# --------------------------------------------------------------------------- #
class SchemaError(Exception):
    """Raised by the schema validator when a payload does not conform."""


try:
    import jsonschema  # type: ignore

    _VALIDATOR = jsonschema.Draft202012Validator(SCHEMA)

    def validate_schema(payload: dict) -> None:
        errors = sorted(_VALIDATOR.iter_errors(payload), key=lambda e: e.path)
        if errors:
            raise SchemaError("; ".join(e.message for e in errors))

    ACTIVE_VALIDATOR = "jsonschema (Draft202012Validator)"
except (ImportError, AttributeError):
    # AttributeError as well as ImportError: a jsonschema old enough to predate
    # Draft202012Validator imports perfectly well and then fails on the attribute, which skipped
    # this fallback entirely and took the whole module down at import time. The structural
    # validator below is exactly what that case wants.
    # Minimal structural validator driven by the published schema's enums.
    _KIND_ENUM = set(SCHEMA["properties"]["kind"]["enum"])
    _ROLE_ENUM = set(SCHEMA["$defs"]["message"]["properties"]["role"]["enum"])
    _SHARING_ENUM = set(SCHEMA["$defs"]["sharing_scope"]["enum"])
    _MSG_REQUIRED_KINDS = {"message", "feedback", "business_data"}
    _CONTENT_SOURCE_KINDS = {"resource", "skill"}

    def _nonempty_messages(payload: dict) -> bool:
        msgs = payload.get("messages")
        return isinstance(msgs, list) and len(msgs) > 0

    def _validate_messages_shape(payload: dict) -> None:
        msgs = payload.get("messages")
        if msgs is None:
            return
        if not isinstance(msgs, list):
            raise SchemaError("messages must be an array")
        for item in msgs:
            if not isinstance(item, dict):
                raise SchemaError("message item must be an object")
            role = item.get("role")
            content = item.get("content")
            if "role" not in item or "content" not in item:
                raise SchemaError("message item requires role and content")
            if role not in _ROLE_ENUM:
                raise SchemaError(f"message role must be one of {sorted(_ROLE_ENUM)}")
            if not isinstance(content, str) or len(content) < 1:
                raise SchemaError("message content must be a non-empty string")

    def validate_schema(payload: dict) -> None:
        if not isinstance(payload, dict):
            raise SchemaError("envelope must be an object")

        # kind enum (kind is optional; enforced only when present)
        kind = payload.get("kind")
        if "kind" in payload and kind not in _KIND_ENUM:
            raise SchemaError(f"kind must be one of {sorted(_KIND_ENUM)}")

        # scope / metadata must be object or null when present
        for field in ("scope", "metadata"):
            if field in payload and payload[field] is not None and not isinstance(payload[field], dict):
                raise SchemaError(f"{field} must be an object")

        # sharing_scope enum (top-level and nested)
        if "sharing_scope" in payload and payload["sharing_scope"] not in _SHARING_ENUM:
            raise SchemaError(f"sharing_scope must be one of {sorted(_SHARING_ENUM)}")
        scope = payload.get("scope")
        if isinstance(scope, dict) and "sharing_scope" in scope:
            if scope["sharing_scope"] not in _SHARING_ENUM:
                raise SchemaError(f"scope.sharing_scope must be one of {sorted(_SHARING_ENUM)}")

        # ingestion_time_ms: positive integer when present (bool is not an int here)
        if "ingestion_time_ms" in payload:
            val = payload["ingestion_time_ms"]
            if isinstance(val, bool) or not isinstance(val, int):
                raise SchemaError("ingestion_time_ms must be an integer")
            if val <= 0:
                raise SchemaError("ingestion_time_ms must be > 0")

        # message shape (always, when present)
        _validate_messages_shape(payload)

        # kind-dependent content requirement (the conditional in allOf)
        if kind in _MSG_REQUIRED_KINDS:
            if not _nonempty_messages(payload):
                raise SchemaError(f"kind={kind} requires a non-empty messages list")
        elif kind in _CONTENT_SOURCE_KINDS:
            has_source = (
                _nonempty_messages(payload)
                or bool(payload.get("text"))
                or bool(payload.get("resource_text"))
                or bool(payload.get("raw_uri"))
            )
            if not has_source:
                raise SchemaError(
                    f"kind={kind} requires one of messages|text|resource_text|raw_uri"
                )

    ACTIVE_VALIDATOR = "builtin minimal structural validator (jsonschema unavailable)"


print(f"[test_ingest_envelope_schema] active schema validator: {ACTIVE_VALIDATOR}")


# --------------------------------------------------------------------------- #
# Example payloads
# --------------------------------------------------------------------------- #
VALID_PAYLOADS = {
    "message_basic": {
        "kind": "message",
        "messages": [{"role": "user", "content": "hello"}],
    },
    "message_multi_role_with_scope": {
        "kind": "message",
        "messages": [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": "a"},
            {"role": "tool", "content": "t"},
            {"role": "system", "content": "s"},
        ],
        "scope": {
            "account_id": "acct-1",
            "tenant_id": "ten-1",
            "team": "team-1",
            "user_id": "usr-1",
            "session_id": "sess-1",
            "sharing_scope": "private_user",
        },
    },
    "resource_with_text": {
        "kind": "resource",
        "text": "some resource body text",
        "resource_type": "note",
    },
    "skill_with_raw_uri": {
        "kind": "skill",
        "raw_uri": "/tmp/some_skill.md",
    },
    "feedback_basic": {
        "kind": "feedback",
        "messages": [{"role": "user", "content": "thumbs up"}],
    },
    "business_data_with_sharing": {
        "kind": "business_data",
        "messages": [{"role": "system", "content": "row=42"}],
        "scope": {"tenant_id": "ten-9", "sharing_scope": "tenant_shared"},
        "sharing_scope": "tenant_shared",
    },
    "message_with_ingestion_time_and_metadata": {
        "kind": "message",
        "messages": [{"role": "user", "content": "timed"}],
        "ingestion_time_ms": 1723800000000,
        "metadata": {"origin": "unit-test"},
    },
    "global_shared_message": {
        "kind": "message",
        "messages": [{"role": "assistant", "content": "public"}],
        "scope": {"sharing_scope": "global_shared"},
    },
    "message_with_alias_roles": {
        # normalize_message_role collapses aliases to canonical roles; the schema
        # role enum lists these aliases so both accept them.
        "kind": "message",
        "messages": [
            {"role": "human", "content": "hi"},
            {"role": "agent", "content": "hello"},
            {"role": "function", "content": "result"},
        ],
    },
}

INVALID_PAYLOADS = {
    "bad_role": {
        "kind": "message",
        "messages": [{"role": "robot", "content": "x"}],
    },
    "invalid_kind": {
        "kind": "telemetry",
        "messages": [{"role": "user", "content": "x"}],
    },
    "empty_messages": {
        "kind": "message",
        "messages": [],
    },
    "message_kind_no_messages": {
        "kind": "message",
    },
    "non_object_scope": {
        "kind": "message",
        "messages": [{"role": "user", "content": "x"}],
        "scope": "not-an-object",
    },
    "non_positive_ingestion_time": {
        "kind": "message",
        "messages": [{"role": "user", "content": "x"}],
        "ingestion_time_ms": 0,
    },
    "empty_message_content": {
        "kind": "message",
        "messages": [{"role": "user", "content": ""}],
    },
}


class IngestEnvelopeSchemaConformance(unittest.TestCase):
    def _accepts_code(self, payload: dict) -> None:
        # normalize_envelope must not raise; default_kind is irrelevant when
        # kind is supplied, but provide a valid one for completeness.
        normalize_envelope(dict(payload), default_kind="message")

    def test_valid_payloads_accepted_by_both(self) -> None:
        for name, payload in VALID_PAYLOADS.items():
            with self.subTest(payload=name):
                # schema accepts
                try:
                    validate_schema(payload)
                except SchemaError as exc:  # pragma: no cover - failure path
                    self.fail(f"{name}: schema rejected a valid payload: {exc}")
                # code accepts
                try:
                    self._accepts_code(payload)
                except MatrixArkError as exc:  # pragma: no cover - failure path
                    self.fail(f"{name}: normalize_envelope rejected a valid payload: {exc}")

    def test_invalid_payloads_rejected_by_both(self) -> None:
        for name, payload in INVALID_PAYLOADS.items():
            with self.subTest(payload=name):
                with self.assertRaises(SchemaError, msg=f"{name}: schema accepted an invalid payload"):
                    validate_schema(payload)
                with self.assertRaises(
                    MatrixArkError, msg=f"{name}: normalize_envelope accepted an invalid payload"
                ):
                    self._accepts_code(payload)

    def test_schema_enums_match_code_contract(self) -> None:
        # The kinds accepted by the schema are exactly those normalize_envelope allows.
        self.assertEqual(
            set(SCHEMA["properties"]["kind"]["enum"]),
            {"message", "feedback", "resource", "skill", "business_data"},
        )
        role_enum = set(SCHEMA["$defs"]["message"]["properties"]["role"]["enum"])
        canonical = {"user", "assistant", "tool", "system"}
        # The four canonical roles must be present.
        self.assertTrue(canonical.issubset(role_enum))
        # Every role the schema advertises must normalize (via the code) into a
        # canonical role -- proving the schema advertises only roles the code accepts.
        for role in role_enum:
            self.assertIn(
                normalize_message_role(role),
                canonical,
                msg=f"schema role {role!r} does not normalize to a canonical role",
            )
        self.assertEqual(
            set(SCHEMA["$defs"]["sharing_scope"]["enum"]),
            {"private_user", "tenant_shared", "global_shared"},
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

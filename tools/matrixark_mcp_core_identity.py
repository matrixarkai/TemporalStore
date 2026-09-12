# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Identity / role-scope / scope-key / request-validation helpers.

Split out of matrixark_mcp_core.py for readability. Re-exported via
`from matrixark_mcp_core_identity import *` at the END of matrixark_mcp_core.py,
so the ~113 modules importing matrixark_mcp_core see these names unchanged.
The few core deps below are all defined above that re-export line, so importing
them here does not create an import-time cycle.
"""
import hashlib
import json
import os
import re
import secrets
import socket
import time
from typing import Any

# From the modules that DEFINE these rather than from matrixark_mcp_core, which republishes
# them and star-imports this module back -- a cycle that made `import matrixark_mcp_core_identity`
# fail part-way. BACKEND_READINESS_CONNECT_TIMEOUT_MS is written out identically in core and in
# matrixark_mcp_runtime_config, so this reads the same value from the module that owns it.
Json = dict[str, Any]

try:  # package path
    from .matrixark_mcp_identity import MATRIXARK_ROLE_SCOPE_LIMITS
    from .matrixark_mcp_runtime_config import BACKEND_READINESS_CONNECT_TIMEOUT_MS
except ImportError:  # top-level path
    from matrixark_mcp_identity import MATRIXARK_ROLE_SCOPE_LIMITS
    from matrixark_mcp_runtime_config import BACKEND_READINESS_CONNECT_TIMEOUT_MS

# mem0-compat scope alias folding lives in the leaf validation module (single
# source of truth) and is re-exported here via core's `import *` so
# normalize_envelope and other scope-assembly paths can call it.
try:  # package path
    from .matrixark_mcp_validation import fold_mem0_scope_aliases
except ImportError:  # top-level path
    from matrixark_mcp_validation import fold_mem0_scope_aliases


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import normalize_matrixark_role
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import normalize_matrixark_role


# Not defined here: the implementation lives in matrixark_mcp_identity and this module carried an
# identical second copy of each -- same body, same docstring, and the free names each body reads
# are bound the same way in both modules, which is what makes re-exporting a no-op rather than a
# swap.
try:
    from tools.matrixark_mcp_identity import (
        json_text,
        role_allows_scopes,
        session_scope_mode,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import (
        json_text,
        role_allows_scopes,
        session_scope_mode,
    )


def stable_hash(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_api_key(prefix: str = "mk_test") -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def now_ms() -> int:
    return int(time.time() * 1000)


# One class, not two. This was written out here as well as in matrixark_mcp_errors, and two
# classes of the same name are not interchangeable: `except` compares by identity, so a handler
# bound to one silently misses an exception raised through the other. matrixark_mcp_errors owns
# it -- more modules import from there, and it is a leaf, so nothing can cycle through it.
try:  # the implementation lives in matrixark_mcp_errors; this module re-exports it
    from .matrixark_mcp_errors import MatrixArkError
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError


class MatrixArkInvalidRequestError(MatrixArkError):
    """The request itself is malformed -- a value outside its allowed vocabulary, say.

    A distinct type so the edge can answer 400. Not applied wholesale: `MatrixArkError` is raised
    for both bad input and internal failures throughout the adapter, and reclassifying all of it
    would turn real faults into 400s. Used where the classification is unambiguous.
    """


class MatrixArkNotFoundError(MatrixArkError):
    """The thing the request addressed does not exist.

    A distinct type so the edge can answer 404 instead of 500. The difference matters to a caller:
    a stale memory id is its own state to fix, while a 500 says the server failed and invites a
    retry or a page.

    Defined HERE, beside the  the adapters actually raise and catch. There is a
    second, unrelated  in matrixark_mcp_errors.py; subclassing that one instead
    would produce an exception that  on this path does not catch.
    """


try:  # the implementation lives in matrixark_mcp_errors; this module re-exports it
    from .matrixark_mcp_errors import is_retryable_temporalstore_error
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import is_retryable_temporalstore_error


def parse_host_port(address: str) -> tuple[str, int] | None:
    if not address or ":" not in address:
        return None
    host, port_text = address.rsplit(":", 1)
    try:
        return host or "127.0.0.1", int(port_text)
    except ValueError:
        return None


def metaserver_reachable(address: str, timeout_ms: int = BACKEND_READINESS_CONNECT_TIMEOUT_MS) -> Json:
    parsed = parse_host_port(address)
    if parsed is None:
        return {"ok": False, "address": address, "error": "invalid metaserver address"}
    host, port = parsed
    try:
        with socket.create_connection((host, port), timeout=max(0.05, timeout_ms / 1000.0)):
            return {"ok": True, "address": address}
    except OSError as exc:
        return {"ok": False, "address": address, "error": str(exc)}


def normalize_message_role(role: Any) -> str:
    role_name = str(role or "").strip().lower()
    role_aliases = {
        "human": "user",
        "prompt": "user",
        "assistant_response": "assistant",
        "agent": "assistant",
        "ai": "assistant",
        "bot": "assistant",
        "llm": "assistant",
        "model": "assistant",
        "tool_result": "tool",
        "tool-output": "tool",
        "tooloutput": "tool",
        "tool_output": "tool",
        "function": "tool",
        "function_call_output": "tool",
        "custom_tool_call_output": "tool",
        "tool_call_output": "tool",
    }
    return role_aliases.get(role_name, role_name)


def require_messages(data: Json) -> list[Json]:
    messages = data.get("messages")
    if not isinstance(messages, list) or not messages:
        raise MatrixArkError("messages must be a non-empty list")
    normalized_messages: list[Json] = []
    for message in messages:
        if not isinstance(message, dict):
            raise MatrixArkError("messages entries must be objects")
        original_role = str(message.get("role") or "").strip()
        role = normalize_message_role(original_role)
        content = message.get("content")
        if role not in {"user", "assistant", "tool", "system"}:
            raise MatrixArkError("message role must be user, assistant, tool, system, or a supported role alias")
        if not isinstance(content, str) or not content:
            raise MatrixArkError("message content must be a non-empty string")
        normalized = dict(message)
        normalized["role"] = role
        if original_role and original_role.lower() != role and "original_role" not in normalized:
            normalized["original_role"] = original_role
        normalized_messages.append(normalized)
    return normalized_messages


try:  # the implementation lives in matrixark_mcp_validation; this module re-exports it
    from .matrixark_mcp_validation import optional_object
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_validation import optional_object


# Not defined here: the implementation lives in matrixark_mcp_validation and this module carried an
# identical second copy of each. Every caller importing these names from here is
# unaffected -- it is the same code, and the free names each body reads are bound the
# same way in both modules, which is what makes re-exporting a no-op rather than a
# swap.
try:
    from tools.matrixark_mcp_validation import (
        optional_string,
        optional_string_list,
        require_string,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_validation import (
        optional_string,
        optional_string_list,
        require_string,
    )


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import safe_identifier
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import safe_identifier


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import local_account_user_id
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import local_account_user_id


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import local_agent_name
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import local_agent_name


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import canonical_account_id
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import canonical_account_id


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import canonical_tenant_id
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import canonical_tenant_id


# Not defined here: the implementation lives in matrixark_mcp_identity and this module carried an
# identical second copy of each. Every caller importing these names from here is
# unaffected -- it is the same code, and the free names each body reads are bound the
# same way in both modules, which is what makes re-exporting a no-op rather than a
# swap.
try:
    from tools.matrixark_mcp_identity import (
        identity_hashes,
        local_identity_defaults,
        scope_key_prefix_for_query,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import (
        identity_hashes,
        local_identity_defaults,
        scope_key_prefix_for_query,
    )


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import scope_key_from_hashes
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import scope_key_from_hashes


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import parse_scope_key
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import parse_scope_key


def scope_key_matches_query(record_scope_key: str, query_scope: Json, explicit_keys: set[str]) -> bool:
    record_parts = parse_scope_key(record_scope_key)
    tenant_hash = int(query_scope.get("tenant_hash") or 0)
    if tenant_hash and record_parts.get("t") != tenant_hash:
        return False
    user_hash = int(query_scope.get("user_hash") or 0)
    if "user_id" in explicit_keys or "user_hash" in explicit_keys or user_hash:
        if user_hash and record_parts.get("u") != user_hash:
            return False
    # agent_id isolation (mem0 dimension): enforced only when the QUERY carries an
    # agent, mirroring user/session. No agent in the query -> no agent filtering,
    # so a broader user-level recall still spans all of that user's agents.
    agent_hash = int(query_scope.get("agent_hash") or 0)
    if "agent_id" in explicit_keys or "agent_hash" in explicit_keys or agent_hash:
        if agent_hash and record_parts.get("a") != agent_hash:
            return False
    session_hash = int(query_scope.get("session_hash") or 0)
    if "session_id" in explicit_keys or "session_hash" in explicit_keys or session_hash:
        if session_scope_mode(query_scope) == "prefer":
            return True
        if session_hash and record_parts.get("s") != session_hash:
            return False
    return True


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import canonical_scope_key
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import canonical_scope_key


try:  # the implementation lives in matrixark_mcp_identity; this module re-exports it
    from .matrixark_mcp_identity import cache_scope_key
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import cache_scope_key


def serving_scope_ref(scope: Json) -> Json:
    key = canonical_scope_key(scope)
    return {"scope_key": key} if key else {}


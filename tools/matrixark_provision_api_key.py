#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Mint a per-tenant MatrixArk Cloud API key for the enforced-mode `/v1` gateway.

The key is returned in PLAINTEXT exactly once (on stdout); only its sha256 hash is persisted, in an
append-only JSONL keystore whose records reuse the backend credential-store shape
(`record_type="matrixark_api_key"`, the same fields `matrixark_access.create_api_key` writes). The
gateway loads this store via `MATRIXARK_API_KEYS_HASHED_FILE` and, in enforced mode
(`MATRIXARK_AUTH_ENFORCED=1`), resolves a presented Bearer key to `{tenant_id, account_id}` by
hash -- never trusting client-supplied scope text.

    python3 matrixark_provision_api_key.py --tenant-id tenantA --account-id acct_a \
        --store /opt/temporalstore/gw/keys/api_keys_hashed.jsonl

Revoke a key by re-appending a record with the same `api_key_hash` and `--status revoked` (the
gateway keeps the last active record per hash).
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import time

try:  # reuse the backend hashing + key-mint primitives when importable
    try:
        from tools.matrixark_mcp_core_identity import make_api_key, secret_hash  # type: ignore
    except ImportError:
        from matrixark_mcp_core_identity import make_api_key, secret_hash  # type: ignore
except Exception:  # pragma: no cover - self-contained fallback (identical algorithm)
    import hashlib

    def secret_hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def make_api_key(prefix: str = "mk_test") -> str:
        return f"{prefix}_{secrets.token_urlsafe(32)}"


_DEFAULT_SCOPES = [
    "context:ingest",
    "context:retrieve",
    "context:feedback",
    "context:replay",
]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _default_store() -> str:
    return (
        os.environ.get("MATRIXARK_API_KEYS_HASHED_FILE")
        or "/opt/temporalstore/gw/keys/api_keys_hashed.jsonl"
    )


def mint(args: argparse.Namespace) -> dict:
    api_key = make_api_key(args.prefix)
    api_key_hash = secret_hash(api_key)
    api_key_id = f"key_{api_key_hash[:24]}"
    record = {
        "record_type": "matrixark_api_key",
        "api_key_id": api_key_id,
        "api_key_hash": api_key_hash,
        "account_id": args.account_id,
        "tenant_id": args.tenant_id,
        "scopes": sorted(set(args.scopes or _DEFAULT_SCOPES)),
        "role": args.role,
        "display_name": args.display_name or f"{args.tenant_id} key",
        "allowed_user_ids": sorted(set(args.allowed_user_ids or [])),
        "status": args.status,
        "expires_at_ms": args.expires_at_ms,
        "created_at_ms": _now_ms(),
    }
    store = args.store or _default_store()
    os.makedirs(os.path.dirname(store), exist_ok=True)
    with open(store, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    return {
        "status": "created" if args.status == "active" else args.status,
        "api_key": api_key,  # plaintext -- shown ONCE, never persisted
        "api_key_id": api_key_id,
        "api_key_hash": api_key_hash,
        "tenant_id": args.tenant_id,
        "account_id": args.account_id,
        "scopes": record["scopes"],
        "store": store,
        "warning": "Store api_key now. MatrixArk only persists its hash.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Mint a per-tenant MatrixArk Cloud API key.")
    parser.add_argument("--tenant-id", dest="tenant_id", required=True)
    parser.add_argument("--account-id", dest="account_id", default="acct_local")
    parser.add_argument("--prefix", default="sk_live")
    parser.add_argument("--role", default="service")
    parser.add_argument("--display-name", dest="display_name", default="")
    parser.add_argument("--status", default="active", choices=["active", "revoked"])
    parser.add_argument("--expires-at-ms", dest="expires_at_ms", type=int, default=None)
    parser.add_argument("--scope", dest="scopes", action="append", help="repeatable MatrixArk scope")
    parser.add_argument(
        "--allowed-user-id", dest="allowed_user_ids", action="append",
        help="repeatable; restrict the key to these user_ids",
    )
    parser.add_argument("--store", default="", help="JSONL keystore path (hashed records only)")
    args = parser.parse_args(argv)
    result = mint(args)
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

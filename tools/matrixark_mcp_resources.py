#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource storage and URI helpers for MatrixArk MCP."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


import hashlib
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_resource_parser import content_hash, normalize_parse_warnings
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_resource_parser import content_hash, normalize_parse_warnings

try:
    from tools.matrixark_mcp_errors import MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError

try:
    from tools.matrixark_mcp_identity import safe_identifier, scope_key_from_hashes, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import safe_identifier, scope_key_from_hashes, stable_hash
Json = dict[str, Any]


MAX_RESOURCE_FACT_CHUNKS = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACT_CHUNKS", "8"))
MAX_RESOURCE_FACTS_PER_RESOURCE = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACTS_PER_RESOURCE", "8"))
MAX_RESOURCE_FACTS_PER_CHUNK = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACTS_PER_CHUNK", "2"))
ENABLE_GENERIC_RESOURCE_FACTS = env_bool("MATRIXARK_ENABLE_GENERIC_RESOURCE_FACTS", False)


def compact_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def preview_text(text: str, *, limit: int = 220) -> str:
    text = compact_ws(text)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."

RAW_BYTE_METADATA_FIELDS = {"raw_bytes", "file_bytes", "bytes", "binary", "payload_bytes", "data_url", "base64"}
SERVING_RESOURCE_METADATA_FIELDS = {
    "resource_type",
    "resource_version",
    "unit_kind",
    "relative_path",
    "heading",
    "heading_slug",
    "heading_path",
    "source_locator",
    "content_hash",
    "token_estimate",
    "row_start",
    "row_end",
    "record_start",
    "record_end",
    "page",
    "page_section",
    "slide_number",
    "sheet_name",
    "row_count",
    "supersedes_chunk_hash",
}
DEBUG_RESOURCE_METADATA_FIELDS = {
    "embedding_text",
    "parse_warnings",
    "parser_name",
    "parser_version",
    "parse_warning_count",
    "columns",
    "links",
    "tables",
    "front_matter",
}


def sanitize_resource_metadata(metadata: Json) -> Json:
    sanitized = {
        key: value
        for key, value in metadata.items()
        if key not in RAW_BYTE_METADATA_FIELDS
    }
    sanitized["parse_warnings"] = normalize_parse_warnings(sanitized)
    sanitized["raw_storage_policy"] = str(sanitized.get("raw_storage_policy") or "raw_uri_only")
    sanitized["raw_bytes_stored"] = False
    return sanitized


def serving_resource_metadata(metadata: Json) -> Json:
    sanitized = sanitize_resource_metadata(metadata)
    serving = {
        key: sanitized[key]
        for key in SERVING_RESOURCE_METADATA_FIELDS
        if key in sanitized and sanitized[key] not in (None, "", [], {})
    }
    # `raw_storage_policy` is not carried per chunk: it is a document fact, identical on every
    # chunk, and no reader takes it from a stored chunk. The dashboard reads the TOP-level
    # field on manifest rows, ingest reads the live storage_resolution, and resource IO reads
    # the ENVELOPE metadata while deciding where raw bytes go. 93.1 KB per 1 MB skill.
    #
    # `resource_version` stays, though it is the same shape: the retrieve path reads it from a
    # stored record to decide version_state, falling back to a top-level field sections do not
    # carry, so dropping it would make every chunk look current.
    # `raw_bytes_stored` is a per-document fact and a constant False on every chunk of a
    # document, 27 B a row -- 66.2 KB per 1 MB skill. It is not carried here because
    # nothing reads it from a chunk: every mention inside a metadata dict is an
    # ASSIGNMENT, and every read takes the top-level field with a False default, which
    # the manifest record supplies.
    parse_warnings = normalize_parse_warnings(sanitized)
    if parse_warnings:
        serving["parse_warning_count"] = len(parse_warnings)
        serving["has_parse_warnings"] = True
    return serving


def debug_resource_metadata(metadata: Json) -> Json:
    sanitized = sanitize_resource_metadata(metadata)
    debug = {
        key: sanitized[key]
        for key in sorted(DEBUG_RESOURCE_METADATA_FIELDS)
        if key in sanitized and sanitized[key] not in (None, "", [], {})
    }
    parse_warnings = normalize_parse_warnings(sanitized)
    if parse_warnings:
        debug["parse_warnings"] = parse_warnings
        debug["parse_warning_count"] = len(parse_warnings)
    embedding_text = str(sanitized.get("embedding_text") or "")
    if embedding_text:
        debug["embedding_text_hash"] = stable_hash(embedding_text)
        debug["embedding_text_preview"] = preview_text(embedding_text, limit=320)
    return debug


def source_locator_from_ref(source_ref: str, raw_uri: str) -> str:
    source_ref = str(source_ref or "")
    raw_uri = str(raw_uri or "")
    if not source_ref:
        return ""
    if raw_uri and source_ref == raw_uri:
        return ""
    if raw_uri and source_ref.startswith(raw_uri + "#"):
        return source_ref.partition("#")[2]
    if "#" in source_ref:
        return source_ref.partition("#")[2]
    return source_ref


def source_ref_from_locator(raw_uri: str, source_locator: str) -> str:
    raw_uri = str(raw_uri or "")
    source_locator = str(source_locator or "")
    if not source_locator:
        return raw_uri
    if source_locator.startswith(("file:", "s3://", "http://", "https://", "/")):
        return source_locator
    return f"{raw_uri}#{source_locator}" if raw_uri else source_locator


def registry_access_scope(scope: Json, *, sharing_scope: str = "private_user") -> Json:
    sharing_scope = str(sharing_scope or "private_user").strip().lower()
    access = {
        "account_id": str(scope.get("account_id") or ""),
        "tenant_id": str(scope.get("tenant_id") or ""),
        "team": str(scope.get("team") or ""),
        "user_id": str(scope.get("user_id") or ""),
        "agent_id": str(scope.get("agent_id") or ""),
        "session_id": str(scope.get("session_id") or ""),
        "account_hash": scope.get("account_hash", 0),
        "tenant_hash": scope.get("tenant_hash", 0),
        "user_hash": scope.get("user_hash", 0),
        "agent_hash": scope.get("agent_hash", 0),
        "session_hash": scope.get("session_hash", 0),
        "scope_key": scope.get("scope_key", ""),
        "sharing_scope": sharing_scope,
    }
    if sharing_scope == "tenant_shared":
        access["user_id"] = ""
        access["agent_id"] = ""
        access["session_id"] = ""
        access["user_hash"] = 0
        access["agent_hash"] = 0
        access["session_hash"] = 0
        tenant_hash = int(access.get("tenant_hash") or 0)
        access["scope_key"] = scope_key_from_hashes(tenant_hash) if tenant_hash else ""
    elif sharing_scope == "global_shared":
        access["tenant_id"] = ""
        access["team"] = ""
        access["user_id"] = ""
        access["agent_id"] = ""
        access["session_id"] = ""
        access["tenant_hash"] = 0
        access["user_hash"] = 0
        access["agent_hash"] = 0
        access["session_hash"] = 0
        access["scope_key"] = "global|shared|"
    return access


def deployment_scope_from_args(args: Json, envelope: Json) -> str:
    value = str(
        args.get("deployment_scope")
        or envelope.get("metadata", {}).get("deployment_scope")
        or os.environ.get("MATRIXARK_DEPLOYMENT_SCOPE")
        or "local"
    ).strip().lower()
    return value if value in {"local", "global", "cloud", "on_prem", "hybrid"} else "local"


def resource_storage_mode_from_args(args: Json, envelope: Json, deployment_scope: str) -> str:
    value = str(
        args.get("raw_storage_mode")
        or envelope.get("metadata", {}).get("raw_storage_mode")
        or os.environ.get("MATRIXARK_RESOURCE_STORAGE_MODE")
        or ("cloud" if deployment_scope == "cloud" else "local")
    ).strip().lower()
    if value in {"s3", "remote"}:
        value = "cloud"
    if value not in {"local", "cloud"}:
        raise MatrixArkError("raw_storage_mode must be local or cloud")
    return value


def is_s3_uri(value: str) -> bool:
    return value.startswith("s3://")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not is_s3_uri(uri):
        raise MatrixArkError(f"not an s3 uri: {uri}")
    rest = uri[len("s3://") :]
    bucket, sep, key = rest.partition("/")
    if not bucket or not sep or not key:
        raise MatrixArkError(f"invalid s3 uri: {uri}")
    return bucket, key


def _cloud_resource_bucket(args: Json, envelope: Json) -> str:
    bucket = str(
        args.get("s3_bucket")
        or envelope.get("metadata", {}).get("s3_bucket")
        or os.environ.get("MATRIXARK_RESOURCE_S3_BUCKET")
        or os.environ.get("MATRIXARK_S3_BUCKET")
        or ""
    ).strip()
    if not bucket:
        raise MatrixArkError("cloud raw resource storage requires s3_bucket or MATRIXARK_RESOURCE_S3_BUCKET")
    return bucket


def _cloud_resource_prefix(args: Json, envelope: Json) -> str:
    prefix = str(
        args.get("s3_prefix")
        or envelope.get("metadata", {}).get("s3_prefix")
        or os.environ.get("MATRIXARK_RESOURCE_S3_PREFIX")
        or "matrixark/raw"
    ).strip().strip("/")
    scope = envelope.get("scope", {}) if isinstance(envelope.get("scope", {}), dict) else {}
    parts = [
        prefix,
        safe_identifier(str(scope.get("account_id") or "acct"), default="acct"),
        safe_identifier(str(scope.get("tenant_id") or "tenant"), default="tenant"),
        safe_identifier(str(scope.get("user_id") or "user"), default="user"),
    ]
    session_id = str(scope.get("session_id") or "")
    if session_id:
        parts.append(safe_identifier(session_id, default="session"))
    return "/".join(part for part in parts if part)


def _s3_client() -> Any:
    try:
        import boto3  # type: ignore

        kwargs: Json = {}
        endpoint_url = os.environ.get("MATRIXARK_S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL_S3")
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        region_name = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if region_name:
            kwargs["region_name"] = region_name
        return boto3.client("s3", **kwargs)
    except Exception:
        return None


def _aws_cli_s3_cp(source: str, target: str) -> None:
    command = ["aws"]
    profile = os.environ.get("AWS_PROFILE")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if profile:
        command.extend(["--profile", profile])
    if region:
        command.extend(["--region", region])
    endpoint_url = os.environ.get("MATRIXARK_S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL_S3")
    if endpoint_url:
        command.extend(["--endpoint-url", endpoint_url])
    command.extend(["s3", "cp", source, target])
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise MatrixArkError(compact_ws(completed.stderr or completed.stdout or f"aws s3 cp failed: {source} -> {target}"))


def upload_file_to_s3(path: Path, *, bucket: str, key: str) -> str:
    client = _s3_client()
    if client is not None:
        try:
            client.upload_file(str(path), bucket, key)
            return f"s3://{bucket}/{key}"
        except Exception as exc:
            raise MatrixArkError(f"S3 upload failed for {path}: {exc}") from exc
    target = f"s3://{bucket}/{key}"
    _aws_cli_s3_cp(str(path), target)
    return target


def download_s3_to_file(uri: str, target: Path) -> Path:
    bucket, key = parse_s3_uri(uri)
    target.parent.mkdir(parents=True, exist_ok=True)
    client = _s3_client()
    if client is not None:
        try:
            client.download_file(bucket, key, str(target))
            return target
        except Exception as exc:
            raise MatrixArkError(f"S3 download failed for {uri}: {exc}") from exc
    _aws_cli_s3_cp(uri, str(target))
    return target


def _resource_object_key(prefix: str, raw_uri: str, source_path: Path | None, resource_type: str) -> str:
    suffix = Path(raw_uri).name if raw_uri and raw_uri != "inline-resource" else ""
    if source_path is not None:
        suffix = source_path.name
    suffix = safe_identifier(suffix or f"resource.{resource_type or 'txt'}", default="resource")
    digest = hashlib.sha256()
    digest.update(raw_uri.encode("utf-8", errors="ignore"))
    if source_path is not None and source_path.exists() and source_path.is_file():
        try:
            with source_path.open("rb") as fh:
                for block in iter(lambda: fh.read(1024 * 1024), b""):
                    digest.update(block)
        except OSError:
            pass
    return f"{prefix}/{digest.hexdigest()[:16]}-{suffix}"


def _archive_directory_for_upload(path: Path) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="matrixark-resource-dir-"))
    archive_base = temp_dir / safe_identifier(path.name or "resource-dir", default="resource-dir")
    archive_path = shutil.make_archive(str(archive_base), "gztar", root_dir=str(path))
    return Path(archive_path)


def resolve_raw_resource_for_ingest(args: Json, envelope: Json, raw_uri: str, resource_type: str, deployment_scope: str, resource_text: str) -> Json:
    """Resolve local/cloud raw storage and parser source for resource/skill ingest."""
    mode = resource_storage_mode_from_args(args, envelope, deployment_scope)
    raw_uri = raw_uri or "inline-resource"
    result: Json = {
        "storage_mode": mode,
        "original_raw_uri": raw_uri,
        "stored_raw_uri": raw_uri,
        "parse_uri": raw_uri,
        "parse_text": resource_text,
        "raw_storage_policy": "local_raw_uri_only" if mode == "local" else "s3_raw_uri_only",
        "raw_bytes_stored": False,
        "upload_status": "not_required",
        "temp_paths": [],
        "cloud_bucket": "",
        "cloud_key": "",
    }
    local_path = Path(raw_uri) if raw_uri != "inline-resource" and not is_s3_uri(raw_uri) else None
    if mode == "local":
        if local_path is not None and local_path.exists():
            result["parse_text"] = None
        return result

    bucket = _cloud_resource_bucket(args, envelope)
    prefix = _cloud_resource_prefix(args, envelope)
    result["cloud_bucket"] = bucket

    if is_s3_uri(raw_uri):
        stored_uri = raw_uri
    else:
        upload_path: Path
        if local_path is not None and local_path.exists():
            upload_path = _archive_directory_for_upload(local_path) if local_path.is_dir() else local_path
            if local_path.is_dir():
                result["temp_paths"].append(str(upload_path.parent))
                result["parse_uri"] = str(local_path)
                result["parse_text"] = None
        else:
            suffix = infer_resource_suffix(resource_type, raw_uri)
            temp_file = Path(tempfile.mkdtemp(prefix="matrixark-inline-resource-")) / f"inline.{suffix}"
            temp_file.write_text(resource_text, encoding="utf-8")
            result["temp_paths"].append(str(temp_file.parent))
            upload_path = temp_file
        key = _resource_object_key(prefix, raw_uri, upload_path, resource_type)
        stored_uri = upload_file_to_s3(upload_path, bucket=bucket, key=key)
        result["upload_status"] = "uploaded"
        result["cloud_key"] = key

    result["stored_raw_uri"] = stored_uri
    if result.get("parse_uri") == raw_uri or is_s3_uri(raw_uri):
        suffix = infer_resource_suffix(resource_type, stored_uri)
        temp_file = Path(tempfile.mkdtemp(prefix="matrixark-s3-resource-")) / f"downloaded.{suffix}"
        result["temp_paths"].append(str(temp_file.parent))
        download_s3_to_file(stored_uri, temp_file)
        result["parse_uri"] = str(temp_file)
        result["parse_text"] = None
    return result


def infer_resource_suffix(resource_type: str, raw_uri: str) -> str:
    suffix = (resource_type or "").lower().lstrip(".")
    if not suffix and raw_uri and raw_uri != "inline-resource":
        suffix = Path(raw_uri).suffix.lower().lstrip(".")
    return suffix or "txt"


def rewrite_chunk_uris(chunks: list[Any], *, parse_uri: str, stored_raw_uri: str) -> list[Any]:
    if not stored_raw_uri or stored_raw_uri == parse_uri:
        return chunks
    rewritten: list[Any] = []
    for chunk in chunks:
        metadata = dict(getattr(chunk, "metadata", {}) or {})
        old_source_ref = str(getattr(chunk, "source_ref", ""))
        fragment = old_source_ref.partition("#")[2]
        relative_path = str(metadata.get("relative_path") or "").strip()
        if relative_path and fragment:
            new_source_ref = f"{stored_raw_uri}#path={relative_path}&{fragment}"
        elif fragment:
            new_source_ref = f"{stored_raw_uri}#{fragment}"
        else:
            new_source_ref = stored_raw_uri
        metadata["raw_uri"] = stored_raw_uri
        metadata["citation"] = new_source_ref
        metadata["source_ref"] = new_source_ref
        metadata["raw_storage_policy"] = "s3_raw_uri_only" if is_s3_uri(stored_raw_uri) else metadata.get("raw_storage_policy", "raw_uri_only")
        metadata["raw_bytes_stored"] = False
        piece_hash = str(metadata.get("content_hash") or content_hash(str(getattr(chunk, "text", ""))))
        version = str(metadata.get("resource_version") or "")
        chunk_hash = stable_hash(f"resource_chunk:{new_source_ref}:{version}:{piece_hash}")
        rewritten.append(
            chunk.__class__(
                chunk_hash=chunk_hash,
                source_ref=new_source_ref,
                text=getattr(chunk, "text", ""),
                token_estimate=int(getattr(chunk, "token_estimate", 1)),
                metadata=metadata,
            )
        )
    return rewritten


def cleanup_temp_paths(paths: list[str]) -> None:
    for path_text in paths:
        try:
            path = Path(path_text)
            if path.exists() and path.is_dir() and path.name.startswith("matrixark-"):
                shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass


def aggregate_parse_warnings_from_chunks(chunks: list[Any]) -> list[str]:
    warnings: list[str] = []
    for chunk in chunks:
        metadata = getattr(chunk, "metadata", {}) or {}
        for warning in normalize_parse_warnings(metadata):
            if warning not in warnings:
                warnings.append(warning)
    return warnings


RESOURCE_FACT_KEYWORDS = re.compile(
    r"\b(decision|decided|owner|owns|deadline|due|cost|budget|approval|approved|control_state|policy|must|should|required|requires|api|endpoint|contract|runbook|rollback|incident|troubleshoot|alert|sla|p95|p99|procedure|checklist)\b",
    flags=re.IGNORECASE,
)

RESOURCE_FACT_SCHEMAS: list[Json] = [
    {
        "fact_type": "resource_decision",
        "entity_type": "resource_decision",
        "entity_prefix": "decision",
        "keywords": ["decision", "decided", "approved", "rejected", "selected"],
    },
    {
        "fact_type": "resource_owner",
        "entity_type": "resource_owner",
        "entity_prefix": "owner",
        "keywords": ["owner", "owns", "reviewer", "assignee", "responsible"],
    },
    {
        "fact_type": "resource_cost",
        "entity_type": "resource_cost",
        "entity_prefix": "cost",
        "keywords": ["cost", "budget", "amount", "price", "spend", "$"],
    },
    {
        "fact_type": "resource_deadline",
        "entity_type": "resource_deadline",
        "entity_prefix": "deadline",
        "keywords": ["deadline", "due", "by monday", "by tuesday", "by wednesday", "by thursday", "by friday", "by saturday", "by sunday"],
    },
    {
        "fact_type": "resource_api_contract",
        "entity_type": "resource_api_contract",
        "entity_prefix": "api",
        "keywords": ["api", "endpoint", "contract", "schema", "request", "response", "http", "grpc"],
    },
    {
        "fact_type": "resource_troubleshooting_step",
        "entity_type": "resource_troubleshooting",
        "entity_prefix": "troubleshooting",
        "keywords": ["troubleshoot", "debug", "incident", "alert", "rollback", "runbook", "remediation", "mitigation"],
    },
    {
        "fact_type": "resource_policy",
        "entity_type": "resource_policy",
        "entity_prefix": "policy",
        "keywords": ["policy", "must", "should", "required", "requires", "cannot", "allowed"],
    },
    {
        "fact_type": "resource_approval",
        "entity_type": "resource_approval",
        "entity_prefix": "approval",
        "keywords": ["approval", "approved", "approve", "signoff", "confirmed"],
    },
    {
        "fact_type": "resource_control_state",
        "entity_type": "resource_control_state",
        "entity_prefix": "control_state",
        "keywords": ["control_state", "blocker", "blocked", "failure", "unsafe", "degraded"],
    },
    {
        "fact_type": "resource_procedure",
        "entity_type": "resource_procedure",
        "entity_prefix": "procedure",
        "keywords": ["procedure", "step", "checklist", "first", "then", "verify", "confirm"],
    },
]


def should_extract_resource_fact(text: str, metadata: Json) -> bool:
    if RESOURCE_FACT_KEYWORDS.search(text):
        return True
    unit_kind = str(metadata.get("unit_kind", ""))
    return unit_kind in {"table_row", "table_row_group", "xlsx_row", "xlsx_row_group", "json_document", "json_record", "json_record_group"}


def matched_resource_fact_schemas(text: str, metadata: Json) -> list[Json]:
    lower = text.lower()
    matches = [
        schema
        for schema in RESOURCE_FACT_SCHEMAS
        if any(keyword in lower for keyword in schema["keywords"])
    ]
    if matches:
        return matches[: max(0, MAX_RESOURCE_FACTS_PER_CHUNK)]
    if ENABLE_GENERIC_RESOURCE_FACTS and should_extract_resource_fact(text, metadata):
        return [{"fact_type": "resource_fact", "entity_type": "resource_fact", "entity_prefix": "fact", "keywords": []}]
    return []


def extract_resource_fact_value(text: str, fact_type: str) -> str:
    patterns = {
        "resource_owner": r"\b(?:owner|owns|reviewer|assignee|responsible)\s*(?:is|:|=)?\s*([^.;\n]{2,120})",
        "resource_deadline": r"\b(?:deadline|due)\s*(?:is|:|=|by)?\s*([^.;\n]{2,120})",
        "resource_cost": r"\b(?:cost|budget|amount|price|spend)\s*(?:is|:|=)?\s*([^.;\n]{2,120})",
        "resource_api_contract": r"\b(?:api|endpoint|contract|schema)\s*(?:is|:|=)?\s*([^.;\n]{2,160})",
        "resource_approval": r"\b(?:approval|approved|approve|signoff|confirmed)\s*(?:is|:|=)?\s*([^.;\n]{0,140})",
        "resource_control_state": r"\b(?:control_state|blocker|blocked|failure)\s*(?:is|:|=)?\s*([^.;\n]{2,160})",
        "resource_decision": r"\b(?:decision|decided)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_policy": r"\b(?:policy|must|should|required|requires)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_troubleshooting_step": r"\b(?:troubleshoot|debug|incident|alert|rollback|runbook|remediation|mitigation)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_procedure": r"\b(?:procedure|step|checklist|verify|confirm)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
    }
    pattern = patterns.get(fact_type, "")
    if pattern:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return preview_text(match.group(1).strip(" :-"), limit=180)
    return preview_text(text, limit=220)


def resource_fact_entity_name(schema: Json, value: str, chunk_metadata: Json, raw_uri: str) -> str:
    prefix = str(schema.get("entity_prefix") or schema.get("entity_type") or "fact")
    semantic_value = preview_text(str(value or "").strip(), limit=80).strip()
    if semantic_value:
        return f"{prefix}:{semantic_value}"
    heading = str(chunk_metadata.get("heading") or chunk_metadata.get("heading_slug") or "").strip()
    if heading:
        return f"{prefix}:{preview_text(heading, limit=80)}"
    return prefix



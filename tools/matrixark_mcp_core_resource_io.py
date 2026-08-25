# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
import hashlib
import os
import shutil
import subprocess
import tempfile
from typing import Any

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        Json,
        MatrixArkError,
        Path,
        safe_identifier,
        stable_hash,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        Path,
        safe_identifier,
        stable_hash,
    )

try:  # resource-parser helpers (core imports these inside a try/except too)
    from .matrixark_resource_parser import content_hash, normalize_parse_warnings
except ImportError:
    from matrixark_resource_parser import content_hash, normalize_parse_warnings

__all__ = ['deployment_scope_from_args', 'resource_storage_mode_from_args', 'resource_chunk_materialization_enabled', 'ATTACHMENT_RESOURCE_POLICY', 'bound_resource_event_text', 'bounded_buffer_envelope', 'RESOURCE_EVENT_TEXT_CHARS', 'is_s3_uri', 'parse_s3_uri', 'is_matrixobject_uri', 'parse_matrixobject_uri', 'is_temporalstore_uri', 'parse_temporalstore_uri', '_cloud_resource_bucket', '_cloud_resource_prefix', '_s3_client', '_aws_cli_s3_cp', 'upload_file_to_s3', 'download_s3_to_file', '_resource_object_key', '_archive_directory_for_upload', 'resolve_raw_resource_for_ingest', 'infer_resource_suffix', 'rewrite_chunk_uris', 'cleanup_temp_paths', 'aggregate_parse_warnings_from_chunks']


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


ATTACHMENT_RESOURCE_POLICY = "attachment"


def resource_chunk_materialization_enabled(args: Json, envelope: Json) -> bool:
    """Whether this resource should be chunked into the context store (default yes).

    A resource is normally chunked, embedded and indexed so it can be recalled selectively --
    that is what makes chunk-level retrieval work. For an ATTACHMENT the trade is wrong: the
    file is fetched rarely, if ever, and it is already durable behind its raw URI, yet the
    context store pays full retrieval cost for it.

    Measured on one 66.2 KB document, with the 32-dim deterministic encoder:

        resource_chunk       60 records   254,293 bytes  (3.75x source)
        context_embedding    76 records   198,656 bytes  (2.93x source)
        TOTAL                            477,827 bytes  (7.05x source)

    The embedding line is the floor, not the ceiling: those vectors are 32-dim. With a real
    encoder (MiniLM, 384 dims) the same attachment approaches 40x its own size.

    Setting raw_storage_policy="attachment" keeps the manifest and the raw URI -- the file stays
    listed, addressable and fetchable on demand -- and skips chunk materialization, so it costs
    metadata instead of multiples of itself. Anything else keeps today's behaviour exactly.
    """
    value = str(
        args.get("raw_storage_policy")
        or envelope.get("metadata", {}).get("raw_storage_policy")
        or os.environ.get("MATRIXARK_RESOURCE_STORAGE_POLICY")
        or ""
    ).strip().lower()
    return value != ATTACHMENT_RESOURCE_POLICY


RESOURCE_EVENT_TEXT_CHARS = int(os.environ.get("MATRIXARK_RESOURCE_EVENT_TEXT_CHARS", "4096"))


def bound_resource_event_text(kind: str, text: str, raw_uri: str) -> str:
    """Bound the context_event text for a resource/skill ingest; messages are left untouched.

    A resource ingest stored its whole document three times: in the chunk records, in the
    context_event, and inside the session_buffer_event envelope. Measured on a 66.2 KB file the
    event alone was 1.05x source.

    The event is not where a resource's content belongs -- chunks are the retrievable form and
    the raw URI the durable one -- so the event keeps a leading excerpt plus a pointer to the
    full content.

    This must NOT be done by clipping envelope["messages"]: resource_text, and therefore every
    chunk, is derived from that same list, so clipping there would truncate the DOCUMENT rather
    than deduplicate its storage. The bound belongs on the record, not on the input.

    Set MATRIXARK_RESOURCE_EVENT_TEXT_CHARS=0 to store the full text.
    """
    if kind not in {"resource", "skill"}:
        return text
    limit = RESOURCE_EVENT_TEXT_CHARS
    if limit <= 0 or len(text) <= limit:
        return text
    pointer = raw_uri or "the stored resource"
    return "{}\n\n[{} of {} chars; full content in the resource chunks and at {}]".format(
        text[:limit], limit, len(text), pointer,
    )

def bounded_buffer_envelope(envelope: Json) -> Json:
    """A copy of `envelope` safe to store in a session_buffer_event.

    The buffer record embeds the whole envelope, so a resource ingest kept its document a
    third time there -- 1.06x source on a 66.2 KB file, the last full copy after the chunk
    records and the event text.

    Only message CONTENT is bounded, and only for resource/skill kinds. Everything the commit
    path actually reads off the buffered envelope is preserved untouched: metadata, hook_type,
    codex_event, and each message's role -- messages_from_event_record() reads
    envelope["messages"] for role counting, so the list shape and roles must survive.

    Returns a COPY. The caller's envelope is still feeding chunk parsing downstream, and
    resource_text is derived from that same messages list, so mutating it in place would
    truncate the document rather than its duplicate storage.
    """
    if envelope.get("kind") not in {"resource", "skill"}:
        return envelope
    limit = RESOURCE_EVENT_TEXT_CHARS
    messages = envelope.get("messages")
    if limit <= 0 or not isinstance(messages, list):
        return envelope
    bounded = []
    for message in messages:
        if not isinstance(message, dict):
            bounded.append(message)
            continue
        content = str(message.get("content") or "")
        if len(content) <= limit:
            bounded.append(message)
            continue
        trimmed = dict(message)
        trimmed["content"] = content[:limit] + "\n\n[bounded: " + str(len(content)) + " chars; full content in the resource chunks]"
        bounded.append(trimmed)
    copied = dict(envelope)
    copied["messages"] = bounded
    return copied

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


def is_matrixobject_uri(value: str) -> bool:
    return bool(value) and value.startswith("matrixobject://")


def parse_matrixobject_uri(uri: str) -> tuple[str, str]:
    """matrixobject://<bucket>/<key> -> (bucket, key). Key may contain slashes
    (the object client stores content-addressed ``ab/cd/<sha256>`` keys)."""
    if not is_matrixobject_uri(uri):
        raise MatrixArkError(f"not a matrixobject uri: {uri}")
    rest = uri[len("matrixobject://") :]
    bucket, sep, key = rest.partition("/")
    if not bucket or not sep or not key:
        raise MatrixArkError(f"invalid matrixobject uri: {uri}")
    return bucket, key


def is_temporalstore_uri(value: str) -> bool:
    return bool(value) and value.startswith("temporalstore://")


def parse_temporalstore_uri(uri: str) -> str:
    """temporalstore://<key> -> key. The key may contain slashes (the blob client
    stores content-addressed ``resources/ab/<sha256>`` keys)."""
    if not is_temporalstore_uri(uri):
        raise MatrixArkError(f"not a temporalstore uri: {uri}")
    key = uri[len("temporalstore://") :]
    if not key:
        raise MatrixArkError(f"invalid temporalstore uri: {uri}")
    return key


def _resource_object_backend_choice(args: Json, envelope: Json) -> str:
    """Select the cloud raw-blob backend: matrixobject | temporalstore | auto.

    'auto' (default): MatrixObject when an object backend is configured, else
    TemporalStore's own blob tier when a blob endpoint is configured, else fall
    through to S3. An explicit value forces that backend when it is available.
    """
    value = str(
        args.get("raw_object_backend")
        or envelope.get("metadata", {}).get("raw_object_backend")
        or os.environ.get("MATRIXARK_RESOURCE_OBJECT_BACKEND")
        or "auto"
    ).strip().lower()
    return value if value in {"matrixobject", "temporalstore", "auto"} else "auto"


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
    # mem0 agent_id: per-agent raw-blob isolation, appended only when supplied so
    # existing (agent-less) layouts are byte-identical.
    agent_id = str(scope.get("agent_id") or "")
    if agent_id:
        parts.append(safe_identifier(agent_id, default="agent"))
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

    # matrixobject:// input -> the raw bytes already live in our object store.
    # Fetch them via MatrixObjectClient, drop them in a temp file, and let the
    # parser chunk that file (parse_text=None). Parallel to the s3:// download
    # branch below; non-breaking for inline/local/s3 inputs.
    if is_matrixobject_uri(raw_uri):
        obj_bucket, obj_key = parse_matrixobject_uri(raw_uri)
        try:  # top-level path (matches the object-upload branch's import style)
            from matrixark_object_store import MatrixObjectClient as _ObjClient
        except ImportError:  # package path
            from tools.matrixark_object_store import MatrixObjectClient as _ObjClient  # type: ignore
        data, meta = _ObjClient(bucket=obj_bucket).get(obj_key, bucket=obj_bucket)
        suffix = infer_resource_suffix(resource_type, obj_key)
        temp_file = Path(tempfile.mkdtemp(prefix="matrixark-object-resource-")) / f"downloaded.{suffix}"
        temp_file.write_bytes(data if isinstance(data, (bytes, bytearray)) else bytes(data))
        result["temp_paths"].append(str(temp_file.parent))
        result["parse_uri"] = str(temp_file)
        result["parse_text"] = None
        result["storage_mode"] = "matrixobject"
        result["stored_raw_uri"] = raw_uri
        result["raw_storage_policy"] = "object_store"
        result["raw_bytes_stored"] = True
        result["cloud_bucket"] = obj_bucket
        result["cloud_key"] = obj_key
        if isinstance(meta, dict) and meta.get("content_hash"):
            result["content_hash"] = meta["content_hash"]
        return result

    # temporalstore:// input -> the raw bytes already live in TemporalStore's own
    # blob tier. Fetch them via TemporalStoreBlobClient, drop them in a temp file,
    # and let the parser chunk that file (parse_text=None). Parallel to the
    # matrixobject:// branch above; the OSS-safe (no object store) counterpart.
    if is_temporalstore_uri(raw_uri):
        # Engine-tier URI (two 16-hex segments): the bytes live in the embedded engine's blob
        # store, reachable only through a rust proxy client -- the HTTP tier can never serve
        # this shape, so falling through would be a guaranteed miss dressed as a 404.
        try:  # top-level path
            from matrixark_temporalstore_blob import (
                engine_blob_get as _engine_get,
                engine_blob_supported as _engine_ok,
                parse_engine_blob_uri as _engine_parse,
            )
        except ImportError:  # package path
            from tools.matrixark_temporalstore_blob import (  # type: ignore
                engine_blob_get as _engine_get,
                engine_blob_supported as _engine_ok,
                parse_engine_blob_uri as _engine_parse,
            )
        if _engine_parse(raw_uri) is not None:
            engine_client = args.get("_engine_blob_client") if isinstance(args, dict) else None
            if not _engine_ok(engine_client):
                raise MatrixArkError(
                    f"resource uri {raw_uri} names the engine blob tier, but this adapter has "
                    "no rust proxy client to fetch it through"
                )
            data = _engine_get(engine_client, raw_uri)
            suffix = infer_resource_suffix(resource_type, raw_uri)
            temp_file = Path(tempfile.mkdtemp(prefix="matrixark-engineblob-resource-")) / f"downloaded.{suffix}"
            temp_file.write_bytes(data)
            result["temp_paths"].append(str(temp_file.parent))
            result["parse_uri"] = str(temp_file)
            result["parse_text"] = None
            result["storage_mode"] = "temporalstore"
            result["stored_raw_uri"] = raw_uri
            result["raw_storage_policy"] = "temporalstore_engine_blob"
            result["raw_bytes_stored"] = True
            result["cloud_key"] = raw_uri.split("://", 1)[-1]
            return result
        ts_key = parse_temporalstore_uri(raw_uri)
        try:  # top-level path (matches the object-download branch's import style)
            from matrixark_temporalstore_blob import TemporalStoreBlobClient as _TsBlobClient
        except ImportError:  # package path
            from tools.matrixark_temporalstore_blob import TemporalStoreBlobClient as _TsBlobClient  # type: ignore
        data, meta = _TsBlobClient().get(ts_key)
        suffix = infer_resource_suffix(resource_type, ts_key)
        temp_file = Path(tempfile.mkdtemp(prefix="matrixark-tsblob-resource-")) / f"downloaded.{suffix}"
        temp_file.write_bytes(data if isinstance(data, (bytes, bytearray)) else bytes(data))
        result["temp_paths"].append(str(temp_file.parent))
        result["parse_uri"] = str(temp_file)
        result["parse_text"] = None
        result["storage_mode"] = "temporalstore"
        result["stored_raw_uri"] = raw_uri
        result["raw_storage_policy"] = "temporalstore_blob"
        result["raw_bytes_stored"] = True
        result["cloud_key"] = ts_key
        if isinstance(meta, dict) and meta.get("content_hash"):
            result["content_hash"] = meta["content_hash"]
        return result

    local_path = Path(raw_uri) if raw_uri != "inline-resource" and not is_s3_uri(raw_uri) else None
    if mode == "local":
        if local_path is not None and local_path.exists():
            result["parse_text"] = None
        return result

    # Prefer MatrixObject over S3 for cloud raw storage when an object backend is configured
    # (MATRIXARK_OBJECT_RPC_URL for the Rust proxy object RPC, or MATRIXARK_OBJECT_STORE_DIR).
    # Uses its own object bucket and does NOT require an S3 bucket. Falls through to S3 below
    # when no object backend is set (backward compatible).
    if not is_s3_uri(raw_uri):
        backend_choice = _resource_object_backend_choice(args, envelope)
        # MatrixObject (enterprise object store) availability.
        try:
            from matrixark_object_store import (
                resolve_object_backend as _obj_backend,
                MatrixObjectClient as _ObjClient,
                resource_blob_storage_resolution as _obj_resolution,
                DEFAULT_BUCKET as _OBJ_DEFAULT_BUCKET,
            )
        except ImportError:
            _obj_backend = None
        # TemporalStore's own blob tier availability (OSS-safe, no object store).
        try:  # top-level path
            from matrixark_temporalstore_blob import (
                resolve_ts_blob_backend as _ts_backend,
                TemporalStoreBlobClient as _TsBlobClient,
                ts_blob_storage_resolution as _ts_resolution,
            )
        except ImportError:
            try:  # package path
                from tools.matrixark_temporalstore_blob import (  # type: ignore
                    resolve_ts_blob_backend as _ts_backend,
                    TemporalStoreBlobClient as _TsBlobClient,
                    ts_blob_storage_resolution as _ts_resolution,
                )
            except ImportError:
                _ts_backend = None

        obj_ok = _obj_backend is not None and _obj_backend() != "inline"
        ts_ok = _ts_backend is not None and _ts_backend() != "inline"
        # The engine tier is the fallback when neither an object backend nor the HTTP blob
        # endpoint is configured but the adapter rides a rust proxy client: the embedded
        # engine stores the attachment itself, so one TemporalStore still holds everything.
        engine_client = args.get("_engine_blob_client") if isinstance(args, dict) else None
        engine_ok = False
        if engine_client is not None and _ts_backend is not None:
            try:  # top-level path
                from matrixark_temporalstore_blob import engine_blob_supported as _engine_ok_fn
            except ImportError:  # package path
                from tools.matrixark_temporalstore_blob import engine_blob_supported as _engine_ok_fn  # type: ignore
            engine_ok = _engine_ok_fn(engine_client)
        if backend_choice == "matrixobject":
            chosen = "matrixobject" if obj_ok else None
        elif backend_choice == "temporalstore":
            chosen = "temporalstore" if ts_ok else ("temporalstore_engine" if engine_ok else None)
        else:  # auto: object store first (dedup/serving), then TemporalStore blob, then engine
            chosen = (
                "matrixobject"
                if obj_ok
                else ("temporalstore" if ts_ok else ("temporalstore_engine" if engine_ok else None))
            )

        if chosen is not None:
            # Build the blob (same local-file / dir-archive / inline-text logic
            # for both backends).
            if local_path is not None and local_path.exists():
                if local_path.is_dir():
                    upload_path = _archive_directory_for_upload(local_path)
                    result["temp_paths"].append(str(upload_path.parent))
                    content_type = "application/gzip"
                else:
                    upload_path = local_path
                    content_type = "application/octet-stream"
                blob = Path(upload_path).read_bytes()
                result["parse_uri"] = str(local_path)   # parse locally; the file is still present
                result["parse_text"] = None
            else:
                blob = (resource_text or "").encode("utf-8")
                content_type = "text/markdown"

            if chosen == "matrixobject":
                obj_bucket = str(
                    args.get("object_bucket")
                    or envelope.get("metadata", {}).get("object_bucket")
                    or _OBJ_DEFAULT_BUCKET
                )
                result["cloud_bucket"] = obj_bucket
                blob_res = _obj_resolution(
                    _ObjClient(bucket=obj_bucket), blob, source_uri=raw_uri, content_type=content_type,
                    scope=envelope.get("scope"), kind=str(envelope.get("kind") or resource_type or "resource"),
                    name=str(envelope.get("metadata", {}).get("name") or ""), bucket=obj_bucket,
                )
            elif chosen == "temporalstore_engine":
                try:  # top-level path
                    from matrixark_temporalstore_blob import engine_blob_put as _engine_put
                except ImportError:  # package path
                    from tools.matrixark_temporalstore_blob import engine_blob_put as _engine_put  # type: ignore
                scope = envelope.get("scope") or {}
                tenant_hash = stable_hash(
                    f"{scope.get('account_id') or ''}:{scope.get('tenant_id') or ''}"
                )
                blob_res = {"storage_mode": "temporalstore", "cloud_bucket": ""}
                blob_res.update(_engine_put(engine_client, tenant_hash, blob))
            else:  # temporalstore blob tier
                blob_res = _ts_resolution(
                    _TsBlobClient(), blob, source_uri=raw_uri, content_type=content_type,
                    scope=envelope.get("scope"), kind=str(envelope.get("kind") or resource_type or "resource"),
                    name=str(envelope.get("metadata", {}).get("name") or ""),
                )
            for _k in ("storage_mode", "stored_raw_uri", "raw_storage_policy", "raw_bytes_stored",
                       "upload_status", "cloud_bucket", "cloud_key"):
                result[_k] = blob_res[_k]
            if result.get("parse_text") is None and result.get("parse_uri") in (raw_uri, blob_res["stored_raw_uri"]):
                result["parse_text"] = resource_text  # inline path -> parse from the in-hand text
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



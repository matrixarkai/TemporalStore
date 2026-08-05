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

__all__ = ['deployment_scope_from_args', 'resource_storage_mode_from_args', 'is_s3_uri', 'parse_s3_uri', '_cloud_resource_bucket', '_cloud_resource_prefix', '_s3_client', '_aws_cli_s3_cp', 'upload_file_to_s3', 'download_s3_to_file', '_resource_object_key', '_archive_directory_for_upload', 'resolve_raw_resource_for_ingest', 'infer_resource_suffix', 'rewrite_chunk_uris', 'cleanup_temp_paths', 'aggregate_parse_warnings_from_chunks']


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



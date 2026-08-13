# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of run_matrixark_rust_scale_report.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import argparse
import os
import subprocess
import time

try:  # package path (tools.run_matrixark_rust_scale_report)
    from .run_matrixark_rust_scale_report import (
        CANONICAL_UBUNTU_REPO,
        Json,
        Path,
        ROOT,
        metaserver_reachable,
    )
except ImportError:  # top-level path (run_matrixark_rust_scale_report)
    from run_matrixark_rust_scale_report import (
        CANONICAL_UBUNTU_REPO,
        Json,
        Path,
        ROOT,
        metaserver_reachable,
    )

__all__ = ['_is_windows_host', '_wsl_path', '_linux_so_on_windows_error', 'validate_runtime_host', 'default_lib_path', 'default_rust_cli_path', '_metaserver_host', '_is_loopback_metaserver', 'default_canonical_release_out_dir', 'ensure_local_topology', 'validate_rust_runtime_path']


def _is_windows_host() -> bool:
    return os.name == "nt"


def _wsl_path(path: str) -> str:
    normalized = str(path).replace("\\", "/")
    if len(normalized) >= 3 and normalized[1] == ":" and normalized[2] == "/":
        drive = normalized[0].lower()
        return f"/mnt/{drive}/{normalized[3:]}"
    return normalized


def _linux_so_on_windows_error(path: str) -> str:
    return (
        "invalid_host_platform: direct SDK parity requires loading libtemporalstore.so from "
        "a Linux process. The current runner is Windows Python, which cannot load a Linux .so. "
        "Run this command from WSL/Linux or provide a Windows-compatible temporalstore.dll. "
        f"WSL path hint: {_wsl_path(path)}"
    )


def validate_runtime_host(native_lib: str) -> None:
    suffix = Path(native_lib).suffix.lower()
    if _is_windows_host() and suffix == ".so":
        raise RuntimeError(_linux_so_on_windows_error(native_lib))


def default_lib_path() -> str:
    candidates = [
        ROOT / "output-ubuntu22/release/sdk/lib/libtemporalstore.so",
        CANONICAL_UBUNTU_REPO / "output-ubuntu22/release/sdk/lib/libtemporalstore.so",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(candidates[0])


def default_rust_cli_path() -> str:
    candidates = [
        ROOT / "target/release/matrixark_rust_proxy",
        CANONICAL_UBUNTU_REPO / "target/release/matrixark_rust_proxy",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(candidates[0])


def _metaserver_host(metaserver: str) -> str:
    if metaserver.startswith("["):
        return metaserver.split("]", 1)[0].lstrip("[")
    return metaserver.rsplit(":", 1)[0] if ":" in metaserver else metaserver


def _is_loopback_metaserver(metaserver: str) -> bool:
    return _metaserver_host(metaserver).strip().lower() in {"127.0.0.1", "localhost", "::1"}


def default_canonical_release_out_dir() -> Path:
    return CANONICAL_UBUNTU_REPO / "output-ubuntu22" / "release"


def ensure_local_topology(
    args: argparse.Namespace,
    *,
    force_restart: bool = False,
    reason: str = "metaserver_unreachable",
) -> Json:
    """Start local topology for loopback scale runs when it is absent or unhealthy."""

    if getattr(args, "no_auto_start_local_topology", False):
        return {"status": "skipped", "reason": "disabled_by_flag", "metaserver": args.metaserver}
    if not _is_loopback_metaserver(str(args.metaserver)):
        return {"status": "skipped", "reason": "non_loopback_metaserver", "metaserver": args.metaserver}
    before = metaserver_reachable(args.metaserver)
    if before.get("ok") and not force_restart:
        return {"status": "already_ready", "metaserver": args.metaserver, "precheck": before}

    deploy_root = CANONICAL_UBUNTU_REPO if CANONICAL_UBUNTU_REPO.exists() else ROOT
    deploy_script = deploy_root / "tools" / "deploy_local_ubuntu22.sh"
    out_dir = Path(os.environ.get("TEMPORALSTORE_CANONICAL_OUT_DIR", str(default_canonical_release_out_dir())))
    if not deploy_script.exists():
        return {
            "status": "failed",
            "reason": "deploy_script_missing",
            "metaserver": args.metaserver,
            "deploy_script": str(deploy_script),
            "precheck": before,
        }
    if not (out_dir / "sdk" / "lib" / "libtemporalstore.so").exists():
        return {
            "status": "failed",
            "reason": "canonical_release_bundle_missing",
            "metaserver": args.metaserver,
            "out_dir": str(out_dir),
            "precheck": before,
        }

    env = os.environ.copy()
    env.setdefault("BUILD_TYPE", "Release")
    env.setdefault("OUT_DIR", str(out_dir))
    env.setdefault("DEPLOY_DIR", "/tmp/temporalstore-parity-deploy")
    env.setdefault("PERSIST_DEPLOY_DIR", "1")
    env.setdefault("SERVER_EXTRA_FLAGS", "--storage_async=true --server_stopping_wait_s=1")
    timeout_sec = max(1, int(getattr(args, "local_topology_start_timeout_sec", 120) or 120))
    command = ["bash", str(deploy_script), "start"]
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            cwd=deploy_root,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "status": "failed",
            "reason": "deploy_timeout",
            "metaserver": args.metaserver,
            "deploy_script": str(deploy_script),
            "out_dir": str(out_dir),
            "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
            "precheck": before,
        }

    after = metaserver_reachable(args.metaserver)
    status = "started" if completed.returncode == 0 and after.get("ok") else "failed"
    return {
        "status": status,
        "reason": reason if status == "started" and force_restart else ("" if status == "started" else "deploy_failed_or_metaserver_unreachable"),
        "force_restart": force_restart,
        "metaserver": args.metaserver,
        "deploy_script": str(deploy_script),
        "out_dir": str(out_dir),
        "returncode": completed.returncode,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
        "precheck": before,
        "postcheck": after,
    }


def validate_rust_runtime_path(args: argparse.Namespace) -> None:
    path = Path(str(args.rust_cli))
    lowered = str(path).replace("\\", "/").lower()
    if not path.exists():
        raise RuntimeError(
            "Rust TemporalStore proxy binary is missing. Build the production proxy first, for example: "
            "`cargo build --release -p temporalstore-rust --bin matrixark_rust_proxy`. "
            f"Configured path: {path}"
        )
    if "/debug/" in lowered and not getattr(args, "allow_rust_debug_cli", False):
        raise RuntimeError(
            "Rust parity runs must not use debug artifacts. Use target/release/matrixark_rust_proxy "
            "or pass --allow-rust-debug-cli only for diagnostics. "
            f"Configured path: {path}"
        )
    if path.name == "matrixark_record_log" and not getattr(args, "allow_rust_record_log_compat", False):
        raise RuntimeError(
            "Rust parity runs must use the production-named matrixark_rust_proxy/direct SDK bridge. "
            "matrixark_record_log is retained only as a compatibility/debug wrapper. "
            "Pass --allow-rust-record-log-compat only for diagnostics. "
            f"Configured path: {path}"
        )

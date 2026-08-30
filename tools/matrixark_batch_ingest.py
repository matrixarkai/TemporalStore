# SPDX-License-Identifier: Apache-2.0
"""Batch-ingest a list of skill documents, one by one or many at a time, with live progress.

Customers arrive with a directory of playbooks, or a manifest listing them, and want them in the
store without writing their own loop. This is that loop, with the parts that are easy to get wrong
already handled:

* **Resumable.** Ingest is a keyed upsert on ``identity_key``, so re-running is safe by construction.
  A state file additionally lets a resumed run SKIP work it already did instead of redoing it.
* **Monitorable.** Progress prints to the terminal, and ``--status-file`` writes the same numbers as
  JSON on every update so a dashboard or a watching process can read them without scraping stdout.
* **Bounded.** ``--concurrency`` caps in-flight requests; failures retry with backoff and are
  reported at the end rather than aborting a long run.

The API key is read from an environment variable named by ``--api-key-env``. It is never accepted as
a command-line argument, so it cannot leak into shell history or a process listing.

Examples
--------
    # every markdown/json playbook under a directory, 4 at a time, resumable
    python3 matrixark_batch_ingest.py --dir ./playbooks --concurrency 4 \
        --state .ingest-state.json --resume

    # an explicit manifest, one at a time, writing status for a dashboard to poll
    python3 matrixark_batch_ingest.py --manifest skills.txt --concurrency 1 \
        --status-file /var/run/ingest-status.json

    # see what would be sent, without sending it
    python3 matrixark_batch_ingest.py --dir ./playbooks --dry-run
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import queue
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Dict, Iterable, List, Optional, Tuple

DEFAULT_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_GLOBS = ("*.md", "*.markdown", "*.json")
RETRY_BACKOFF_S = (1.0, 3.0, 8.0)


# ---------------------------------------------------------------------------------------------
# input discovery
# ---------------------------------------------------------------------------------------------
def discover_from_dir(root: str, globs: Iterable[str]) -> List[str]:
    """Every file under `root` matching any glob, sorted so runs are reproducible."""
    found: List[str] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if any(fnmatch.fnmatch(name, pattern) for pattern in globs):
                found.append(os.path.join(dirpath, name))
    return sorted(found)


def discover_from_manifest(path: str) -> List[str]:
    """Paths from a manifest: one per line, or JSON Lines with a "path" field.

    Blank lines and ``#`` comments are ignored so a manifest can be maintained by hand.
    """
    paths: List[str] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("{"):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise SystemExit("%s: bad JSON line: %s" % (path, exc)) from exc
                value = record.get("path") or record.get("file")
                if not value:
                    raise SystemExit("%s: JSON line has no 'path' field" % path)
                paths.append(str(value))
            else:
                paths.append(line)
    return paths


def resolve_inputs(args: argparse.Namespace) -> List[str]:
    """Positional paths, then --dir discovery, then --manifest; de-duplicated, order preserved."""
    paths: List[str] = list(args.paths)
    if args.dir:
        globs = args.glob or list(DEFAULT_GLOBS)
        paths.extend(discover_from_dir(args.dir, globs))
    if args.manifest:
        paths.extend(discover_from_manifest(args.manifest))
    seen = set()
    unique: List[str] = []
    for path in paths:
        if path not in seen:
            seen.add(path)
            unique.append(path)
    return unique


# ---------------------------------------------------------------------------------------------
# resume state
# ---------------------------------------------------------------------------------------------
def load_state(path: Optional[str]) -> Dict[str, str]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        done = data.get("done")
        return dict(done) if isinstance(done, dict) else {}
    except Exception:
        # A corrupt state file must not block a run: ingest is idempotent, so the worst case of
        # starting over is wasted time, never wrong data.
        return {}


def save_state(path: Optional[str], done: Dict[str, str]) -> None:
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump({"done": done}, handle)
    os.replace(tmp, path)


def identity_key_for(path: str) -> str:
    """Stable identity for a document, so re-ingesting replaces rather than duplicates."""
    return "skill:" + os.path.abspath(path)


def resource_type_for(path: str) -> str:
    return "json" if path.lower().endswith(".json") else "markdown"


# ---------------------------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------------------------
def post_document(
    base_url: str, path: str, *, user_id: str, api_key: str, timeout_s: float
) -> Tuple[bool, str]:
    """Ingest one document. Returns (ok, detail). Retries transient failures with backoff."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            content = handle.read()
    except OSError as exc:
        return False, "read failed: %s" % exc

    body = {
        "kind": "resource",
        "resource_type": resource_type_for(path),
        "raw_uri": os.path.abspath(path),
        "identity_key": identity_key_for(path),
        "content": content,
        "messages": [{"role": "user", "content": content}],
        "user_id": user_id,
        "scope": {"user_id": user_id},
    }
    payload = json.dumps(body).encode("utf-8")
    url = base_url.rstrip("/") + "/v1/ingest"

    last = ""
    for backoff in (0.0,) + RETRY_BACKOFF_S:
        if backoff:
            time.sleep(backoff)
        request = urllib.request.Request(
            url, data=payload, method="POST", headers={"Content-Type": "application/json"}
        )
        if api_key:
            request.add_header("Authorization", "Bearer " + api_key)
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                response.read()
                if 200 <= response.status < 300:
                    return True, "http %d" % response.status
                last = "http %d" % response.status
        except urllib.error.HTTPError as exc:
            last = "http %d" % exc.code
            # A 4xx is the caller's fault and will not improve on retry.
            if 400 <= exc.code < 500:
                return False, last
        except Exception as exc:  # network/timeout: worth retrying
            last = ("%s: %s" % (type(exc).__name__, exc))[:120]
    return False, last


# ---------------------------------------------------------------------------------------------
# progress
# ---------------------------------------------------------------------------------------------
class Progress:
    """Counters plus their two renderings: a terminal line and a JSON status file."""

    def __init__(self, total: int, status_file: Optional[str], quiet: bool) -> None:
        self.total = total
        self.status_file = status_file
        self.quiet = quiet
        self.done = 0
        self.failed = 0
        self.skipped = 0
        self.bytes = 0
        self.started = time.time()
        self.failures: List[Tuple[str, str]] = []
        self._lock = threading.Lock()

    def snapshot(self) -> Dict[str, object]:
        elapsed = max(1e-6, time.time() - self.started)
        processed = self.done + self.failed
        rate = processed / elapsed
        remaining = max(0, self.total - processed - self.skipped)
        return {
            "total": self.total,
            "done": self.done,
            "failed": self.failed,
            "skipped": self.skipped,
            "remaining": remaining,
            "bytes": self.bytes,
            "elapsed_s": round(elapsed, 1),
            "docs_per_s": round(rate, 3),
            "eta_s": round(remaining / rate, 1) if rate > 0 and remaining else 0,
            "running": remaining > 0,
        }

    def _write_status(self, snap: Dict[str, object]) -> None:
        if not self.status_file:
            return
        try:
            tmp = self.status_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(snap, handle)
            os.replace(tmp, self.status_file)
        except OSError:
            pass  # monitoring must never break the run

    def update(self, *, ok: bool, path: str, detail: str, size: int, skipped: bool = False) -> None:
        with self._lock:
            if skipped:
                self.skipped += 1
            elif ok:
                self.done += 1
                self.bytes += size
            else:
                self.failed += 1
                self.failures.append((path, detail))
            snap = self.snapshot()
            self._write_status(snap)
            if not self.quiet:
                processed = self.done + self.failed + self.skipped
                print(
                    "  [%d/%d] ok=%d failed=%d skipped=%d %s/s eta=%ss"
                    % (
                        processed,
                        self.total,
                        self.done,
                        self.failed,
                        self.skipped,
                        snap["docs_per_s"],
                        snap["eta_s"],
                    ),
                    flush=True,
                )

    def finish(self) -> Dict[str, object]:
        snap = self.snapshot()
        snap["running"] = False
        self._write_status(snap)
        return snap


# ---------------------------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    paths = resolve_inputs(args)
    if not paths:
        print(
            "no input documents found (use --dir, --manifest, or positional paths)", file=sys.stderr
        )
        return 2

    if args.dry_run:
        print("would ingest %d document(s) to %s/v1/ingest:" % (len(paths), args.base_url.rstrip("/")))
        for path in paths[:20]:
            print("  %-8s %s" % (resource_type_for(path), path))
        if len(paths) > 20:
            print("  ... and %d more" % (len(paths) - 20))
        return 0

    api_key = os.environ.get(args.api_key_env, "").strip()
    state = load_state(args.state) if args.resume else {}
    progress = Progress(len(paths), args.status_file, args.quiet)

    work: "queue.Queue[str]" = queue.Queue()
    for path in paths:
        work.put(path)
    state_lock = threading.Lock()

    def worker() -> None:
        while True:
            try:
                path = work.get_nowait()
            except queue.Empty:
                return
            key = identity_key_for(path)
            if args.resume and key in state:
                progress.update(ok=True, path=path, detail="skipped", size=0, skipped=True)
                continue
            try:
                size = os.path.getsize(path)
            except OSError:
                size = 0
            ok, detail = post_document(
                args.base_url, path, user_id=args.user_id, api_key=api_key, timeout_s=args.timeout
            )
            if ok:
                with state_lock:
                    state[key] = detail
                    save_state(args.state, state)
            progress.update(ok=ok, path=path, detail=detail, size=size)

    threads = [
        threading.Thread(target=worker, name="ingest-%d" % index, daemon=True)
        for index in range(max(1, args.concurrency))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    snap = progress.finish()
    print(
        "\ningested %d/%d document(s) in %ss (%s/s, %.1f MB)"
        % (progress.done, len(paths), snap["elapsed_s"], snap["docs_per_s"], progress.bytes / 1e6)
    )
    if progress.skipped:
        print("skipped %d already-ingested document(s) (--resume)" % progress.skipped)
    if progress.failures:
        print("\n%d failure(s):" % len(progress.failures), file=sys.stderr)
        for path, detail in progress.failures[:20]:
            print("  %-24s %s" % (detail, path), file=sys.stderr)
        if len(progress.failures) > 20:
            print("  ... and %d more" % (len(progress.failures) - 20), file=sys.stderr)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="matrixark_batch_ingest",
        description="Batch-ingest skill documents with progress, resume, and bounded concurrency.",
    )
    parser.add_argument("paths", nargs="*", help="document paths to ingest")
    parser.add_argument("--dir", help="ingest every matching document under this directory")
    parser.add_argument(
        "--glob",
        action="append",
        help="glob to match under --dir (default: %s)" % " ".join(DEFAULT_GLOBS),
    )
    parser.add_argument("--manifest", help="file listing documents (one path per line, or JSON Lines)")
    parser.add_argument("--base-url", default=os.environ.get("MATRIXARK_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--user-id", default=os.environ.get("MATRIXARK_USER_ID", "default"))
    parser.add_argument(
        "--api-key-env",
        default="MATRIXARK_API_KEY",
        help="NAME of the env var holding the API key (never pass the key itself)",
    )
    parser.add_argument("--concurrency", type=int, default=1, help="documents in flight (default 1)")
    parser.add_argument("--timeout", type=float, default=300.0, help="per-request timeout in seconds")
    parser.add_argument("--state", help="resume state file (records completed documents)")
    parser.add_argument("--resume", action="store_true", help="skip documents recorded in --state")
    parser.add_argument("--status-file", help="write JSON progress here on every update")
    parser.add_argument("--dry-run", action="store_true", help="list what would be sent, send nothing")
    parser.add_argument("--quiet", action="store_true", help="suppress the per-document progress line")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.resume and not args.state:
        print("--resume needs --state FILE", file=sys.stderr)
        return 2
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())

# SPDX-License-Identifier: Apache-2.0
"""In-process ingestion jobs: start a batch import over HTTP and watch it while it runs.

The CLI (``matrixark_batch_ingest``) covers an operator with shell access. This covers the customer
who wants to hand over a list of skills and watch the import from a page, without a terminal — and it
gives Prometheus something to scrape, so an import shows up on a dashboard next to everything else.

A job runs in a background thread inside the gateway worker, walking the same
``post_document`` path the CLI uses, so both routes behave identically and there is one
implementation of the wire shape to keep correct.

**Path safety.** An HTTP endpoint that ingests server-side paths is a file-disclosure risk: without a
boundary, "ingest /etc" is a valid request. Every resolved path must therefore sit inside
``MATRIXARK_INGESTION_ROOT``. When that variable is unset, submitting paths is refused outright
rather than defaulting to the filesystem root — the safe default for a feature whose whole job is
reading files the caller names.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Dict, List, Optional

import matrixark_batch_ingest as batch

MAX_RETAINED_JOBS = 50
MAX_PATHS_PER_JOB = 100_000


# Failures kept per job. A 10k-document import that fails wholesale would otherwise hold every
# path twice. The snapshot says when it truncated, so a retry cannot quietly cover only the first
# slice of a larger problem.
MAX_FAILURES_KEPT = 500
# Completed documents kept for the progress view. A ring, not a log: enough to see the import
# moving and what it is chewing through, bounded so a long run cannot grow.
RECENT_KEPT = 25


def classify_failure(detail: str) -> bool:
    """Is this failure worth retrying?

    post_document already retries transient failures with backoff and gives up immediately on a
    4xx, so a 4xx here means the request itself is wrong -- a malformed document, a rejected key,
    a body over the limit -- and will fail again identically. Anything else (a timeout, a refused
    connection, an exhausted 5xx) is the deployment having a bad minute, which is exactly what a
    retry is for. Distinguishing them is the difference between "retry the 12 that timed out" and
    "re-run all 1000 and get the same 3 failures".
    """
    text = (detail or "").strip().lower()
    if text.startswith("http "):
        try:
            code = int(text.split()[1])
        except (IndexError, ValueError):
            return True
        # 429 (back-pressure) and 408 (timeout) are the server asking for the request again,
        # not rejecting it -- they are the most retryable failures there are, and a plain
        # "4xx is permanent" rule gets them exactly backwards.
        if code in (408, 429):
            return True
        return not (400 <= code < 500)
    if text.startswith("read failed"):
        return False  # the file did not open; retrying reads the same missing file
    return True


class IngestionRootNotConfigured(Exception):
    """Raised when a job is submitted but no ingestion root bounds it."""


class PathOutsideRoot(Exception):
    """Raised when a submitted path resolves outside the configured ingestion root."""


def ingestion_root() -> Optional[str]:
    root = os.environ.get("MATRIXARK_INGESTION_ROOT", "").strip()
    return os.path.abspath(root) if root else None


def _resolve_within_root(path: str, root: str) -> str:
    """Absolute path, proven to sit inside `root`. Resolves symlinks so a link cannot escape."""
    resolved = os.path.realpath(os.path.abspath(path))
    root_real = os.path.realpath(root)
    if resolved != root_real and not resolved.startswith(root_real + os.sep):
        raise PathOutsideRoot(path)
    return resolved


def resolve_request_paths(
    *, paths: Optional[List[str]] = None, directory: Optional[str] = None,
    globs: Optional[List[str]] = None,
) -> List[str]:
    """Every document a job should ingest, each one proven to be inside the ingestion root."""
    root = ingestion_root()
    if root is None:
        raise IngestionRootNotConfigured(
            "MATRIXARK_INGESTION_ROOT is not set: submitting server-side paths is refused so the "
            "endpoint cannot be used to read arbitrary files. Set it to the directory holding the "
            "documents to import."
        )

    collected: List[str] = []
    if directory:
        safe_dir = _resolve_within_root(directory, root)
        collected.extend(batch.discover_from_dir(safe_dir, globs or list(batch.DEFAULT_GLOBS)))
    for path in paths or []:
        collected.append(_resolve_within_root(path, root))

    seen = set()
    unique: List[str] = []
    for path in collected:
        if path not in seen:
            seen.add(path)
            unique.append(path)
    return unique[:MAX_PATHS_PER_JOB]


class Job:
    """One import: its counters, and the thread walking the document list."""

    def __init__(self, job_id: str, paths: List[str], options: Dict[str, object]) -> None:
        self.id = job_id
        self.paths = paths
        self.options = options
        self.state = "queued"
        self.total = len(paths)
        self.done = 0
        self.failed = 0
        self.bytes = 0
        self.started_at = time.time()
        self.finished_at: Optional[float] = None
        self.failures: List[Dict[str, object]] = []
        self.failures_truncated = False
        self.recent: List[Dict[str, object]] = []
        self.current: Optional[str] = None
        self.current_started: Optional[float] = None
        self.retry_of: Optional[str] = str(options.get("retry_of") or "") or None
        self.retried_by: Optional[str] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            elapsed = (self.finished_at or time.time()) - self.started_at
            processed = self.done + self.failed
            rate = processed / elapsed if elapsed > 0 else 0.0
            remaining = max(0, self.total - processed)
            return {
                "job_id": self.id,
                "state": self.state,
                "total": self.total,
                "done": self.done,
                "failed": self.failed,
                "remaining": remaining,
                "bytes": self.bytes,
                "elapsed_s": round(elapsed, 1),
                "docs_per_s": round(rate, 3),
                "eta_s": round(remaining / rate, 1) if rate > 0 and remaining else 0,
                "current": self.current,
                "current_elapsed_s": (round(time.time() - self.current_started, 1)
                                      if self.current_started else None),
                "failures": [dict(f) for f in self.failures[:20]],
                "failure_count": len(self.failures),
                "failures_truncated": self.failures_truncated,
                "retryable_failures": sum(1 for f in self.failures if f.get("retryable")),
                "recent": [dict(r) for r in self.recent],
                "retry_of": self.retry_of,
                "retried_by": self.retried_by,
                "user_id": self.options.get("user_id"),
            }

    def record(self, path: str, ok: bool, took_ms: float, size: int, detail: str = "") -> None:
        """Record one document's outcome: the counters, the failure list, and the recent ring.

        One place rather than inline in the runner, so the two bounds -- MAX_FAILURES_KEPT and
        RECENT_KEPT -- cannot be honoured by one caller and forgotten by the next.
        """
        with self._lock:
            if ok:
                self.done += 1
                self.bytes += size
            else:
                self.failed += 1
                if len(self.failures) < MAX_FAILURES_KEPT:
                    self.failures.append({
                        "path": path, "detail": detail, "bytes": size,
                        "retryable": classify_failure(detail),
                    })
                else:
                    self.failures_truncated = True
            self.recent.append({"path": path, "ok": ok, "ms": took_ms, "bytes": size})
            if len(self.recent) > RECENT_KEPT:
                del self.recent[0]

    def failed_paths(self, only_retryable: bool = True) -> List[str]:
        """The documents to hand a retry. Ordered as the original run saw them."""
        with self._lock:
            return [str(f["path"]) for f in self.failures
                    if not only_retryable or f.get("retryable")]

    def cancel(self) -> None:
        self._stop.set()

    def run(self) -> None:
        base_url = str(self.options.get("base_url") or "http://127.0.0.1:8080")
        user_id = str(self.options.get("user_id") or "default")
        api_key = os.environ.get(str(self.options.get("api_key_env") or "MATRIXARK_API_KEY"), "")
        timeout_s = float(self.options.get("timeout_s") or 1800.0)

        with self._lock:
            self.state = "running"
        for path in self.paths:
            if self._stop.is_set():
                with self._lock:
                    self.state = "cancelled"
                    self.finished_at = time.time()
                    self.current = None
                    self.current_started = None
                return
            started = time.time()
            with self._lock:
                self.current = path
                self.current_started = started
            try:
                size = os.path.getsize(path)
            except OSError:
                size = 0
            ok, detail = batch.post_document(
                base_url, path, user_id=user_id, api_key=api_key, timeout_s=timeout_s
            )
            self.record(path, ok, round((time.time() - started) * 1000.0, 1), size, detail)
        with self._lock:
            self.state = "failed" if self.failed and not self.done else "completed"
            self.finished_at = time.time()
            self.current = None
            self.current_started = None


class JobRegistry:
    """Every job this worker has run, newest first, bounded so a long-lived worker cannot grow."""

    def __init__(self) -> None:
        self._jobs: Dict[str, Job] = {}
        self._order: List[str] = []
        self._lock = threading.Lock()

    def submit(self, paths: List[str], options: Dict[str, object]) -> Job:
        job = Job(uuid.uuid4().hex[:12], paths, options)
        with self._lock:
            self._jobs[job.id] = job
            self._order.insert(0, job.id)
            while len(self._order) > MAX_RETAINED_JOBS:
                dropped = self._order.pop()
                self._jobs.pop(dropped, None)
        threading.Thread(target=job.run, name="ingest-job-%s" % job.id, daemon=True).start()
        return job

    def retry(self, job_id: str, only_retryable: bool = True) -> Optional[Job]:
        """Re-run a finished job's failed documents as a new job.

        Only the failures, not the whole list. Ingest is a keyed upsert, so re-running everything
        is *safe* -- which is exactly why it is the tempting thing to do, and why a thousand-document
        import with three failures gets re-run in full and takes as long the second time. The two
        jobs are linked in both directions so the history reads as one import rather than two
        unrelated ones.
        """
        parent = self.get(job_id)
        if parent is None:
            return None
        paths = parent.failed_paths(only_retryable=only_retryable)
        if not paths:
            return None
        options = dict(parent.options)
        options["retry_of"] = parent.id
        child = self.submit(paths, options)
        with parent._lock:  # noqa: SLF001 - same module, same object's lock
            parent.retried_by = child.id
        return child

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self) -> List[Dict[str, object]]:
        with self._lock:
            jobs = [self._jobs[jid] for jid in self._order if jid in self._jobs]
        return [job.snapshot() for job in jobs]

    def totals(self) -> Dict[str, float]:
        """Aggregate counters, shaped for a Prometheus scrape."""
        with self._lock:
            jobs = list(self._jobs.values())
        totals = {
            "jobs_total": float(len(jobs)),
            "jobs_running": 0.0,
            "documents_total": 0.0,
            "documents_done": 0.0,
            "documents_failed": 0.0,
            "documents_retried": 0.0,
            "documents_retryable": 0.0,
            "bytes_total": 0.0,
        }
        for job in jobs:
            snap = job.snapshot()
            if snap["state"] == "running":
                totals["jobs_running"] += 1
            totals["documents_total"] += float(snap["total"])
            totals["documents_done"] += float(snap["done"])
            totals["documents_failed"] += float(snap["failed"])
            totals["bytes_total"] += float(snap["bytes"])
            if snap.get("retry_of"):
                totals["documents_retried"] += float(snap["total"])
            totals["documents_retryable"] += float(snap.get("retryable_failures") or 0)
        return totals


REGISTRY = JobRegistry()


def prometheus_text(extra: Optional[Dict[str, float]] = None) -> str:
    """Ingestion counters in Prometheus text exposition format."""
    totals = REGISTRY.totals()
    lines = [
        "# HELP matrixark_ingestion_jobs_total Ingestion jobs submitted to this worker.",
        "# TYPE matrixark_ingestion_jobs_total counter",
        "matrixark_ingestion_jobs_total %g" % totals["jobs_total"],
        "# HELP matrixark_ingestion_jobs_running Ingestion jobs currently running.",
        "# TYPE matrixark_ingestion_jobs_running gauge",
        "matrixark_ingestion_jobs_running %g" % totals["jobs_running"],
        "# HELP matrixark_ingestion_documents_total Documents across all jobs.",
        "# TYPE matrixark_ingestion_documents_total counter",
        "matrixark_ingestion_documents_total %g" % totals["documents_total"],
        "# HELP matrixark_ingestion_documents_done Documents ingested successfully.",
        "# TYPE matrixark_ingestion_documents_done counter",
        "matrixark_ingestion_documents_done %g" % totals["documents_done"],
        "# HELP matrixark_ingestion_documents_failed Documents that failed to ingest.",
        "# TYPE matrixark_ingestion_documents_failed counter",
        "matrixark_ingestion_documents_failed %g" % totals["documents_failed"],
        "# HELP matrixark_ingestion_documents_retried Documents submitted as part of a retry.",
        "# TYPE matrixark_ingestion_documents_retried counter",
        "matrixark_ingestion_documents_retried %g" % totals["documents_retried"],
        "# HELP matrixark_ingestion_documents_retryable Failed documents that are worth retrying "
        "-- a timeout or an exhausted 5xx rather than a rejected request. Non-zero after an import "
        "means work is waiting for someone to press retry.",
        "# TYPE matrixark_ingestion_documents_retryable gauge",
        "matrixark_ingestion_documents_retryable %g" % totals["documents_retryable"],
        "# HELP matrixark_ingestion_bytes_total Source bytes ingested.",
        "# TYPE matrixark_ingestion_bytes_total counter",
        "matrixark_ingestion_bytes_total %g" % totals["bytes_total"],
    ]
    for name, value in sorted((extra or {}).items()):
        lines.append("# TYPE %s gauge" % name)
        lines.append("%s %g" % (name, value))
    return "\n".join(lines) + "\n"

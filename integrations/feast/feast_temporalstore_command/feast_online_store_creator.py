# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An ``OnlineStoreCreator`` so Feast's own universal suite can target this store.

Feast parametrizes its integration tests from ``AVAILABLE_ONLINE_STORES`` /
``FULL_REPO_CONFIGS``, and a third-party store joins that by supplying an
:class:`OnlineStoreCreator` -- a thing that brings a store up, hands back the
``online_store`` block to point at it, and tears it down again. Providing one is
step 4 of Feast's own "adding a new online store" guide, and it is the
difference between "we wrote some tests" and "Feast's suite runs against this".

STANDARD LIBRARY ONLY, AND NO CONTAINER. The published creators for other
stores use ``testcontainers`` and a Docker image. This one runs the data node
binary directly, because that is what the repository builds and because a test
that needs Docker is a test most contributors will skip. If you would rather
point at a node you already have running, set ``TEMPORALSTORE_TEST_ADDR`` and
this attaches to it instead of starting anything.

THE DENOMINATOR IS ASSERTED HERE TOO. ``create_online_store`` refuses to hand
back a config for a shard that is not empty: a universal-suite run that began
against someone else's rows would produce failures that look like this store's
fault, or passes that were never this store's doing.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
from typing import Any, Dict, Optional

__all__ = ["TemporalStoreOnlineStoreCreator"]

_STORE_TYPE = "feast_temporalstore_command.TemporalStoreCommandOnlineStore"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _find_server_binary() -> str:
    """The data node binary. Note the bin is named ``server``, not its filename.

    Stated because it has already cost time: the source file is
    ``src/bin/temporalstore_data_server.rs`` but ``[[bin]] name`` is ``server``,
    so ``cargo build --bin temporalstore_data_server`` fails and
    ``target/<profile>/temporalstore_data_server`` does not exist.
    """
    explicit = os.environ.get("TEMPORALSTORE_SERVER_BIN")
    if explicit:
        return explicit
    here = os.path.dirname(os.path.abspath(__file__))
    # integrations/feast/feast_temporalstore_command -> repository root
    root = os.path.abspath(os.path.join(here, "..", "..", ".."))
    for profile in ("release", "debug"):
        candidate = os.path.join(root, "target", profile, "server")
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError(
        "no data node binary found. Build it with "
        "`cargo build --release -p temporalstore-rust --bin server`, or set "
        "TEMPORALSTORE_SERVER_BIN, or set TEMPORALSTORE_TEST_ADDR to attach to "
        "a node that is already running."
    )


class TemporalStoreOnlineStoreCreator:
    """Brings a TemporalStore data node up for the duration of a test run."""

    def __init__(self, project_name: str, **kwargs: Any) -> None:
        self.project_name = project_name
        self.shard_id = int(kwargs.get("shard_id", 1))
        self._process: Optional[subprocess.Popen] = None
        self._tempdir: Optional[str] = None
        self._attached = False
        self._addr: Optional[str] = None

    # -- helpers -------------------------------------------------------------

    def _get(self, path: str, timeout: float = 30.0):
        with urllib.request.urlopen(f"http://{self._addr}{path}", timeout=timeout) as r:
            return json.load(r)

    def _total_records(self) -> Optional[int]:
        try:
            for shard in self._get("/server/info"):
                if shard.get("shard_id") == self.shard_id:
                    return shard.get("total_records")
        except Exception:
            return None
        return None

    def _wait_healthy(self, seconds: int = 60) -> None:
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                if (self._get("/health", timeout=3) or {}).get("ok"):
                    return
            except Exception:
                pass
            if self._process is not None and self._process.poll() is not None:
                log = ""
                if self._tempdir:
                    try:
                        with open(os.path.join(self._tempdir, "server.log")) as handle:
                            log = handle.read()[-2000:]
                    except OSError:
                        pass
                raise RuntimeError(
                    f"the data node exited with {self._process.returncode} before "
                    f"becoming healthy. Last log:\n{log}"
                )
            time.sleep(0.5)
        raise RuntimeError(f"the data node at {self._addr} never answered /health")

    # -- the OnlineStoreCreator contract -------------------------------------

    def create_online_store(self) -> Dict[str, Any]:
        attached = os.environ.get("TEMPORALSTORE_TEST_ADDR")
        if attached:
            self._addr = attached
            self._attached = True
        else:
            port = _free_port()
            self._addr = f"127.0.0.1:{port}"
            self._tempdir = tempfile.mkdtemp(prefix="feast-temporalstore-")
            environment = dict(
                os.environ,
                TS_SERVER_BIND_ADDR=self._addr,
                TS_SERVER_ADVERTISE_ADDR=self._addr,
                TS_SHARD_ID=str(self.shard_id),
                TS_SERVER_NODE_ID="1",
                TS_CACHE_DIR=os.path.join(self._tempdir, "cache"),
                TS_BLOCK_STORE_DIR=os.path.join(self._tempdir, "blocks"),
                TS_INDEX_DIR=os.path.join(self._tempdir, "indexes"),
            )
            log_path = os.path.join(self._tempdir, "server.log")
            with open(log_path, "wb") as log:
                self._process = subprocess.Popen(
                    [_find_server_binary()],
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
        self._wait_healthy()

        records = self._total_records()
        if records:
            raise RuntimeError(
                f"shard {self.shard_id} at {self._addr} already holds {records} "
                f"records. A universal-suite run against a populated shard "
                f"produces failures that are not this store's and passes that "
                f"are not either, so it is refused."
            )

        host, _, port_text = self._addr.rpartition(":")
        return {
            "type": _STORE_TYPE,
            "host": host,
            "port": int(port_text),
            "shard_id": self.shard_id,
            # The suite writes small batches and expects to read them straight
            # back, so the dedup read is left ON -- it is the default and the
            # behaviour Feast's own tests assume.
            "timeout_seconds": 60.0,
        }

    def teardown(self) -> None:
        """Stop only the process this object started, by its own handle."""
        if self._process is not None:
            self._process.terminate()
            try:
                self._process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=20)
            self._process = None
        if self._tempdir:
            shutil.rmtree(self._tempdir, ignore_errors=True)
            self._tempdir = None
        self._attached = False

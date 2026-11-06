#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Compare feature-serving reads across the online stores Feast supports.

Every arm is driven **through Feast itself**, so the only thing that differs
between two rows of the output table is the ``online_store`` block in
``feature_store.yaml``. The harness refuses to print a number it cannot defend:

* **Matched payloads.** The feature values are built once and written to every
  arm, so each arm stores the identical ``ValueProto`` bytes. The exact
  per-row payload width is measured, printed, and asserted equal across arms.
  A ratio taken between arms holding different payloads means nothing, so the
  harness makes that impossible rather than warning about it.
* **The denominator is asserted before any per-unit figure.** Every entity is
  read back and compared against what was written before a single latency is
  recorded. An arm that served zero rows, or served the wrong value, fails the
  run instead of dividing into a beautiful number.
* **A control the measurement cannot influence.** The SQLite arm runs first and
  again last. SQLite is untouched by anything the other arms do, so if its two
  p50s disagree by more than ``--control-tolerance``, the host or the harness
  moved during the run and the whole table is marked void.
* **Warm and cold are separate columns**, never averaged together. The cold
  column is a single pass over every entity, each touched once. The warm column
  is a bounded hot set re-read after an explicit warm-up.
* **Durability is a column, not a footnote.** It is printed for every arm, and
  TemporalStore appears twice -- once committing asynchronously and once
  durably -- because a store answering from memory and a store fsyncing per
  write are not the same system.
* **Load is stamped on every phase.** A p99 taken on a box at load 90 measures
  the box. Exceeding ``--max-load`` marks the figures PROVISIONAL and exits
  non-zero.

Usage::

    python bench_feature_serving.py --arms sqlite,redis,postgres,ts_async \\
        --entities 10000 --hot 512 --reads 2000
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import statistics
import sys
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# payload profiles
# ---------------------------------------------------------------------------

PAYLOADS = {
    # Ten integer features: the shape a counter-heavy view has.
    "small": {"ints": 10, "blob_bytes": 0, "sequence_len": 0},
    # Four integers and a kilobyte blob: an embedding or a serialized struct.
    "large": {"ints": 4, "blob_bytes": 1024, "sequence_len": 0},
    # A Feast list feature, not an opaque byte blob. Opt in with --payloads sequence.
    "sequence": {"ints": 1, "blob_bytes": 0, "sequence_len": 4096},
}


@dataclass
class Phase:
    name: str
    latencies_us: List[float] = field(default_factory=list)
    load_before: float = 0.0
    load_after: float = 0.0
    rows_served: int = 0
    wall_seconds: float = 0.0

    def percentile(self, fraction: float) -> float:
        if not self.latencies_us:
            return float("nan")
        ordered = sorted(self.latencies_us)
        index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
        return ordered[index]

    @property
    def p50(self) -> float:
        return self.percentile(0.50)

    @property
    def p99(self) -> float:
        return self.percentile(0.99)

    @property
    def calls(self) -> int:
        return len(self.latencies_us)

    @property
    def throughput_rows_per_s(self) -> float:
        total_us = self.wall_seconds * 1_000_000 if self.wall_seconds else sum(self.latencies_us)
        if total_us <= 0 or self.rows_served == 0:
            return float("nan")
        return self.rows_served / (total_us / 1_000_000.0)


@dataclass
class ArmResult:
    arm: str
    store_label: str
    durability: str
    transport: str
    payload_bytes_per_row: int
    entities: int
    cold: Phase
    warm_b1: Phase
    warm_bn: Phase
    serving: Phase
    concurrent: Optional[Phase]
    write_seconds: float
    batch_n: int
    void_reason: Optional[str] = None


def load_average() -> float:
    """Raw 1-minute load average. REPORTED, never gated on -- see io_pressure.

    Measured on this host: 10,206 with 21 GB of 24 GB available, no oom-kill in
    dmesg, /proc/pressure/io some avg10 at 5% and ZERO tasks in uninterruptible
    sleep. The kernel counts the hypervisor's per-call relay helpers, of which a
    long session accumulates hundreds. A threshold on this number is a coin
    flip, so it is printed for context and nothing decides on it.
    """
    return os.getloadavg()[0]


def io_pressure() -> float:
    """Percentage of the last 10s in which at least one task stalled on I/O.

    This is the contention signal that means what it says, and it is what
    --max-io-pressure gates on. Absent (no PSI) reads as 0.0 rather than as a
    failure, because a kernel without pressure accounting is not a loaded box.
    """
    try:
        with open("/proc/pressure/io") as handle:
            for line in handle:
                if line.startswith("some "):
                    for field in line.split():
                        if field.startswith("avg10="):
                            return float(field.split("=", 1)[1])
    except OSError:
        return 0.0
    return 0.0


# ---------------------------------------------------------------------------
# arm definitions
# ---------------------------------------------------------------------------


def arm_online_store_block(arm: str, args: argparse.Namespace, repo_dir: str) -> Dict[str, Any]:
    if arm in ("sqlite", "sqlite_control"):
        return {"type": "sqlite", "path": os.path.join(repo_dir, "online.db")}
    if arm == "redis":
        return {
            "type": "redis",
            "redis_type": "redis",
            "connection_string": args.redis,
        }
    if arm == "redis_aof":
        # The SAME Feast online-store type as `redis`: the difference is entirely
        # in how the server it points at was started, which is why the address is
        # a separate flag rather than a mode on one. Keeping the arm separate
        # means both can run in ONE process, so the durable and non-durable
        # figures share a control and a payload assertion.
        return {
            "type": "redis",
            "redis_type": "redis",
            "connection_string": args.redis_aof,
        }
    if arm == "postgres":
        host, port = args.postgres.split(":")
        return {
            "type": "postgres",
            "host": host,
            "port": int(port),
            "database": args.postgres_db,
            "db_schema": "public",
            "user": args.postgres_user,
            "password": args.postgres_password,
            # Feast defaults to sslmode=require. The benchmark's Postgres is a
            # loopback container with no TLS, and every other arm here is also
            # plaintext on loopback, so requiring TLS on this one arm alone
            # would put an encryption cost in one row of the table and nowhere
            # else.
            "sslmode": args.postgres_sslmode,
        }
    if arm in ("ts_async", "ts_sync"):
        host, port = args.temporalstore.split(":")
        return {
            "type": "feast_temporalstore_command.TemporalStoreCommandOnlineStore",
            "host": host,
            "port": int(port),
            "shard_id": args.shard_id,
            # The dedup read is for out-of-order materialization. The fixture
            # arrives in order, so omit it from the measured write path.
            "skip_dedup": True,
            # The harness knows every entity key and deletes those keys
            # directly, so tracking would add work without helping cleanup.
            "timeout_seconds": 120.0,
        }
    if arm == "ts_proxy":
        host, port = args.temporalstore_proxy.split(":")
        return {
            "type": "feast_temporalstore_command.TemporalStoreCommandOnlineStore",
            "host": host,
            "port": int(port),
            "shard_id": args.shard_id,
            "skip_dedup": True,
        }
    raise SystemExit(f"unknown arm: {arm}")


ARM_METADATA = {
    "sqlite": ("SQLite", "file on the run directory, default rollback journal", "in-process"),
    "sqlite_control": ("SQLite (control re-run)", "file on the run directory, default rollback journal", "in-process"),
    "redis": ("Redis (Valkey 8)", "no persistence: --save '' --appendonly no", "RESP, pipelined"),
    # The durability string is filled in at run time from the server's OWN
    # `CONFIG GET appendfsync`, not from what we intended to pass -- a flag that
    # silently failed to apply would otherwise be reported as a durability
    # promise the server never made.
    "redis_aof": ("Redis (Valkey 8) durable", "appendonly yes, appendfsync <probed>", "RESP, pipelined"),
    "postgres": ("PostgreSQL 16", "default synchronous_commit=on, PGDATA on tmpfs", "libpq"),
    "ts_async": (
        "TemporalStore",
        "async commit (async_storage=true)",
        "HTTP shared-field hash write/read MessagePack routes, "
        "binary command fallback",
    ),
    "ts_sync": (
        "TemporalStore",
        "durable commit (async_storage=false)",
        "HTTP shared-field hash write/read MessagePack routes, "
        "binary command fallback",
    ),
    "ts_proxy": ("TemporalStore via proxy", "async commit (async_storage=true)", "HTTP /batch_execute through the proxy hop"),
}


# ---------------------------------------------------------------------------
# the run
# ---------------------------------------------------------------------------


def build_values(payload: str, index: int):
    from feast.protos.feast.types.Value_pb2 import Value as ValueProto

    spec = PAYLOADS[payload]
    values = {}
    for i in range(spec["ints"]):
        values[f"f{i}"] = ValueProto(int64_val=(index * 31 + i) % 1_000_003)
    if spec["blob_bytes"]:
        # Deterministic, incompressible-ish, and the same for a given index in
        # every arm, so the stored bytes match.
        blob = bytes(((index + j) * 2654435761) % 256 for j in range(spec["blob_bytes"]))
        values["blob"] = ValueProto(bytes_val=blob)
    if spec["sequence_len"]:
        sequence = ValueProto()
        sequence.int64_list_val.val.extend(
            100_000 + (index * 131 + position) % 500_000
            for position in range(spec["sequence_len"])
        )
        values["sequence"] = sequence
    return values


def feature_schema(payload: str):
    from feast import Field
    from feast.types import Array, Bytes, Int64

    spec = PAYLOADS[payload]
    schema = [Field(name=f"f{i}", dtype=Int64) for i in range(spec["ints"])]
    if spec["blob_bytes"]:
        schema.append(Field(name="blob", dtype=Bytes))
    if spec["sequence_len"]:
        schema.append(Field(name="sequence", dtype=Array(Int64)))
    return schema


def write_source_parquet(path: str, payload: str) -> None:
    """A two-row source file, only so Feast's apply-time inference has a schema."""
    import pandas as pd

    spec = PAYLOADS[payload]
    frame = {
        "eid": [0, 1],
        "ts": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
    }
    for i in range(spec["ints"]):
        frame[f"f{i}"] = [0, 1]
    if spec["blob_bytes"]:
        frame["blob"] = [b"\x00", b"\x01"]
    if spec["sequence_len"]:
        frame["sequence"] = [[0, 1], [1, 2]]
    pd.DataFrame(frame).to_parquet(path, index=False)


def temporalstore_record_count(args: argparse.Namespace) -> Optional[int]:
    """Total records the shard reports holding, or ``None`` if it cannot be read.

    Used to check that a teardown actually removed rows, and to stamp the store
    size a latency figure was taken against: a read is answered out of a shard
    of some size, and a sweep that grows the shard as it goes is not varying one
    dimension.
    """
    import http.client

    host, port = args.temporalstore.split(":")
    try:
        conn = http.client.HTTPConnection(host, int(port), timeout=10)
        conn.request("GET", "/server/info")
        shards = json.loads(conn.getresponse().read())
        conn.close()
    except Exception:
        return None
    for shard in shards if isinstance(shards, list) else []:
        if shard.get("shard_id") == args.shard_id:
            return shard.get("total_records")
    return None


def set_temporalstore_durability(args: argparse.Namespace, async_storage: bool) -> None:
    """Flip the shard's commit mode, and read it back to prove it took."""
    import http.client

    host, port = args.temporalstore.split(":")
    conn = http.client.HTTPConnection(host, int(port), timeout=10)
    # `Config` has no serde default on `version` or `feature_max_size`, so a
    # partial document is refused outright. Read the live config, change the one
    # field, and send the whole thing back.
    conn.request("GET", f"/get_config/{args.shard_id}")
    current = json.loads(conn.getresponse().read()).get("config") or {}
    if not current:
        raise SystemExit("get_config returned no config; is the shard loaded?")
    current["async_storage"] = async_storage
    current["version"] = int(current.get("version", 1)) + 1
    body = json.dumps({"shard_id": args.shard_id, "config": current})
    conn.request("POST", "/set_config", body, {"Content-Type": "application/json"})
    response = conn.getresponse()
    raw = response.read()
    if response.status != 200:
        raise SystemExit(f"set_config failed: HTTP {response.status}: {raw[:300]!r}")
    status = json.loads(raw) if raw else {}
    if isinstance(status, dict) and status.get("ok") is False:
        raise SystemExit(f"set_config refused: {raw[:300]!r}")
    conn.request("GET", f"/get_config/{args.shard_id}")
    readback = json.loads(conn.getresponse().read())
    observed = (readback.get("config") or {}).get("async_storage")
    if observed != async_storage:
        # A success code is not evidence the setting landed.
        raise SystemExit(
            f"set_config reported ok but async_storage reads back {observed!r}, "
            f"wanted {async_storage!r}"
        )
    print(f"    durability set and verified: async_storage={observed}")
    conn.close()


def run_arm(
    arm: str,
    args: argparse.Namespace,
    payload: str,
    expected_payload_bytes: Optional[int],
) -> ArmResult:
    from feast import Entity, FeatureStore, FeatureView, FileSource, RepoConfig
    from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
    from feast.protos.feast.types.Value_pb2 import Value as ValueProto
    from feast.repo_config import RepoConfig as _RepoConfig  # noqa: F401

    label, durability, transport = ARM_METADATA[arm]
    if arm == "redis_aof":
        # Probed, not assumed. `appendonly` and `appendfsync` are both read back,
        # and a server that is NOT in fact appending refuses the run rather than
        # being reported as durable -- the whole point of this arm is the promise.
        import redis as _redis_probe

        _host, _, _port = args.redis_aof.rpartition(":")
        _client = _redis_probe.Redis(host=_host, port=int(_port))
        _appendonly = (_client.config_get("appendonly") or {}).get("appendonly", "?")
        _appendfsync = (_client.config_get("appendfsync") or {}).get("appendfsync", "?")
        if isinstance(_appendonly, bytes):
            _appendonly = _appendonly.decode()
        if isinstance(_appendfsync, bytes):
            _appendfsync = _appendfsync.decode()
        if _appendonly != "yes":
            raise SystemExit(
                f"the redis_aof arm points at {args.redis_aof}, which reports "
                f"appendonly={_appendonly!r}. That server is not durable, so this "
                f"arm would measure the non-durable one twice."
            )
        durability = f"appendonly {_appendonly}, appendfsync {_appendfsync}"
    print(f"\n=== arm {arm} ({label}) payload={payload} ===")

    repo_dir = os.path.join(args.run_dir, f"{arm}_{payload}")
    shutil.rmtree(repo_dir, ignore_errors=True)
    os.makedirs(repo_dir, exist_ok=True)

    if arm == "ts_async":
        set_temporalstore_durability(args, True)
    elif arm == "ts_sync":
        set_temporalstore_durability(args, False)

    config = RepoConfig(
        project=f"bench_{arm}_{payload}",
        provider="local",
        registry=os.path.join(repo_dir, "registry.db"),
        entity_key_serialization_version=3,
        online_store=arm_online_store_block(arm, args, repo_dir),
    )
    # Feast infers the view's entity columns from the source's schema at apply
    # time, so the source has to be a real file even though no arm reads from
    # it: every arm is driven through `online_write_batch` directly, which is
    # the same call materialization makes.
    source_path = os.path.join(repo_dir, "source.parquet")
    write_source_parquet(source_path, payload)
    source = FileSource(name="src", path=source_path, timestamp_field="ts")
    entity = Entity(name="eid", join_keys=["eid"])
    view = FeatureView(
        name="bench_view", entities=[entity], schema=feature_schema(payload), source=source
    )

    store = FeatureStore(config=config)
    store.apply([entity, view])
    provider = store._get_provider()

    # -- write ---------------------------------------------------------------
    event_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    rows: List[Tuple[Any, Dict[str, Any], datetime, Optional[datetime]]] = []
    for index in range(args.entities):
        key = EntityKeyProto(
            join_keys=["eid"], entity_values=[ValueProto(int64_val=index)]
        )
        rows.append((key, build_values(payload, index), event_time, None))

    payload_bytes = sum(
        len(value.SerializeToString()) for value in rows[0][1].values()
    )
    if expected_payload_bytes is not None and payload_bytes != expected_payload_bytes:
        raise SystemExit(
            f"payload mismatch: arm {arm} would store {payload_bytes} B/row, "
            f"earlier arms stored {expected_payload_bytes} B/row -- a latency "
            f"comparison across different payloads is meaningless"
        )

    write_start = time.perf_counter()
    for start in range(0, len(rows), args.write_batch):
        provider.online_write_batch(
            config, view, rows[start : start + args.write_batch], None
        )
    write_seconds = time.perf_counter() - write_start
    print(
        f"    wrote {args.entities} rows, {payload_bytes} B payload/row, "
        f"in {write_seconds:.2f}s ({args.entities / write_seconds:,.0f} rows/s)"
    )

    # -- ASSERT THE DENOMINATOR before any timing is reported ---------------
    verify_indices = list(range(0, args.entities, max(1, args.entities // 256)))
    verify_keys = [
        EntityKeyProto(join_keys=["eid"], entity_values=[ValueProto(int64_val=i)])
        for i in verify_indices
    ]
    feature_names = [f.name for f in feature_schema(payload)]

    def read(keys):
        # A FRESH LIST EVERY CALL, and this is not defensive style -- it is a
        # correctness requirement imposed by one of the arms. Feast 0.67.0's
        # Redis online store calls `_generate_hset_keys_for_features`, which
        # does `requested_features.append(ts_key)` on the list the CALLER
        # passed. Reusing one list therefore grows the requested field set by
        # one entry per read: the Redis arm would be answering a longer and
        # longer HMGET while every other arm answered a fixed one, and the
        # measurement would quietly become a comparison of different work.
        # It surfaced here as a FeatureNameCollisionError thousands of reads
        # later, which is the lucky outcome; the unlucky one is a slow arm and
        # no error at all.
        return provider.online_read(config, view, keys, list(feature_names))

    verified = read(verify_keys)
    if len(verified) != len(verify_keys):
        raise SystemExit(
            f"{arm}: read {len(verified)} rows for {len(verify_keys)} keys"
        )
    served = 0
    for index, (event_ts, values) in zip(verify_indices, verified):
        if values is None:
            raise SystemExit(
                f"{arm}: entity {index} served NOTHING after a write that "
                f"reported success -- refusing to report latency for a store "
                f"that is not holding the data"
            )
        expected = build_values(payload, index)
        for name, want in expected.items():
            got = values.get(name)
            if got is None or got.SerializeToString() != want.SerializeToString():
                raise SystemExit(
                    f"{arm}: entity {index} feature {name} read back wrong: "
                    f"{got!r} != {want!r}"
                )
        served += 1
    if served == 0:
        raise SystemExit(f"{arm}: zero rows verified; nothing to measure")
    print(f"    verified {served} sampled rows match what was written")

    # -- cold pass -----------------------------------------------------------
    cold = Phase("cold", load_before=load_average())
    cold_indices = list(range(min(args.entities, args.cold_reads)))
    for index in cold_indices:
        key = [
            EntityKeyProto(join_keys=["eid"], entity_values=[ValueProto(int64_val=index)])
        ]
        begin = time.perf_counter()
        result = read(key)
        cold.latencies_us.append((time.perf_counter() - begin) * 1_000_000)
        cold.rows_served += sum(1 for _, values in result if values is not None)
    cold.load_after = load_average()
    if cold.rows_served != len(cold_indices):
        raise SystemExit(
            f"{arm}: cold pass served {cold.rows_served} of {len(cold_indices)} rows"
        )

    # -- warm-up, then warm passes ------------------------------------------
    hot = list(range(min(args.entities, args.hot)))
    for index in hot:
        read(
            [EntityKeyProto(join_keys=["eid"], entity_values=[ValueProto(int64_val=index)])]
        )

    def warm_pass(batch_n: int, name: str) -> Phase:
        phase = Phase(name, load_before=load_average())
        position = 0
        for _ in range(args.reads):
            batch = []
            for _ in range(batch_n):
                batch.append(
                    EntityKeyProto(
                        join_keys=["eid"],
                        entity_values=[ValueProto(int64_val=hot[position % len(hot)])],
                    )
                )
                position += 1
            begin = time.perf_counter()
            result = read(batch)
            phase.latencies_us.append((time.perf_counter() - begin) * 1_000_000)
            phase.rows_served += sum(1 for _, values in result if values is not None)
        phase.load_after = load_average()
        expected_rows = args.reads * batch_n
        if phase.rows_served != expected_rows:
            raise SystemExit(
                f"{arm}: warm pass {name} served {phase.rows_served} of "
                f"{expected_rows} rows"
            )
        return phase

    warm_b1 = warm_pass(1, "warm_batch1")
    warm_bn = warm_pass(args.batch, f"warm_batch{args.batch}")

    # The whole serving call, not just the store's read. This carries Feast's
    # own request handling -- entity-row marshalling, feature-ref grouping,
    # proto to python conversion -- which is the same work in every arm, so it
    # raises every row by a similar constant. It is reported beside the store
    # read rather than instead of it: the store read is the sharper instrument,
    # this one is what a serving endpoint actually pays.
    refs_warm = [f"bench_view:{f.name}" for f in feature_schema(payload)]
    # Feast caches the request context -- registry lookup, feature-ref
    # grouping -- on the FIRST get_online_features call for a set of refs. That
    # first call cost 515 ms in an early run and landed in p99 as if it were a
    # store cost. Warm it before the clock starts; the warm column says warm.
    for _ in range(3):
        store.get_online_features(
            features=refs_warm, entity_rows=[{"eid": hot[0]}]
        ).to_dict()

    serving = Phase("serving_get_online_features", load_before=load_average())
    refs = [f"bench_view:{name}" for name in [f.name for f in feature_schema(payload)]]
    position = 0
    for _ in range(args.reads):
        entity_rows = []
        for _ in range(args.batch):
            entity_rows.append({"eid": hot[position % len(hot)]})
            position += 1
        begin = time.perf_counter()
        answer = store.get_online_features(features=refs, entity_rows=entity_rows)
        rendered = answer.to_dict()
        serving.latencies_us.append((time.perf_counter() - begin) * 1_000_000)
        served_now = sum(1 for v in rendered[feature_names[0]] if v is not None)
        serving.rows_served += served_now
    serving.load_after = load_average()
    if serving.rows_served != args.reads * args.batch:
        raise SystemExit(
            f"{arm}: serving pass served {serving.rows_served} of "
            f"{args.reads * args.batch} rows"
        )

    concurrent = None
    if args.concurrency > 1:
        concurrent = Phase("concurrent_online_read", load_before=load_average())
        worker_state = threading.local()

        def read_concurrently(call_index: int) -> Tuple[float, int]:
            if not hasattr(worker_state, "provider"):
                worker_state.provider = FeatureStore(config=config)._get_provider()
            keys = [
                EntityKeyProto(
                    join_keys=["eid"],
                    entity_values=[ValueProto(int64_val=hot[(call_index * args.batch + offset) % len(hot)])],
                )
                for offset in range(args.batch)
            ]
            begin = time.perf_counter()
            answer = worker_state.provider.online_read(config, view, keys, list(feature_names))
            elapsed_us = (time.perf_counter() - begin) * 1_000_000
            if len(answer) != len(keys):
                raise RuntimeError(f"{arm}: concurrent read returned {len(answer)} of {len(keys)} rows")
            served_now = sum(1 for _, values in answer if values is not None)
            return elapsed_us, served_now

        wall_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            next_call = iter(range(args.reads))
            pending = set()
            for call_index in next_call:
                pending.add(pool.submit(read_concurrently, call_index))
                if len(pending) == args.concurrency * 2:
                    break
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    elapsed_us, served_now = future.result()
                    concurrent.latencies_us.append(elapsed_us)
                    concurrent.rows_served += served_now
                    call_index = next(next_call, None)
                    if call_index is not None:
                        pending.add(pool.submit(read_concurrently, call_index))
        concurrent.wall_seconds = time.perf_counter() - wall_start
        concurrent.load_after = load_average()
        if concurrent.rows_served != args.reads * args.batch:
            raise SystemExit(
                f"{arm}: concurrent pass served {concurrent.rows_served} of "
                f"{args.reads * args.batch} rows"
            )
        print(
            f"    concurrent workers={args.concurrency} rows/s="
            f"{concurrent.throughput_rows_per_s:.0f} "
            f"p50={concurrent.p50:.0f}us p99={concurrent.p99:.0f}us"
        )

    # The fixture knows every key it wrote. Delete those keys directly on the
    # command-surface arms, then verify the server's own record count fell.
    records_before = temporalstore_record_count(args) if arm.startswith("ts_") else None
    try:
        if arm.startswith("ts_"):
            from feast_temporalstore_command import TemporalStoreCommandOnlineStore

            adapter = TemporalStoreCommandOnlineStore()
            client = adapter._get_client(config)
            try:
                client.batch_execute(
                    [
                        {
                            "kind": "common_delete",
                            "key": adapter._entity_hash_key(config, entity_key),
                        }
                        for entity_key, _, _, _ in rows
                    ]
                )
            finally:
                client.close()
        else:
            provider.teardown_infra(config.project, [view], [entity])
        teardown_error: Optional[str] = None
    except Exception as exc:  # noqa: BLE001 -- reported, not swallowed
        teardown_error = f"{type(exc).__name__}: {exc}"
    if teardown_error:
        print(f"    teardown FAILED: {teardown_error}")
    elif records_before is not None:
        records_after = temporalstore_record_count(args)
        if records_after is None:
            print("    teardown reported ok; could not read the record count back")
        else:
            removed = records_before - records_after
            print(
                f"    teardown removed {removed:,} records "
                f"({records_before:,} -> {records_after:,}); "
                f"wrote {args.entities:,} entities"
            )
            if removed != args.entities:
                raise SystemExit(
                    f"{arm}: expected to remove {args.entities:,} entity records, "
                    f"removed {removed:,} ({records_before:,} -> {records_after:,})"
                )

    return ArmResult(
        arm=arm,
        store_label=label,
        durability=durability,
        transport=transport,
        payload_bytes_per_row=payload_bytes,
        entities=args.entities,
        cold=cold,
        warm_b1=warm_b1,
        warm_bn=warm_bn,
        serving=serving,
        concurrent=concurrent,
        write_seconds=write_seconds,
        batch_n=args.batch,
    )


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------


def render(
    results: List[ArmResult],
    args: argparse.Namespace,
    provisional: bool,
    peak_io_pressure: float,
) -> str:
    lines: List[str] = []
    by_payload: Dict[str, List[ArmResult]] = {}
    for result in results:
        by_payload.setdefault(result.payload_bytes_per_row, []).append(result)

    for payload_bytes, group in sorted(by_payload.items()):
        lines.append("")
        lines.append(
            f"### Payload {payload_bytes} B/row, {group[0].entities} entities, "
            f"hot set {args.hot}, {args.reads} reads per cell"
        )
        lines.append("")
        lines.append(
            "| store | durability | transport | write rows/s | cold p50 | cold p99 | "
            f"warm p50 (b=1) | warm p99 (b=1) | warm p50 (b={args.batch}) | "
            f"warm p99 (b={args.batch}) | warm rows/s (b={args.batch}) | "
            f"serving p50 (b={args.batch}) | serving p99 (b={args.batch}) | load |"
        )
        lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for r in group:
            peak = max(
                r.cold.load_after, r.warm_b1.load_after, r.warm_bn.load_after, r.serving.load_after
            )
            lines.append(
                f"| {r.store_label} | {r.durability} | {r.transport} | "
                f"{r.entities / r.write_seconds:,.0f} | "
                f"{r.cold.p50:,.0f} us | {r.cold.p99:,.0f} us | "
                f"{r.warm_b1.p50:,.0f} us | {r.warm_b1.p99:,.0f} us | "
                f"{r.warm_bn.p50:,.0f} us | {r.warm_bn.p99:,.0f} us | "
                f"{r.warm_bn.throughput_rows_per_s:,.0f} | "
                f"{r.serving.p50:,.0f} us | {r.serving.p99:,.0f} us | {peak:.0f} |"
            )
        if args.concurrency > 1:
            lines.extend([
                "",
                "| store | workers | calls | rows | wall seconds | rows/s | p50 | p99 |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ])
            for r in group:
                phase = r.concurrent
                if phase is not None:
                    lines.append(
                        f"| {r.store_label} | {args.concurrency} | {phase.calls} | "
                        f"{phase.rows_served} | {phase.wall_seconds:.3f} | "
                        f"{phase.throughput_rows_per_s:,.0f} | "
                        f"{phase.p50:,.0f} us | {phase.p99:,.0f} us |"
                    )
    if provisional:
        lines.append("")
        lines.append(
            f"**PROVISIONAL** -- peak I/O pressure {peak_io_pressure:.1f}% exceeded "
            f"--max-io-pressure ({args.max_io_pressure}%). These figures measure "
            f"the host, not the stores. (Peak loadavg was {peak_load:.0f}, which is "
            f"not a load signal on this host and did not decide this.)"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--arms",
        default="sqlite,redis,postgres,ts_async,ts_sync,sqlite_control",
        help="comma-separated arms, in run order",
    )
    parser.add_argument("--payloads", default="small,large")
    parser.add_argument("--entities", type=int, default=10000)
    parser.add_argument("--hot", type=int, default=512)
    parser.add_argument("--reads", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--cold-reads", type=int, default=2000)
    parser.add_argument("--batch", type=int, default=10)
    parser.add_argument("--write-batch", type=int, default=500)
    parser.add_argument("--redis", default="127.0.0.1:6380")
    parser.add_argument("--redis-aof", default="127.0.0.1:6381")
    parser.add_argument("--postgres", default="127.0.0.1:55432")
    parser.add_argument("--postgres-db", default="feast")
    parser.add_argument("--postgres-user", default="feast")
    parser.add_argument("--postgres-password", default="feast")
    parser.add_argument("--postgres-sslmode", default="disable")
    parser.add_argument("--temporalstore", default="127.0.0.1:17402")
    parser.add_argument("--temporalstore-proxy", default="127.0.0.1:17000")
    parser.add_argument("--shard-id", type=int, default=1)
    parser.add_argument("--run-dir", default="/dev/shm/feast_bench")
    parser.add_argument("--max-load", type=float, default=16.0)
    # The gate that actually decides. --max-load is kept so an existing caller
    # does not break, but it no longer votes: see load_average's docstring.
    parser.add_argument("--max-io-pressure", type=float, default=25.0)
    parser.add_argument("--control-tolerance", type=float, default=0.25)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    if args.concurrency < 1 or args.reads < 1 or args.batch < 1 or args.hot < 1:
        parser.error("concurrency, reads, batch and hot must be positive")

    os.makedirs(args.run_dir, exist_ok=True)
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    payloads = [p.strip() for p in args.payloads.split(",") if p.strip()]

    print(
        f"io pressure at start: {io_pressure():.2f}%  "
        f"(--max-io-pressure {args.max_io_pressure}%); "
        f"loadavg {load_average():.0f} (reported, not gated)"
    )
    results: List[ArmResult] = []
    peak_load = load_average()
    peak_io_pressure = io_pressure()

    for payload in payloads:
        expected_payload_bytes: Optional[int] = None
        for arm in arms:
            result = run_arm(arm, args, payload, expected_payload_bytes)
            expected_payload_bytes = result.payload_bytes_per_row
            results.append(result)
            peak_io_pressure = max(peak_io_pressure, io_pressure())
            peak_load = max(
                peak_load,
                result.cold.load_after,
                result.warm_b1.load_after,
                result.warm_bn.load_after,
                result.serving.load_after,
            )

    # -- the control -------------------------------------------------------
    # ONE CHECK PER PAYLOAD GROUP, AND NO `break`. The first version of this
    # block filtered `results` by ARM only and then broke out of the loop
    # unconditionally, so with `--payloads small,large` it compared the small
    # group's two SQLite runs and never looked at the large group at all: the
    # second payload's control could drift arbitrarily and the run still
    # reported itself usable. A guard that filters its own population down to
    # one group is disarmed by adding a second. The results carry the payload
    # width, so group on that -- the same key `render` already groups by.
    control_void: Optional[str] = None
    drift_reasons: List[str] = []
    for payload_bytes in sorted({r.payload_bytes_per_row for r in results}):
        group = [r for r in results if r.payload_bytes_per_row == payload_bytes]
        first = next((r for r in group if r.arm == "sqlite"), None)
        last = next((r for r in group if r.arm == "sqlite_control"), None)
        if not (first and last):
            # Said out loud: without both control runs there IS no control, and
            # a silent pass here would read as one.
            print(
                f"\ncontrol: payload {payload_bytes} B/row has no sqlite/"
                f"sqlite_control pair -- this group has NO control"
            )
            continue
        if first.warm_b1.p50 <= 0:
            continue
        drift = abs(last.warm_b1.p50 - first.warm_b1.p50) / first.warm_b1.p50
        print(
            f"\ncontrol: payload {payload_bytes} B/row SQLite warm p50 "
            f"first={first.warm_b1.p50:,.0f} us last={last.warm_b1.p50:,.0f} us "
            f"drift={drift:.1%}"
        )
        if drift > args.control_tolerance:
            drift_reasons.append(f"{payload_bytes} B/row drifted {drift:.1%}")
    if drift_reasons:
        control_void = (
            f"the control arm drifted beyond the {args.control_tolerance:.0%} "
            f"tolerance between its first and last run ("
            + "; ".join(drift_reasons)
            + "); the host or the harness was the variable, so no cross-store "
            "comparison in this run is usable"
        )

    # Decided on I/O pressure, not on loadavg, for the reason in load_average's
    # docstring. `peak_load` is still carried and printed.
    provisional = peak_io_pressure > args.max_io_pressure
    table = render(results, args, provisional, peak_io_pressure)
    print(table)
    if control_void:
        print(f"\n**VOID** -- {control_void}")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(table + "\n")
        csv_path = os.path.splitext(args.out)[0] + ".csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "arm", "store", "durability", "transport", "payload_bytes_per_row",
                    "entities", "phase", "calls", "rows_served", "p50_us", "p99_us",
                    "rows_per_s", "load_before", "load_after", "workers", "wall_seconds",
                ]
            )
            for r in results:
                writer.writerow(
                    [
                        r.arm, r.store_label, r.durability, r.transport,
                        r.payload_bytes_per_row, r.entities, "write_bulk",
                        (r.entities + args.write_batch - 1) // args.write_batch,
                        r.entities, "", "", f"{r.entities / r.write_seconds:.1f}",
                        "", "", 1, f"{r.write_seconds:.6f}",
                    ]
                )
                for phase in (r.cold, r.warm_b1, r.warm_bn, r.serving, r.concurrent):
                    if phase is None:
                        continue
                    writer.writerow(
                        [
                            r.arm, r.store_label, r.durability, r.transport,
                            r.payload_bytes_per_row, r.entities, phase.name,
                            phase.calls, phase.rows_served,
                            f"{phase.p50:.1f}", f"{phase.p99:.1f}",
                            f"{phase.throughput_rows_per_s:.1f}",
                            f"{phase.load_before:.2f}", f"{phase.load_after:.2f}",
                            args.concurrency if phase is r.concurrent else 1,
                            f"{phase.wall_seconds:.6f}" if phase is r.concurrent else "",
                        ]
                    )
        print(f"\nwrote {args.out} and {csv_path}")

    if provisional or control_void:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

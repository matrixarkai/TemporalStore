# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Measure Feast integer-history writes and native windowed aggregate reads."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from typing import List
from uuid import uuid4

from feast import Entity, FeatureStore, FeatureView, Field, FileSource, RepoConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value
from feast.types import Int64
from feast.value_type import ValueType

from feast_temporalstore_command import TemporalStoreCommandOnlineStore


def percentile(values: List[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(fraction * (len(ordered) - 1)))]


def record_count(host: str, port: int, shard_id: int) -> int:
    import http.client

    conn = http.client.HTTPConnection(host, port, timeout=10)
    try:
        conn.request("GET", "/server/info")
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f"server info returned HTTP {response.status}")
        for shard in json.load(response):
            if shard.get("shard_id") == shard_id:
                return int(shard["total_records"])
        raise RuntimeError(f"shard {shard_id} is not loaded")
    finally:
        conn.close()


def async_storage_mode(host: str, port: int, shard_id: int) -> bool:
    import http.client

    conn = http.client.HTTPConnection(host, port, timeout=10)
    try:
        conn.request("GET", f"/get_config/{shard_id}")
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f"shard config returned HTTP {response.status}")
        config = json.load(response).get("config") or {}
        if "async_storage" not in config:
            raise RuntimeError("shard config did not report async_storage")
        return bool(config["async_storage"])
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=17102)
    parser.add_argument("--shard-id", type=int, default=1)
    parser.add_argument("--entities", type=int, default=256)
    parser.add_argument("--events", type=int, default=64)
    parser.add_argument("--write-batch", type=int, default=256)
    parser.add_argument("--reads", type=int, default=1000)
    parser.add_argument("--aggregate-batch", type=int, default=64)
    parser.add_argument("--batch-repeats", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--control-tolerance", type=float, default=0.25)
    parser.add_argument("--run-dir", default="/dev/shm/feast_bench")
    parser.add_argument("--out")
    args = parser.parse_args()
    if min(
        args.entities,
        args.events,
        args.write_batch,
        args.reads,
        args.aggregate_batch,
        args.batch_repeats,
        args.concurrency,
    ) < 1:
        parser.error(
            "entities, events, write-batch, reads, aggregate-batch, batch-repeats, "
            "and concurrency must be positive"
        )
    if not 0 <= args.control_tolerance < 1:
        parser.error("control-tolerance must be in [0, 1)")
    if args.timeout_seconds <= 0:
        parser.error("timeout-seconds must be positive")

    import pandas as pd

    os.makedirs(args.run_dir, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="aggregate_", dir=args.run_dir) as run_dir:
        source_path = os.path.join(run_dir, "source.parquet")
        now = datetime.now(timezone.utc).replace(microsecond=0)
        pd.DataFrame({"eid": [0, 1], "ts": [now, now], "amount": [0, 1]}).to_parquet(
            source_path, index=False
        )
        config = RepoConfig(
            project=f"feast_aggregate_{uuid4().hex[:12]}",
            provider="local",
            registry=os.path.join(run_dir, "registry.db"),
            entity_key_serialization_version=3,
            online_store={
                "type": "feast_temporalstore_command.TemporalStoreCommandOnlineStore",
                "host": args.host,
                "port": args.port,
                "shard_id": args.shard_id,
                "skip_dedup": True,
                "write_feature_history": True,
                "timeout_seconds": args.timeout_seconds,
            },
        )
        entity = Entity(
            name="eid", join_keys=["eid"], value_type=ValueType.INT64
        )
        view = FeatureView(
            name="aggregate_view",
            entities=[entity],
            schema=[Field(name="amount", dtype=Int64)],
            source=FileSource(name="source", path=source_path, timestamp_field="ts"),
        )
        FeatureStore(config=config).apply([entity, view])
        adapter = TemporalStoreCommandOnlineStore()
        client = adapter._get_client(config)
        before = record_count(args.host, args.port, args.shard_id)
        async_storage = async_storage_mode(args.host, args.port, args.shard_id)
        print(f"project={config.project} starting_records={before}", flush=True)
        base = now - timedelta(days=1)
        base_ms = int(base.timestamp() * 1000)
        keys = [EntityKey(join_keys=["eid"], entity_values=[Value(int64_val=i)])
                for i in range(args.entities)]
        view_name = adapter._view_name(view, config)
        try:
            write_seconds = 0.0
            pending = []
            for index, key in enumerate(keys):
                for event in range(args.events):
                    timestamp = base + timedelta(seconds=event)
                    pending.append(
                        (key, {"amount": Value(int64_val=index + event)}, timestamp, None)
                    )
                    if len(pending) < args.write_batch:
                        continue
                    started = time.perf_counter()
                    adapter.online_write_batch(config, view, pending, None)
                    write_seconds += time.perf_counter() - started
                    pending = []
            if pending:
                started = time.perf_counter()
                adapter.online_write_batch(config, view, pending, None)
                write_seconds += time.perf_counter() - started

            last = adapter.online_read(config, view, keys, ["amount"])
            if len(last) != args.entities:
                raise RuntimeError("latest-value read returned the wrong row count")
            for index, (_, values) in enumerate(last):
                if values is None or values["amount"].int64_val != index + args.events - 1:
                    raise RuntimeError(f"latest value differs for entity {index}")
                expected_sum = args.events * index + args.events * (args.events - 1) // 2
                observed_sum = adapter.query_feature_aggregate(
                    config, view, keys[index], "amount", base_ms,
                    base_ms + (args.events - 1) * 1000, "sum",
                )
                if observed_sum != expected_sum:
                    raise RuntimeError(f"full-window sum differs for entity {index}")
                observed_count = adapter.query_feature_aggregate(
                    config, view, keys[index], "amount", base_ms,
                    base_ms + (args.events - 1) * 1000, "count",
                )
                if observed_count != args.events:
                    raise RuntimeError(f"history count differs for entity {index}")

            tail_start = max(0, args.events - 8)
            tail_sum = sum(range(tail_start, args.events))
            for index in range(min(args.entities, 256)):
                observed = adapter.query_feature_aggregate(
                    config, view, keys[index], "amount",
                    base_ms + tail_start * 1000,
                    base_ms + (args.events - 1) * 1000, "sum",
                )
                if observed != (args.events - tail_start) * index + tail_sum:
                    raise RuntimeError(f"tail-window sum differs for entity {index}")

            latencies_us: List[float] = []
            def read_one(call_index: int) -> float:
                index = call_index % args.entities
                begin = time.perf_counter()
                observed = adapter.query_feature_aggregate(
                    config, view, keys[index], "amount", base_ms,
                    base_ms + (args.events - 1) * 1000, "sum",
                )
                elapsed = (time.perf_counter() - begin) * 1_000_000
                expected = args.events * index + args.events * (args.events - 1) // 2
                if observed != expected:
                    raise RuntimeError(f"measured aggregate differs for entity {index}")
                return elapsed

            def control_sample() -> float:
                return percentile(
                    [read_one(index) for index in range(min(args.entities, 128))], 0.50
                )

            # The first pass can materialize log-backed blocks into the serving
            # cache. It is initialization, not a control observation, so warm
            # once before comparing the two measured control samples.
            control_sample()
            control_before = control_sample()
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                next_call = iter(range(args.reads))
                pending = set()
                started = time.perf_counter()
                for call_index in next_call:
                    pending.add(pool.submit(read_one, call_index))
                    if len(pending) >= 2 * args.concurrency:
                        break
                while pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        latencies_us.append(future.result())
                        call_index = next(next_call, None)
                        if call_index is not None:
                            pending.add(pool.submit(read_one, call_index))
                read_seconds = time.perf_counter() - started

            batch_cases = []
            for start in range(0, args.reads, args.aggregate_batch):
                call_indices = list(
                    range(start, min(start + args.aggregate_batch, args.reads))
                )
                queries = []
                expected = []
                for call_index in call_indices:
                    index = call_index % args.entities
                    queries.append(
                        (
                            keys[index],
                            "amount",
                            base_ms,
                            base_ms + (args.events - 1) * 1000,
                            "sum",
                            None,
                        )
                    )
                    expected.append(
                        args.events * index
                        + args.events * (args.events - 1) // 2
                    )
                batch_cases.append((start, call_indices, queries, expected))

            batch_repeat_reads_per_second = []
            batch_repeat_p50_us = []
            batch_repeat_p99_us = []
            for _repeat in range(args.batch_repeats):
                batch_latencies_us: List[float] = []
                batched_reads = 0
                batch_started = time.perf_counter()
                for start, call_indices, queries, expected in batch_cases:
                    begin = time.perf_counter()
                    observed = adapter.query_feature_aggregates(config, view, queries)
                    batch_latencies_us.append(
                        (time.perf_counter() - begin) * 1_000_000
                    )
                    if observed != expected:
                        raise RuntimeError(
                            f"batched aggregate differs for calls {start}.."
                            f"{start + len(call_indices) - 1}"
                        )
                    batched_reads += len(call_indices)
                batch_read_seconds = time.perf_counter() - batch_started
                batch_repeat_reads_per_second.append(
                    batched_reads / batch_read_seconds
                )
                batch_repeat_p50_us.append(percentile(batch_latencies_us, 0.50))
                batch_repeat_p99_us.append(percentile(batch_latencies_us, 0.99))

            batched_reads_per_second = statistics.median(
                batch_repeat_reads_per_second
            )
            batch_control_drift = max(
                abs(value - batched_reads_per_second) / batched_reads_per_second
                for value in batch_repeat_reads_per_second
            )
            control_after = control_sample()
            control_drift = abs(control_after - control_before) / control_before

            report = {
                "entities": args.entities,
                "async_storage": async_storage,
                "timeout_seconds": args.timeout_seconds,
                "events_per_entity": args.events,
                "history_writes": args.entities * args.events,
                "writes_per_second": args.entities * args.events / write_seconds,
                "write_seconds": write_seconds,
                "aggregate_reads": len(latencies_us),
                "concurrency": args.concurrency,
                "aggregate_reads_per_second": len(latencies_us) / read_seconds,
                "aggregate_p50_us": percentile(latencies_us, 0.50),
                "aggregate_p99_us": percentile(latencies_us, 0.99),
                "read_seconds": read_seconds,
                "aggregate_batch_size": args.aggregate_batch,
                "aggregate_batch_repeats": args.batch_repeats,
                "batched_aggregate_reads": batched_reads,
                "batched_aggregate_reads_per_second": batched_reads_per_second,
                "batched_aggregate_repeat_reads_per_second": batch_repeat_reads_per_second,
                "aggregate_batch_call_p50_us": statistics.median(
                    batch_repeat_p50_us
                ),
                "aggregate_batch_call_p99_us": statistics.median(
                    batch_repeat_p99_us
                ),
                "batch_control_drift": batch_control_drift,
                "batch_read_seconds": batched_reads / batched_reads_per_second,
                "verified_entities": args.entities,
                "control_before_p50_us": control_before,
                "control_after_p50_us": control_after,
                "control_drift": control_drift,
                "status": "void"
                if max(control_drift, batch_control_drift) > args.control_tolerance
                else "valid",
            }
        finally:
            commands = []
            for key in keys:
                commands.extend(
                    {"kind": "common_delete", "key": item}
                    for item in (
                        adapter._entity_hash_key(config, key),
                        adapter._history_key(config, view_name, key, "amount"),
                        adapter._aggregate_key(config, view_name, key, "amount"),
                    )
                )
            try:
                client.batch_execute(commands)
            finally:
                client.close()
        after = record_count(args.host, args.port, args.shard_id)
        if after != before:
            raise RuntimeError(f"cleanup changed record count {before} -> {after}")
        report["records_after_cleanup"] = after
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as output:
                output.write(rendered + "\n")
        if report["status"] == "void":
            raise SystemExit(1)


if __name__ == "__main__":
    main()

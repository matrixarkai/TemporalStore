#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Measure exact-teardown tracking through the Feast command provider.

The node must start on empty data directories. The benchmark writes feature
rows in bounded Feast batches, reads every value back, and reports how evenly
the deterministic tracking sets absorbed the row keys.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Int64
from feast.value_type import ValueType

from feast_temporalstore_command import TemporalStoreCommandOnlineStore


def node_records(host: str, port: int, shard_id: int) -> int:
    with urllib.request.urlopen(f"http://{host}:{port}/server/info", timeout=30) as response:
        for shard in json.loads(response.read()):
            if shard.get("shard_id") == shard_id:
                return int(shard.get("total_records", 0))
    raise RuntimeError(f"shard {shard_id} is not loaded")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, round(fraction * (len(ordered) - 1)))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=17999)
    parser.add_argument("--shard-id", type=int, default=1)
    parser.add_argument("--entities", type=int, default=50_000)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--warm-calls", type=int, default=500)
    parser.add_argument("--warm-batch", type=int, default=10)
    parser.add_argument("--hot", type=int, default=512)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--warm-repeats", type=int, default=3)
    parser.add_argument("--control-tolerance", type=float, default=0.25)
    parser.add_argument(
        "--verify-teardown",
        action="store_true",
        help="run exact teardown and verify that feature fields and tracking sets are empty",
    )
    args = parser.parse_args()
    if min(
        args.entities,
        args.batch,
        args.warm_calls,
        args.warm_batch,
        args.hot,
        args.concurrency,
        args.warm_repeats,
    ) < 1:
        parser.error("entity, batch, hot-set, call, and concurrency values must be positive")
    if not 0 <= args.control_tolerance < 1:
        parser.error("--control-tolerance must be in [0, 1)")
    if node_records(args.host, args.port, args.shard_id) != 0:
        parser.error("the target shard is not empty")

    config = RepoConfig(
        project="tracking_bench",
        provider="local",
        registry="/tmp/feast-tracking-bench-registry.db",
        online_store={
            "type": "feast_temporalstore_command.TemporalStoreCommandOnlineStore",
            "host": args.host,
            "port": args.port,
            "shard_id": args.shard_id,
            "skip_dedup": True,
            "track_keys_for_teardown": True,
            "max_batch_commands": 512,
        },
        entity_key_serialization_version=3,
    )
    source = FileSource(
        name="tracking_bench_source",
        path="/tmp/feast-tracking-bench.parquet",
        timestamp_field="event_timestamp",
    )
    entity = Entity(
        name="driver", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    view = FeatureView(
        name="stats",
        entities=[entity],
        schema=[Field(name="trips", dtype=Int64)],
        source=source,
    )
    store = TemporalStoreCommandOnlineStore()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def entity_key(index: int) -> EntityKeyProto:
        return EntityKeyProto(
            join_keys=["driver_id"],
            entity_values=[ValueProto(int64_val=index)],
        )

    started = time.perf_counter()
    for start in range(0, args.entities, args.batch):
        stop = min(start + args.batch, args.entities)
        store.online_write_batch(
            config,
            view,
            [
                (
                    entity_key(index),
                    {"trips": ValueProto(int64_val=index)},
                    now,
                    None,
                )
                for index in range(start, stop)
            ],
            None,
        )
    write_seconds = time.perf_counter() - started

    client = store._get_client(config)
    tracking_keys = store._teardown_shard_keys(config, "stats")
    tracking_answers = client.batch_execute(
        [{"kind": "set_members", "key": key} for key in tracking_keys]
    )
    shard_sizes = [len(answer.get("members") or []) for answer in tracking_answers]

    missing = 0
    mismatched = 0
    read_started = time.perf_counter()
    for start in range(0, args.entities, args.batch):
        stop = min(start + args.batch, args.entities)
        answers = store.online_read(
            config,
            view,
            [entity_key(index) for index in range(start, stop)],
            ["trips"],
        )
        for index, (_timestamp, values) in enumerate(answers, start=start):
            if not values or "trips" not in values:
                missing += 1
            elif values["trips"].int64_val != index:
                mismatched += 1
    read_seconds = time.perf_counter() - read_started

    hot_indices = list(range(min(args.hot, args.entities)))
    store.online_read(
        config,
        view,
        [entity_key(index) for index in hot_indices],
        ["trips"],
    )

    def warm_read(call_index: int) -> float:
        indices = [
            hot_indices[(call_index * args.warm_batch + offset) % len(hot_indices)]
            for offset in range(args.warm_batch)
        ]
        started = time.perf_counter()
        answers = store.online_read(
            config,
            view,
            [entity_key(index) for index in indices],
            ["trips"],
        )
        elapsed_us = (time.perf_counter() - started) * 1_000_000
        if len(answers) != len(indices):
            raise RuntimeError(
                f"warm read returned {len(answers)} of {len(indices)} rows"
            )
        for index, (_timestamp, values) in zip(indices, answers):
            if not values or values["trips"].int64_val != index:
                raise RuntimeError(f"warm read returned the wrong value for entity {index}")
        return elapsed_us

    warm_repeat_rows_per_second = []
    warm_repeat_p50_us = []
    warm_repeat_p99_us = []
    for repeat in range(args.warm_repeats):
        first_call = repeat * args.warm_calls
        warm_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            warm_latencies_us = list(
                pool.map(
                    warm_read,
                    range(first_call, first_call + args.warm_calls),
                )
            )
        warm_seconds = time.perf_counter() - warm_started
        warm_repeat_rows_per_second.append(
            args.warm_calls * args.warm_batch / warm_seconds
        )
        warm_repeat_p50_us.append(percentile(warm_latencies_us, 0.50))
        warm_repeat_p99_us.append(percentile(warm_latencies_us, 0.99))

    warm_rows_per_second = statistics.median(warm_repeat_rows_per_second)
    warm_control_drift = max(
        abs(value - warm_rows_per_second) / warm_rows_per_second
        for value in warm_repeat_rows_per_second
    )
    warm_stable = warm_control_drift <= args.control_tolerance

    tracked_members = sum(shard_sizes)
    occupied = sum(size > 0 for size in shard_sizes)
    result = {
        "entities": args.entities,
        "batch": args.batch,
        "write_seconds": round(write_seconds, 4),
        "write_rows_per_second": round(args.entities / write_seconds, 1),
        "read_seconds": round(read_seconds, 4),
        "read_rows_per_second": round(args.entities / read_seconds, 1),
        "warm_calls": args.warm_calls,
        "warm_batch": args.warm_batch,
        "warm_hot_entities": len(hot_indices),
        "warm_concurrency": args.concurrency,
        "warm_repeats": args.warm_repeats,
        "warm_call_p50_us": round(statistics.median(warm_repeat_p50_us), 1),
        "warm_call_p99_us": round(statistics.median(warm_repeat_p99_us), 1),
        "warm_rows_per_second": round(warm_rows_per_second, 1),
        "warm_repeat_rows_per_second": [
            round(value, 1) for value in warm_repeat_rows_per_second
        ],
        "warm_control_drift": round(warm_control_drift, 4),
        "warm_stable": warm_stable,
        "missing_rows": missing,
        "mismatched_rows": mismatched,
        "tracked_members": tracked_members,
        "tracking_sets": len(shard_sizes),
        "occupied_tracking_sets": occupied,
        "tracking_members_max": max(shard_sizes),
        "tracking_members_mean_occupied": round(tracked_members / max(occupied, 1), 2),
        "node_records": node_records(args.host, args.port, args.shard_id),
        "load_average_1m": round(os.getloadavg()[0], 2),
    }
    if args.verify_teardown:
        teardown_started = time.perf_counter()
        store.teardown(config, [view], [entity])
        teardown_seconds = time.perf_counter() - teardown_started
        remaining_feature_rows = 0
        for start in range(0, args.entities, args.batch):
            stop = min(start + args.batch, args.entities)
            answers = store.online_read(
                config,
                view,
                [entity_key(index) for index in range(start, stop)],
                ["trips"],
            )
            remaining_feature_rows += sum(
                bool(values and "trips" in values) for _timestamp, values in answers
            )
        remaining_tracking_members = sum(
            len(answer.get("members") or [])
            for answer in client.batch_execute(
                [{"kind": "set_members", "key": key} for key in tracking_keys]
            )
        )
        result.update(
            {
                "teardown_seconds": round(teardown_seconds, 4),
                "remaining_feature_rows": remaining_feature_rows,
                "remaining_tracking_members": remaining_tracking_members,
            }
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    store.close()
    valid = (
        missing == 0
        and mismatched == 0
        and tracked_members == args.entities
        and warm_stable
    )
    if args.verify_teardown:
        valid = valid and remaining_feature_rows == 0 and remaining_tracking_members == 0
    return 0 if valid else 2


if __name__ == "__main__":
    raise SystemExit(main())

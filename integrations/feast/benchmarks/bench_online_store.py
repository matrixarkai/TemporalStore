# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
r"""Compare Feast on TemporalStore against Feast on Redis, one arm per invocation.

ONE ARM PER PROCESS, ON PURPOSE. Both arms are driven through Feast itself, so the only
difference between them is the ``online_store`` block in ``feature_store.yaml``. Running them in
one process would let the first arm's warmed client pools and page cache pay for the second.

IT REFUSES TO CALL A NUMBER A LATENCY WHEN THE BOX IS BUSY. A p99 measured while a build is
running is a measurement of the build. The load average is read before and after every phase and
printed beside every figure; above ``--max-load`` the latency figures are labelled PROVISIONAL
and the script exits non-zero, so a report cannot quietly contain them. Throughput and
bytes-per-row are less load-sensitive but are still stamped with the load they were taken at.

Usage:
    python bench_online_store.py --store temporalstore --entities 10000 --features 20
    python bench_online_store.py --store redis         --entities 10000 --features 20

The two runs print the same table, and the caller differences them. Nothing here computes a
ratio: a ratio across two processes is the caller's to form, and quoting one from a single arm is
how a half-measured comparison gets published.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time


def load_average() -> float:
    try:
        return os.getloadavg()[0]
    except (OSError, AttributeError):
        return float("nan")


def directory_bytes(path: str) -> int:
    """Bytes under a directory, which is the denominator this script NAMES in its output."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def percentile(samples, fraction: float) -> float:
    if not samples:
        return float("nan")
    ordered = sorted(samples)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", choices=("temporalstore", "redis"), required=True)
    parser.add_argument("--repo", default=".", help="a Feast repo directory holding feature_store.yaml")
    parser.add_argument("--entities", type=int, default=10_000)
    parser.add_argument("--features", type=int, default=20)
    parser.add_argument("--reads", type=int, default=2_000)
    parser.add_argument("--batch", type=int, default=100, help="entities per get_online_features call")
    parser.add_argument("--max-load", type=float, default=2.0)
    parser.add_argument(
        "--data-dir",
        default=None,
        help="the store's data directory, for the bytes-per-row figure. Omitted means that "
        "figure is reported as unavailable rather than guessed.",
    )
    args = parser.parse_args()

    from feast import FeatureStore

    load_before = load_average()
    store = FeatureStore(repo_path=args.repo)

    # --- the fixture -----------------------------------------------------------------
    views = store.list_feature_views()
    if not views:
        print("no feature views in this repo -- run `feast apply` first", file=sys.stderr)
        return 2
    view = views[0]
    entity_name = view.entities[0]
    feature_names = [f.name for f in view.features][: args.features]
    refs = ["{}:{}".format(view.name, name) for name in feature_names]

    bytes_before = directory_bytes(args.data_dir) if args.data_dir else None

    # --- writes ----------------------------------------------------------------------
    # Driven through Feast's own materialization entry point rather than a hand-rolled write, so
    # the figure covers the code path a user actually runs.
    write_started = time.perf_counter()
    subprocess.run(
        [sys.executable, "-m", "feast", "materialize-incremental", "--views", view.name,
         time.strftime("%Y-%m-%dT%H:%M:%S")],
        cwd=args.repo,
        check=False,
        capture_output=True,
    )
    write_seconds = time.perf_counter() - write_started
    load_after_write = load_average()

    bytes_after = directory_bytes(args.data_dir) if args.data_dir else None

    # --- reads -----------------------------------------------------------------------
    samples = []
    rows_per_call = args.batch
    for index in range(args.reads):
        entity_rows = [
            {entity_name: (index * rows_per_call + offset) % args.entities}
            for offset in range(rows_per_call)
        ]
        started = time.perf_counter()
        store.get_online_features(features=refs, entity_rows=entity_rows).to_dict()
        samples.append((time.perf_counter() - started) * 1000.0)
    load_after_read = load_average()

    peak_load = max(load_before, load_after_write, load_after_read)
    trustworthy = peak_load <= args.max_load

    result = {
        "store": args.store,
        "entities": args.entities,
        "features": len(feature_names),
        "entities_per_read_call": rows_per_call,
        "read_calls": args.reads,
        "read_ms_p50": round(percentile(samples, 0.50), 4),
        "read_ms_p99": round(percentile(samples, 0.99), 4),
        "read_ms_mean": round(statistics.fmean(samples), 4),
        "materialize_seconds": round(write_seconds, 3),
        "rows_per_second": round(args.entities / write_seconds, 1) if write_seconds else None,
        "load_before": round(load_before, 2),
        "load_after_write": round(load_after_write, 2),
        "load_after_read": round(load_after_read, 2),
        "latency_trustworthy": trustworthy,
        "latency_label": "MEASURED" if trustworthy else "PROVISIONAL -- box was loaded",
    }
    if bytes_before is not None and bytes_after is not None:
        grew = bytes_after - bytes_before
        result["disk_bytes_denominator"] = (
            "sum of file sizes under {} , over {} entity rows x {} features".format(
                args.data_dir, args.entities, len(feature_names)
            )
        )
        result["disk_bytes_growth"] = grew
        result["disk_bytes_per_row"] = round(grew / max(args.entities, 1), 2)
    else:
        result["disk_bytes_per_row"] = "unavailable -- pass --data-dir"

    print(json.dumps(result, indent=2))
    if not trustworthy:
        print(
            "\nREFUSING to present the latency figures as measured: peak load average was "
            "{:.2f} against a limit of {:.2f}. Re-run on a quiet box.".format(
                peak_load, args.max_load
            ),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

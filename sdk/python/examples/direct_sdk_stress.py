from __future__ import annotations

import argparse
import hashlib
import json
import time

from temporalstore import Client, FeaturePoint, Options


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stress TemporalStore's native C SDK path with repeated hash and feature calls."
    )
    parser.add_argument("--metaserver", default="127.0.0.1:18200")
    parser.add_argument("--namespace", default="sdk_ns")
    parser.add_argument("--table", default="sdk_table")
    parser.add_argument("--prefix", default="direct-stress")
    parser.add_argument("--hash-ops", type=int, default=2000)
    parser.add_argument("--feature-keys", type=int, default=32)
    parser.add_argument("--feature-points-per-key", type=int, default=64)
    parser.add_argument("--value-bytes", type=int, default=512)
    parser.add_argument("--request-timeout-ms", type=int, default=10000)
    parser.add_argument("--io-timeout-ms", type=int, default=5000)
    parser.add_argument("--report-json", default="")
    return parser.parse_args()


def digest_json(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    value_payload = "x" * max(args.value_bytes, 1)
    options = Options(
        metaserver_addr=args.metaserver,
        namespace_name=args.namespace,
        table_name=args.table,
        request_timeout_ms=args.request_timeout_ms,
        io_timeout_ms=args.io_timeout_ms,
        max_read_retries=2,
        max_write_retries=1,
    )

    hash_reads = 0
    feature_reads = 0
    hash_oracle: dict[str, dict[str, str]] = {}
    feature_oracle: dict[str, list[tuple[int, str]]] = {}
    hash_overwrite_checks = 0
    with Client(options) as client:
        for i in range(args.hash_ops):
            key = f"{args.prefix}:hash:{i % 97}"
            field = f"field:{i}"
            value = f"{i}:{value_payload}"
            client.hset(key, field, value)
            hash_oracle.setdefault(key, {})[field] = value
            if i % 3 == 0:
                got = client.hget(key, field)
                expected = hash_oracle[key][field]
                if got != expected:
                    raise AssertionError(f"hget mismatch at {i}: {got!r} != {expected!r}")
                hash_reads += 1

        # MatrixArk-style records frequently upsert the same logical key/field
        # during idempotent ingest, index repair, summary refresh, and audit
        # replay. Verify the C++ path has the same overwrite semantics as the
        # Python in-memory oracle under the same scale corpus.
        for i in range(0, args.hash_ops, max(1, args.hash_ops // 100)):
            key = f"{args.prefix}:hash:{i % 97}"
            field = f"field:{i}"
            value = f"{i}:overwrite:{value_payload[:128]}"
            client.hset(key, field, value)
            hash_oracle[key][field] = value
            got = client.hget(key, field)
            if got != value:
                raise AssertionError(f"hset overwrite mismatch at {i}: {got!r} != {value!r}")
            hash_overwrite_checks += 1

        for key, fields in hash_oracle.items():
            for field, expected in fields.items():
                if field.endswith("0") or field.endswith("7"):
                    got = client.hget(key, field)
                    if got != expected:
                        raise AssertionError(
                            f"hash oracle mismatch for {key}/{field}: {got!r} != {expected!r}"
                        )
                    hash_reads += 1

        base_ts = 1_800_000_000_000
        for key_idx in range(args.feature_keys):
            key = f"{args.prefix}:feature:{key_idx}"
            points = [
                FeaturePoint(base_ts + offset, f"{key_idx}:{offset}:{value_payload[:64]}")
                for offset in range(args.feature_points_per_key)
            ]
            feature_oracle[key] = [(point.timestamp, point.value) for point in points]
            client.add_feature_points(key, points)
            got_points = client.query_feature_points(
                key,
                base_ts,
                base_ts + args.feature_points_per_key,
                args.feature_points_per_key,
            )
            expected_points = feature_oracle[key]
            actual_points = [(point.timestamp, point.value) for point in got_points]
            if actual_points != expected_points:
                raise AssertionError(
                    f"feature query mismatch for {key}: "
                    f"{actual_points[:3]}... != {expected_points[:3]}..."
                )
            feature_reads += len(got_points)

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    hash_field_count = sum(len(fields) for fields in hash_oracle.values())
    feature_point_count = sum(len(points) for points in feature_oracle.values())
    result = {
        "status": "passed",
        "parity_checked": True,
        "metaserver": args.metaserver,
        "namespace": args.namespace,
        "table": args.table,
        "hash_ops": args.hash_ops,
        "hash_keys": len(hash_oracle),
        "hash_fields": hash_field_count,
        "hash_reads": hash_reads,
        "hash_overwrite_checks": hash_overwrite_checks,
        "hash_oracle_digest": digest_json(hash_oracle),
        "feature_keys": args.feature_keys,
        "feature_points_written": feature_point_count,
        "feature_points_read": feature_reads,
        "feature_oracle_digest": digest_json(feature_oracle),
        "value_bytes": args.value_bytes,
        "elapsed_ms": round(elapsed_ms, 3),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, sort_keys=True)
            fh.write("\n")


if __name__ == "__main__":
    main()

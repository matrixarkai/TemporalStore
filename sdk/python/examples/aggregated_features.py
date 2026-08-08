"""
Aggregated features on TemporalStore — runnable example.

Demonstrates the aggregated-feature serving patterns against a live proxy:
  * dual-write raw observations + Control-State rollup counters
  * exact serving-time FeatureAggregate (count / sum / max)
  * long-window HYBRID: sealed rollup buckets + a bounded raw-sequence tail
  * a frequency cap

Run against a local single-node proxy:
    TEMPORALSTORE_ENDPOINT=http://127.0.0.1:17102 \
      python sdk/python/examples/aggregated_features.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

# Allow running straight from a checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from temporalstore.features import Config, TemporalFeatureStore  # noqa: E402

MINUTE = 60_000


def main() -> int:
    with TemporalFeatureStore(Config.from_env()) as fs:
        if not fs.health():
            print("proxy not reachable at", fs.endpoint, "- start a node first")
            return 1

        now = int(time.time() * 1000)
        feature_key = "feature:content_interaction:user:demo42"
        cs_key = "cs:clicks:user:demo42"

        # 1) dual-write 5 clicks with dwell metrics (raw row + rollup increment)
        for i, dwell in enumerate([12, 91, 42, 7, 60]):
            fs.record_event(feature_key, cs_key, now + i * 1000, metric=dwell, precision_ms=MINUTE)

        hour_ago = now - 3_600_000
        end = now + 10_000
        print("clicks (count) last hour :", fs.aggregate(feature_key, hour_ago, end, "count"))
        print("dwell  (sum)   last hour :", fs.aggregate(feature_key, hour_ago, end, "sum"))
        print("dwell  (max)   last hour :", fs.aggregate(feature_key, hour_ago, end, "max"))
        print("dwell  (avg)   last hour :", fs.aggregate(feature_key, hour_ago, end, "avg"))

        # 2) long window served as sealed rollup buckets + exact raw tail
        seven_days = now - 7 * 86_400_000
        print("clicks (count) last 7d   :",
              fs.aggregate_long_window(feature_key, cs_key, seven_days, end,
                                       op="count", precision_ms=MINUTE))

        # 3) frequency cap: allow up to 5 impressions / day
        decision = fs.frequency_cap(cs_key, now + 20_000, limit=5, window_ms=86_400_000)
        print("cap decision             :", decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

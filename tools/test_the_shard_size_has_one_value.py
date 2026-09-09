"""Shard size is part of the on-disk contract, so every surface must agree on it.

A record lands in shard `arrival_index // shard_size`. The asymmetry is what makes this dangerous:
a reader assuming a LARGER shard size than the writer used computes `max_shard = (count - 1) / size`,
enumerates too few shards, and returns part of the store while reporting success. The other
direction is harmless -- surplus shard keys simply do not exist and read back empty.

It was 256 in the two serving modules, 4096 in the backfill (the only module that honoured the
environment variable), and 1024 in the engine's fallback. The engine guessing 1024 against a store
written at 256 would have read a quarter of it.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)


def python_defaults():
    found = {}
    for name in (
        "matrixark_mcp_core.py",
        "matrixark_mcp_runtime_config.py",
        "matrixark_context_backfill.py",
    ):
        body = open(os.path.join(HERE, name), encoding="utf-8").read()
        match = re.search(
            r'DIRECT_RECORD_LOG_SHARD_SIZE = int\(os\.environ\.get\(\s*"MATRIXARK_DIRECT_RECORD_LOG_SHARD_SIZE",\s*"(\d+)"\s*\)\)',
            body,
        )
        found[name] = int(match.group(1)) if match else None
    return found


def rust_default():
    path = os.path.join(ROOT, "crates", "temporalstore-rust", "src", "matrixark_rust_proxy_impl.rs")
    body = open(path, encoding="utf-8").read()
    match = re.search(r"const DEFAULT_RECORD_LOG_SHARD_SIZE: u64 = (\d+);", body)
    return int(match.group(1)) if match else None


def every_surface_agrees_on_the_shard_size():
    values = python_defaults()
    values["rust"] = rust_default()
    for name, value in values.items():
        check(value is not None, "%s no longer declares the shard size the expected way" % name)
    distinct = {v for v in values.values() if v is not None}
    check(len(distinct) == 1, "shard size disagrees across surfaces: %s" % (values,))


def every_surface_honours_the_environment_variable():
    """It was hardcoded in the two serving modules, so the knob existed and only the backfill read
    it -- which is how the writer and the reader came to disagree in the first place."""
    for name in ("matrixark_mcp_core.py", "matrixark_mcp_runtime_config.py"):
        body = open(os.path.join(HERE, name), encoding="utf-8").read()
        check(
            "MATRIXARK_DIRECT_RECORD_LOG_SHARD_SIZE" in body,
            "%s hardcodes the shard size instead of reading the variable" % name,
        )


def the_engine_does_not_guess_a_larger_size():
    """The engine's fallback must never exceed the writer's default.

    Larger is the losing direction: it under-enumerates shards and silently drops records. This
    asserts the relationship, not just the number, so raising the writer's default later cannot
    quietly leave the engine reading a fraction of the store.
    """
    writer = python_defaults().get("matrixark_mcp_core.py")
    engine = rust_default()
    if writer is None or engine is None:
        return
    check(
        engine <= writer,
        "the engine assumes %d records per shard against a writer at %d, so it would enumerate "
        "too few shards and read part of the store" % (engine, writer),
    )


for test in (
    every_surface_agrees_on_the_shard_size,
    every_surface_honours_the_environment_variable,
    the_engine_does_not_guess_a_larger_size,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all shard-size checks pass")

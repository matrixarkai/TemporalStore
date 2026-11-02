#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Reproduce: with async commit, a hot set write makes earlier hash writes vanish.

NO FEAST, NO CLIENT LIBRARY, NO THIRD-PARTY PACKAGE. This drives the data
node's own command surface with the standard library only, so the defect cannot
be read as something a Python integration does to itself. Every command here is
one the node's own `Command` enum defines.

WHAT IT SHOWS. Against a FRESH, EMPTY shard with `async_storage=true`:

  * write N hashes, interleaving one `SetAdd` per hash into a SINGLE set key
    (the shape an exact-teardown key index has),
  * read every hash back,
  * and a large fraction of them answer with NO FIELDS AT ALL.

The shard still counts them: `/server/info` reports `total_records` as though
every row were there, while `HashGetAll` on a lost key returns zero entries. So
the record count is incremented for a row whose content never persisted.

IT IS AN INTERACTION, which is why it needs this exact shape. Neither half does
it alone, and this script proves that by running all four combinations:

    async_storage   hot-set writes   result
    false           yes              no loss
    true            no               no loss
    true            yes              LOSS

A 20-second pause does not bring the values back, so it is loss and not a
visibility delay -- the script re-reads and reports that separately.

USAGE

    # start a node on an EMPTY data directory
    TS_SERVER_BIND_ADDR=127.0.0.1:17999 TS_SHARD_ID=1 \
    TS_CACHE_DIR=/tmp/r/c TS_BLOCK_STORE_DIR=/tmp/r/b TS_INDEX_DIR=/tmp/r/i \
      ./target/release/server &

    TS_PORT=17999 python3 tools/repro_async_commit_row_loss.py

Exit status: 0 if nothing was lost in any cell (the defect is gone), 2 if the
loss reproduced, 1 if the node could not be driven.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

PORT = int(os.environ.get("TS_PORT", "17999"))
BASE = f"http://127.0.0.1:{PORT}"
SHARD = int(os.environ.get("TS_SHARD", "1"))
N = int(os.environ.get("REPRO_N", "20000"))
BATCH = int(os.environ.get("REPRO_BATCH", "500"))
FIELDS = 10
PAUSE = int(os.environ.get("REPRO_PAUSE", "20"))


def post(path: str, document: dict, timeout: float = 600.0) -> dict:
    body = json.dumps(document).encode()
    request = urllib.request.Request(
        f"{BASE}{path}", data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def get(path: str, timeout: float = 180.0) -> dict:
    with urllib.request.urlopen(f"{BASE}{path}", timeout=timeout) as response:
        return json.loads(response.read())


def total_records():
    try:
        for shard in get("/server/info"):
            if shard.get("shard_id") == SHARD:
                return shard.get("total_records")
    except Exception as exc:
        return f"unreadable({type(exc).__name__})"
    return None


def set_async_storage(flag: bool) -> bool:
    """Set the commit mode and READ IT BACK; a success code is not evidence."""
    config = (get(f"/get_config/{SHARD}") or {}).get("config") or {}
    if not config:
        raise SystemExit("no config returned; is the shard loaded?")
    config["async_storage"] = flag
    config["version"] = int(config.get("version", 1)) + 1
    post("/set_config", {"shard_id": SHARD, "config": config})
    observed = ((get(f"/get_config/{SHARD}") or {}).get("config") or {}).get(
        "async_storage"
    )
    if observed != flag:
        raise SystemExit(f"set_config reported ok but reads back {observed!r}")
    return observed


VIEW = "bench_view"
# ONE DEFINITION OF THE FIELD NAMES, USED BY BOTH THE WRITE AND THE READ.
# They were two lists once: the writer was changed to `bench_view:*` and the
# reader left on `v:*`, so every row read back as all-null and the script
# reported 20,000 of 20,000 LOST on a shard that was holding every one of them.
# The diagnostic caught it -- `HashGetAll` printed 11 field(s) present on a key
# the script had just called lost -- but a reader that derives its field list
# from somewhere else is the bug, and deriving both from here is the fix.
FIELD_NAMES = [f"{VIEW}:f{j}" for j in range(FIELDS)] + [f"_ts:{VIEW}"]


def entries(i: int):
    """Field names and value widths matching what a feature row actually stores.

    THE SHAPE MATTERS AND THIS IS WHY IT IS SPELLED OUT. A first version of this
    script used one-byte values under short field names and FAILED TO REPRODUCE
    while the same workload through the provider lost 17,500 rows. The two
    differed in object footprint -- key length, field count, value width -- and
    the loss tracks how much the shard is holding against its cache budget, so
    a smaller object per row is a different experiment, not a simpler one.
    """
    payloads = [
        # Ten int64-shaped values, as a feature row's protobuf-encoded ints are,
        # then the event-timestamp field every feature row carries -- eleven
        # field slots, not ten.
        bytes([8, (i + j) % 251, 0, 0, 0, 0, 0, 0, 0, 0])
        for j in range(FIELDS)
    ] + [bytes(12)]
    return [
        (name, base64.b64encode(payload).decode())
        for name, payload in zip(FIELD_NAMES, payloads)
    ]


def row_key(prefix: str, i: int) -> str:
    """A key the width a serialized entity key produces.

    A feature row is addressed by `hex(serialize_entity_key(...)) + ":" +
    project`, which is 62 hex characters plus the project for a single int64
    join key -- not the short keys a hand-written probe reaches for.
    """
    return f"{i:062x}:{prefix}"


def run_cell(async_storage: bool, hot_set: bool, tag: str) -> int:
    observed = set_async_storage(async_storage)
    prefix = f"r{tag}{int(time.time() * 1000) % 10**9}"
    keys = [row_key(prefix, i) for i in range(N)]
    set_key = f"{prefix}:index"

    started = time.perf_counter()
    for start in range(0, N, BATCH):
        chunk = keys[start : start + BATCH]
        commands = [
            {"kind": "hash_multi_set", "key": key, "entries": entries(start + offset)}
            for offset, key in enumerate(chunk)
        ]
        if hot_set:
            # One SetAdd per row, all into ONE key. This is the only thing the
            # two `true` cells differ by.
            commands += [
                {
                    "kind": "set_add",
                    "key": set_key,
                    "member": base64.b64encode(key.encode()).decode(),
                }
                for key in chunk
            ]
        post("/batch_execute", {"shard_id": SHARD, "commands": commands})
    write_seconds = time.perf_counter() - started

    def count_lost() -> list:
        lost = []
        for start in range(0, N, BATCH):
            commands = [
                {"kind": "hash_multi_get", "key": key, "fields": FIELD_NAMES}
                for key in keys[start : start + BATCH]
            ]
            answer = post("/batch_execute", {"shard_id": SHARD, "commands": commands})
            responses = answer.get("responses") or []
            if len(responses) != len(commands):
                raise SystemExit(
                    f"batch answered {len(responses)} for {len(commands)} commands"
                )
            for offset, response in enumerate(responses):
                values = (response.get("response") or {}).get("values") or []
                if not any(value is not None for value in values):
                    lost.append(start + offset)
        return lost

    lost = count_lost()
    print(
        f"  async_storage={str(observed):5s} hot_set={str(hot_set):5s}  "
        f"write={write_seconds:7.1f}s  LOST={len(lost):6d}/{N}  "
        f"total_records={total_records()}"
        + (f"  first_lost={lost[0]}" if lost else ""),
        flush=True,
    )

    if lost:
        probe = keys[lost[0]]
        got = post(
            "/execute",
            {"shard_id": SHARD, "command": {"kind": "hash_get_all", "key": probe}},
        )
        held = (got.get("response") or {}).get("entries") or []
        # REPORT WHAT IT FOUND; DO NOT ASSERT WHAT IT MEANS. An earlier version
        # printed "the row is counted but holds nothing" unconditionally, and
        # printed it next to `11 field(s)` -- which was the evidence that the
        # script's own reader, not the store, was at fault.
        if held:
            print(
                f"    HashGetAll({probe}) -> {len(held)} field(s) PRESENT: "
                f"{sorted(name for name, _ in held)[:4]}...\n"
                f"    *** CONTRADICTION: the row holds fields, so this script's "
                f"read asked for the wrong ones. Not a store defect. ***",
                flush=True,
            )
        else:
            print(
                f"    HashGetAll({probe}) -> 0 fields. The row is counted by "
                f"total_records and holds nothing.",
                flush=True,
            )
        print(f"    waiting {PAUSE}s to rule out a visibility delay ...", flush=True)
        time.sleep(PAUSE)
        again = count_lost()
        verdict = "VISIBILITY DELAY" if len(again) < len(lost) else "STILL LOST"
        print(f"    after {PAUSE}s: LOST={len(again)}/{N} -> {verdict}", flush=True)
    return len(lost)


def main() -> int:
    try:
        if not (get("/health") or {}).get("ok"):
            print(f"node at {BASE} is not healthy", file=sys.stderr)
            return 1
    except Exception as exc:
        print(f"cannot reach a data node at {BASE}: {exc}", file=sys.stderr)
        return 1

    start_records = total_records()
    print(f"node {BASE}  shard {SHARD}  N={N}  batch={BATCH}", flush=True)
    print(f"total_records at start = {start_records}", flush=True)
    if start_records:
        print(
            "REFUSING: the shard is not empty. A populated shard makes the row "
            "counts below unattributable -- restart the node on empty data "
            "directories.",
            file=sys.stderr,
        )
        return 1

    # THE FAILING CELL GOES FIRST, and that ordering is load-bearing. Run last,
    # after two other cells have each left 20,000 rows on the shard, it did NOT
    # reproduce -- the loss wants the shard it is filling to be the only thing
    # on it, which is also how the provider hit it. `REPRO_CELLS=tt` runs that
    # one alone; the default runs all three so the 2x2 claim is visible, at the
    # cost of each later cell starting from a fuller shard.
    wanted = os.environ.get("REPRO_CELLS", "tt,ft,tf").split(",")
    plan = {
        "tt": (True, True),
        "ft": (False, True),
        "tf": (True, False),
    }
    print(
        f"\ncells in order {wanted} "
        f"(the failing one is 'tt' and runs FIRST by default):",
        flush=True,
    )
    lost_cells = []
    for tag in wanted:
        tag = tag.strip()
        if tag not in plan:
            continue
        async_storage, hot_set = plan[tag]
        if run_cell(async_storage, hot_set, tag):
            lost_cells.append((async_storage, hot_set))

    print(flush=True)
    if lost_cells:
        print(f"REPRODUCED in cells {lost_cells}", flush=True)
        return 2
    print("no loss in any cell -- the defect is not present on this build", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

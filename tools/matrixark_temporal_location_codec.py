#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A record location is a shard and an offset. Storing it took 69 bytes.

A placement or locator row lists where records live, as
``{"key": "matrixark:mcp:records:000003", "field": "00000000000000000014"}``. Measured over 300
ingests, that list was **87% of every byte written to a page**: 1,132 entries per add at 69 bytes
each, and across 339,660 entries the ``key`` took just **nine** distinct values, because the base
is one deployment-wide string and the shard is a number that fits in three digits.

So 41% of those bytes re-spelled a constant, 29% were a twenty-digit zero-padded rendering of a
number usually under five digits, and 30% were JSON punctuation around them.

The compact form is ``"3:14"`` -- shard and offset, in decimal, in one string. About six bytes
where there were sixty-nine.

Two decisions are load-bearing:

* **Shard and offset are both written out**, rather than the single global sequence they can be
  packed into. The sequence form needs the reader to know ``shard_size`` to unpack it, which makes
  every stored location depend on a constant staying the same forever -- change it and old
  locations silently decode to the wrong records. This form needs no shared constant.
* **A location whose base is not the reader's own record log stays a dict.** Readers already skip
  foreign bases; compacting one would assert something the form cannot express.

Decoding accepts both shapes, so a store written before this reads unchanged. The writer has
no switch back: the long form is what the compact form replaced, and a way to re-emit it kept
a second encoding alive that no reader treated differently.
"""

from __future__ import annotations

import os
from typing import Any

SHARD_DIGITS = 6
FIELD_DIGITS = 20
def compact_location(key: str, field: str, base: str) -> Any:
    """``(key, field)`` as ``"shard:offset"`` when it belongs to ``base``, else the dict as-is."""
    original = {"key": key, "field": field}
    if not base:
        return original
    if not key.startswith(base + ":"):
        return original
    shard = key[len(base) + 1 :]
    if len(shard) != SHARD_DIGITS or not shard.isdigit():
        return original
    if len(field) != FIELD_DIGITS or not field.isdigit():
        return original
    return "%d:%d" % (int(shard), int(field))


def compact_location_list(locations: list, base: str) -> list:
    """Compact every entry that can be, leaving the rest exactly as they are."""
    out = []
    for entry in locations or []:
        if isinstance(entry, dict):
            key = str(entry.get("key") or "")
            field = str(entry.get("field") or "")
            if key and field:
                out.append(compact_location(key, field, base))
                continue
        elif isinstance(entry, tuple) and len(entry) == 2:
            # The merge's token for a location it could not compact -- a foreign base. It has to
            # go back out as the long form, which is the only shape that can express it.
            out.append({"key": str(entry[0]), "field": str(entry[1])})
            continue
        out.append(entry)
    return out


def expand_location(entry: Any, base: str) -> tuple[str, str] | None:
    """Any stored location shape -> ``(key, field)``, or ``None`` if it is neither."""
    if isinstance(entry, str):
        shard, _, offset = entry.partition(":")
        if not shard.isdigit() or not offset.isdigit() or not base:
            return None
        return (
            "%s:%0*d" % (base, SHARD_DIGITS, int(shard)),
            "%0*d" % (FIELD_DIGITS, int(offset)),
        )
    if isinstance(entry, dict):
        key = str(entry.get("key") or "")
        field = str(entry.get("field") or "")
        if not key or not field:
            return None
        return key, field
    return None

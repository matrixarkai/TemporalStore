# SPDX-License-Identifier: Apache-2.0
"""Stable table names for LMCache model layouts."""

from hashlib import sha256
import json


def layout_table_name(model_name: str, world_size: int, local_world_size: int) -> str:
    layout = json.dumps((model_name, world_size, local_world_size), separators=(",", ":"))
    return "kv_" + sha256(layout.encode("utf-8")).hexdigest()[:16]

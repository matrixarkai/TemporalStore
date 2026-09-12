#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Bundle flattening for MatrixArk TemporalStore adapters.

This module once also held a second copy of the latest context-state adapter methods --
`LatestContextStateAdapterMixin` and the five module-level helpers it delegated to. They were
the OLDER variant of the seven methods on `_TemporalDirectBackendMixin`, extracted and then
never adopted: the mixin was named exactly once in the repository, its own class line.

They are gone rather than shared, and the measurement that decided it is worth keeping: of the
seven method names the two classes had in common, two bodies agreed and on the other five the
backend carried logic this copy did not. `latest_context_state_payload` serialised the whole
record where the backend serialises `slim_persisted_record(record)` first, so adopting the mixin
would have fattened every latest-state write; `_with_latest_context_state_records` and
`_split_compacted_latest_context_state` both run on the retrieval hot path. So this was not a
shared home waiting to be used -- it was the variant the backend had grown past, kept alive only
by a guard describing it.

`expand_record_bundles` is the one export anything reaches, from
`matrixark_temporal_direct_backend` and `matrixark_mcp_temporal_adapters`.
"""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


def expand_record_bundles(records: list[Json]) -> list[Json]:
    """Flatten bundled appends into their constituent records.

    A bundled append stores ``{"record_bundle": [rec, rec, ...]}`` as ONE hash field. The
    wrapper carries no ``record_type``; the type every reader filters on lives on the
    records inside it. Readers walking the raw entries therefore saw a typeless wrapper and
    skipped it, so a bundled ``context_event`` was invisible to get / get_all / update /
    history even though it was durably stored. The JSONL adapter appends records
    individually and never bundles, which is why the same reader code worked there.

    Idempotent: unbundled records pass through untouched.
    """
    expanded: list[Json] = []
    for record in records:
        if isinstance(record, dict):
            bundle = record.get("record_bundle")
            if isinstance(bundle, list):
                expanded.extend(inner for inner in bundle if isinstance(inner, dict))
                continue
        expanded.append(record)
    return expanded

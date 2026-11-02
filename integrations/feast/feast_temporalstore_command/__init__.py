# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""TemporalStore as a Feast online store, over its native command surface.

Point Feast at it from ``feature_store.yaml``::

    online_store:
      type: feast_temporalstore_command.TemporalStoreCommandOnlineStore
      host: 127.0.0.1
      port: 17102

THE ONLINE-STORE IMPORT IS LAZY, AND THAT IS LOAD-BEARING. ``client`` is pure
standard library, and its tests are the correctness argument for the wire
encoding, so they have to run with neither Feast nor pydantic installed. An
eager ``from .online_store import ...`` here would drag Feast's whole stack in
through the package and the first casualty would be those tests. PEP 562 keeps
the public names reachable without paying for them at import.
"""

from .client import (
    TemporalStoreClient,
    TemporalStoreCommandError,
    decode_bytes,
    encode_bytes,
)

__all__ = [
    "TemporalStoreClient",
    "TemporalStoreCommandError",
    "TemporalStoreCommandOnlineStore",
    "TemporalStoreCommandOnlineStoreConfig",
    "decode_bytes",
    "encode_bytes",
]

__version__ = "0.1.0"

_LAZY = {
    "TemporalStoreCommandOnlineStore",
    "TemporalStoreCommandOnlineStoreConfig",
}


def __getattr__(name: str):
    if name in _LAZY:
        from . import online_store

        return getattr(online_store, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)

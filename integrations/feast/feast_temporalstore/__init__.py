# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""TemporalStore as a Feast online store.

THE IMPORTS HERE ARE LAZY, AND THAT IS LOAD-BEARING. ``wire`` is pure stdlib and its tests are
the correctness argument for the encoding, so they must run with neither Feast nor pydantic
installed. An eager ``from .online_store import ...`` at module scope would pull Feast's whole
stack in through the package, and the first thing it would break is the test suite that proves
the encoding is needed at all. PEP 562 keeps the public names available without that cost.
"""

__all__ = ["TemporalStoreOnlineStore", "TemporalStoreOnlineStoreConfig"]


def __getattr__(name: str):
    if name in __all__:
        from . import online_store

        return getattr(online_store, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)

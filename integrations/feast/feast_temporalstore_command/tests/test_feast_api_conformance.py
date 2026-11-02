#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Conformance: drive this store through FEAST'S OWN PUBLIC API, not our stubs.

WHY THIS EXISTS BESIDE THE STUB TESTS. ``test_online_store.py`` replaces the
transport with a recording stub and asserts on the commands emitted, which is
the right instrument for "is the wire format correct" and a useless one for "is
this provider usable from Feast". Everything between ``get_online_features``
and ``online_read`` -- registry lookup, feature-ref grouping, entity-row
marshalling, the proto-to-python conversion, the async entry point -- is Feast's
code, and a stub cannot exercise any of it.

So this file calls only the public surface: ``FeatureStore.apply``,
``write_to_online_store``, ``get_online_features``,
``get_online_features_async`` and ``teardown``. If Feast changes how it reaches
an online store, these fail and the stub tests do not.

IT NEEDS A RUNNING NODE, and it says so rather than passing. Set
``TEMPORALSTORE_TEST_ADDR=host:port`` to point at one, or leave it unset and
the suite starts one itself through
:class:`~feast_temporalstore_command.feast_online_store_creator.TemporalStoreOnlineStoreCreator`.
With neither available every test here SKIPS with a reason -- it does not
quietly report success for work it did not do.

VALUES ARE COMPARED AS BYTES. A feature that comes back as the right type and
the wrong number is a defect that an equality check on a rendered dict can miss
when both sides are ``None``, so the fixtures use values that are distinct per
entity and the assertions name the entity they belong to.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import feast  # noqa: F401
    import pandas  # noqa: F401

    FEAST_AVAILABLE = True
except Exception:  # pragma: no cover
    FEAST_AVAILABLE = False

FEAST_VERSION_TESTED = "0.67.0"


def _node_or_skip():
    """Return an ``online_store`` block, or a reason to skip."""
    from feast_temporalstore_command.feast_online_store_creator import (
        TemporalStoreOnlineStoreCreator,
    )

    creator = TemporalStoreOnlineStoreCreator("conformance")
    try:
        return creator.create_online_store(), creator
    except Exception as exc:
        return None, f"no TemporalStore data node available: {exc}"


@unittest.skipUnless(FEAST_AVAILABLE, "feast and pandas are not installed")
class FeastPublicApiConformanceTest(unittest.TestCase):
    """The provider, exercised only through Feast's public API."""

    @classmethod
    def setUpClass(cls):
        cls._creator = None
        block, second = _node_or_skip()
        if block is None:
            raise unittest.SkipTest(second)
        cls._online_store = block
        cls._creator = second
        cls._tempdir = tempfile.mkdtemp(prefix="feast-conformance-")

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "_creator", None) is not None:
            try:
                cls._creator.teardown()
            except Exception:
                pass
        shutil.rmtree(getattr(cls, "_tempdir", ""), ignore_errors=True)

    # -- fixture -------------------------------------------------------------

    def _repo(self, project):
        from feast import Entity, FeatureView, Field, FileSource, FeatureStore, RepoConfig
        from feast.types import Int64, String

        import pandas as pd

        directory = os.path.join(self._tempdir, project)
        os.makedirs(directory, exist_ok=True)
        source_path = os.path.join(directory, "src.parquet")
        now = datetime.now(timezone.utc)
        pd.DataFrame(
            {
                "driver_id": [1, 2],
                "trips": [10, 20],
                "city": ["a", "b"],
                "ts": [now, now],
            }
        ).to_parquet(source_path, index=False)

        config = RepoConfig(
            project=project,
            provider="local",
            registry=os.path.join(directory, "registry.db"),
            entity_key_serialization_version=3,
            online_store=dict(self._online_store),
        )
        source = FileSource(name="src", path=source_path, timestamp_field="ts")
        entity = Entity(name="driver", join_keys=["driver_id"])
        view = FeatureView(
            name="driver_stats",
            entities=[entity],
            schema=[Field(name="trips", dtype=Int64), Field(name="city", dtype=String)],
            source=source,
        )
        store = FeatureStore(config=config)
        store.apply([entity, view])
        return store, entity, view

    def _rows(self, count):
        import pandas as pd

        event = datetime.now(timezone.utc) - timedelta(minutes=1)
        return pd.DataFrame(
            {
                "driver_id": list(range(count)),
                "trips": [i * 7 + 1 for i in range(count)],
                "city": [f"city-{i}" for i in range(count)],
                "ts": [event] * count,
                "created": [event] * count,
            }
        )

    # -- the tests -----------------------------------------------------------

    def test_apply_then_write_then_get_online_features_round_trips_the_values(self):
        """`feast apply` -> `write_to_online_store` -> `get_online_features`."""
        store, _entity, _view = self._repo("conf_rt")
        count = 25
        store.write_to_online_store("driver_stats", self._rows(count))

        answer = store.get_online_features(
            features=["driver_stats:trips", "driver_stats:city"],
            entity_rows=[{"driver_id": i} for i in range(count)],
        ).to_dict()

        # DENOMINATOR FIRST: the fixture really has 25 rows and 25 came back.
        self.assertEqual(len(answer["driver_id"]), count)
        self.assertEqual(len(answer["trips"]), count)
        served = sum(1 for v in answer["trips"] if v is not None)
        self.assertEqual(
            served, count, f"only {served} of {count} entities served a value"
        )
        for i in range(count):
            position = answer["driver_id"].index(i)
            self.assertEqual(
                answer["trips"][position], i * 7 + 1, f"entity {i} trips"
            )
            self.assertEqual(answer["city"][position], f"city-{i}", f"entity {i} city")

    def test_an_entity_never_written_reads_as_none_not_as_a_zero(self):
        store, _e, _v = self._repo("conf_absent")
        store.write_to_online_store("driver_stats", self._rows(3))
        answer = store.get_online_features(
            features=["driver_stats:trips"],
            entity_rows=[{"driver_id": 999_999}],
        ).to_dict()
        self.assertEqual(len(answer["trips"]), 1)
        self.assertIsNone(
            answer["trips"][0],
            "an unwritten entity must read as None; a zero would be a silently "
            "wrong feature rather than a missing one",
        )

    def test_get_online_features_async_works_at_all(self):
        """The async endpoint, which the base class leaves raising.

        `OnlineStore.online_read_async` is `raise NotImplementedError` in Feast
        0.67.0, so a provider that implements only the four abstract methods
        fails here. This is the test that says whether the async serving path is
        usable, and it goes through `get_online_features_async`, not through our
        own coroutine, so Feast's own async plumbing is what is exercised.
        """
        import asyncio

        store, _e, _v = self._repo("conf_async")
        count = 10
        store.write_to_online_store("driver_stats", self._rows(count))

        async def ask():
            return (
                await store.get_online_features_async(
                    features=["driver_stats:trips"],
                    entity_rows=[{"driver_id": i} for i in range(count)],
                )
            ).to_dict()

        answer = asyncio.run(ask())
        self.assertEqual(len(answer["trips"]), count)
        served = sum(1 for v in answer["trips"] if v is not None)
        self.assertEqual(served, count, f"async served {served} of {count}")
        for i in range(count):
            position = answer["driver_id"].index(i)
            self.assertEqual(answer["trips"][position], i * 7 + 1)

    def test_a_newer_write_wins_and_an_older_one_does_not_move_it_backwards(self):
        """Feast's own write path, twice, out of order.

        The dedup read is ON here -- it is the default -- so this is the
        guarantee a late-arriving materialization relies on.
        """
        import pandas as pd

        store, _e, _v = self._repo("conf_order")
        base = datetime.now(timezone.utc) - timedelta(hours=1)

        def frame(trips, when):
            return pd.DataFrame(
                {
                    "driver_id": [7],
                    "trips": [trips],
                    "city": ["x"],
                    "ts": [when],
                    "created": [when],
                }
            )

        store.write_to_online_store("driver_stats", frame(100, base))
        store.write_to_online_store("driver_stats", frame(200, base + timedelta(minutes=30)))
        store.write_to_online_store("driver_stats", frame(300, base - timedelta(minutes=30)))

        answer = store.get_online_features(
            features=["driver_stats:trips"], entity_rows=[{"driver_id": 7}]
        ).to_dict()
        self.assertEqual(
            answer["trips"][0],
            200,
            "the newest event time must win, and the stale third write must not "
            "move the value backwards",
        )

    def test_teardown_runs_through_feasts_api_without_error(self):
        store, entity, view = self._repo("conf_teardown")
        store.write_to_online_store("driver_stats", self._rows(5))
        # With key tracking off -- the default -- teardown cannot enumerate and
        # logs that it cannot. What is asserted here is that Feast's teardown
        # completes rather than raising, which is what `feast teardown` needs.
        store.teardown()


def main() -> int:
    print(f"conformance against feast=={FEAST_VERSION_TESTED} (installed: ", end="")
    try:
        import feast

        print(f"{feast.__version__})")
    except Exception:
        print("absent)")
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    collected = suite.countTestCases()
    if collected == 0:
        print("FAIL: collected 0 tests", file=sys.stderr)
        return 1
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    # CLAMPED, because a class-level skip makes this NEGATIVE. When
    # `setUpClass` raises `SkipTest`, `testsRun` is 0 while `skipped` holds one
    # entry for the whole class, so the naive subtraction gives -1 -- and the
    # guard below, written as `executed == 0`, then did not fire and this
    # returned 0 for a run that verified nothing. Measured, not reasoned about:
    # it printed `executed -1` and exited 0.
    executed = max(0, result.testsRun - len(result.skipped))
    print(
        f"collected {collected}, executed {executed}, skipped {len(result.skipped)}"
    )
    # No floor here: unlike the offline suites, this one legitimately skips in
    # full when no node is reachable, and a floor would turn "no node" into a
    # failure. The CALLER decides whether a skipped conformance run is
    # acceptable; what this refuses to do is report it as a pass.
    if executed == 0 and collected > 0:
        print(
            "SKIPPED ENTIRELY: nothing was verified, so this is NOT a pass. "
            "Point TEMPORALSTORE_TEST_ADDR at an EMPTY shard, or let the "
            "creator start a node by leaving it unset.",
            file=sys.stderr,
        )
        return 2
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())

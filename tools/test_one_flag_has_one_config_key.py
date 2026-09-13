#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A flag is named once in the config file and once on the portal, and 38 of them differ.

Configuration is declared in several places. Two of them both give every flag a dotted key:

    tools/matrixark_load_config.py   ENV_MAP   config-file key -> environment variable   130 keys
    tools/matrixark_gateway_config.py SETTINGS  Setting(key, group, env, ...)             121 settings

They agree about the environment variable every time -- there is not one flag where the two name
different variables. What they disagree about is the KEY, which is what an operator types in
`config/temporalstore.toml` and reads on the portal:

    ENV_MAP keys with a Setting of the same key               28
    ENV_MAP keys whose env var is offered under ANOTHER key   38   <- this file
    ENV_MAP keys whose env var the portal does not offer      64

Of the 38, most are a taxonomy choice: the file groups by subsystem and the page groups by panel,
so `extraction.summary_refresh_limit` and `ingestion.summary_refresh_limit` are the same leaf under
two sections. Those are recorded but not argued with.

TWELVE differ in the LEAF NAME itself, which is harder to defend -- `limits.rl_ingest_rps` against
`limits.ingest_rps`, `retrieval.retrieval_min_score` against `retrieval.min_score`. An operator who
sets one and searches the page for the other does not find it.

NOTHING IS RENAMED HERE, deliberately. `matrixarkai/TemporalStore` is Apache-2.0 with 41 forks and
215 stars; a config key is a published interface, and renaming one breaks every deployment whose
file uses it. Adding an alias for each would grow the surface rather than shrink it. So this file
records the split exactly, in both directions, so it cannot grow quietly and so a future
consolidation starts from a measured list rather than a guess.

One of the twelve is NOT drift and is called out so it is not "fixed": the storage key says `oplog`
where the variable says `WAL`, and `config/temporalstore.toml` explains why on the same line --
`TS_INDEX_DUMP_OPLOG_GAP_BYTES` is the previous variable name and the engine still honours it. The
old word survives in the key on purpose.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_gateway_config as config_module

#: env var -> (config-file key, portal key), where the LEAF names differ.
LEAF_DIFFERS = {
    "MATRIXARK_EMBEDDING_MODEL": ("extraction.embedding_model", "embedding.model"),
    "MATRIXARK_QUOTA_MAX_BATCH": ("limits.quota_max_batch", "limits.max_batch"),
    "MATRIXARK_QUOTA_MAX_BLOB_BYTES": ("limits.quota_max_blob_bytes", "limits.max_blob_bytes"),
    "MATRIXARK_QUOTA_MAX_BODY_BYTES": ("limits.quota_max_body_bytes", "limits.max_body_bytes"),
    "MATRIXARK_RL_BLOB_STREAMS": ("limits.rl_blob_streams", "limits.blob_streams"),
    "MATRIXARK_RL_INGEST_BURST": ("limits.rl_ingest_burst", "limits.ingest_burst"),
    "MATRIXARK_RL_INGEST_RPS": ("limits.rl_ingest_rps", "limits.ingest_rps"),
    "MATRIXARK_RL_RETRIEVE_BURST": ("limits.rl_retrieve_burst", "limits.retrieve_burst"),
    "MATRIXARK_RL_RETRIEVE_RPS": ("limits.rl_retrieve_rps", "limits.retrieve_rps"),
    "MATRIXARK_RETRIEVAL_MIN_SCORE": ("retrieval.retrieval_min_score", "retrieval.min_score"),
    "MATRIXARK_SKILL_DISCOVERY": ("retrieval.skill_discovery", "skills.discovery"),
    "TS_INDEX_DUMP_WAL_GAP_BYTES": ("storage.index_dump_oplog_gap_bytes",
                                    "storage_engine.index_dump_wal_gap_bytes"),
}

#: How many differ only in the SECTION, the same leaf under two taxonomies.
SECTION_ONLY = 26


def _env_map():
    path = os.path.join(TOOLS, "matrixark_load_config.py")
    with open(path, encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        target = value = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        if target is None or getattr(target, "id", "") != "ENV_MAP":
            continue
        if not isinstance(value, ast.Dict):
            continue
        return {k.value: v.value for k, v in zip(value.keys, value.values)
                if isinstance(k, ast.Constant) and isinstance(v, ast.Constant)}
    return {}


def _split():
    """(leaf-differs, section-only) between ENV_MAP keys and portal keys."""
    env_map = _env_map()
    by_env = {s.env: s for s in config_module.SETTINGS if s.env}
    keys = {s.key for s in config_module.SETTINGS}
    leaf, section = {}, {}
    for key, env in env_map.items():
        if key in keys:
            continue
        setting = by_env.get(env)
        if setting is None:
            continue
        if key.split(".", 1)[-1] == setting.key.split(".", 1)[-1]:
            section[env] = (key, setting.key)
        else:
            leaf[env] = (key, setting.key)
    return leaf, section


class OneFlagHasOneConfigKey(unittest.TestCase):

    def test_both_registries_are_there_to_compare(self) -> None:
        """A floor. An empty ENV_MAP would make every assertion below pass over nothing."""
        env_map = _env_map()
        self.assertGreater(len(env_map), 100,
                           "ENV_MAP has %d entries; the scan has stopped reading it" % len(env_map))
        self.assertGreater(len([s for s in config_module.SETTINGS if s.env]), 100,
                           "the portal registry is nearly empty, so nothing is being compared")

    def test_the_two_registries_never_name_different_variables(self) -> None:
        """The half that is RIGHT, asserted so a regression is visible.

        A key meaning one variable in the file and another on the page would be worse than the
        naming split this file records, and today it does not happen once.
        """
        env_map = _env_map()
        by_key = {s.key: s for s in config_module.SETTINGS}
        for key, env in sorted(env_map.items()):
            setting = by_key.get(key)
            if setting is None or not setting.env:
                continue
            with self.subTest(key=key):
                self.assertEqual(
                    env, setting.env,
                    "%s maps to %s in the config file and %s on the portal" % (key, env, setting.env))

    def test_the_leaf_name_split_is_exactly_what_is_recorded(self) -> None:
        """Both directions: a new one fails, and a resolved one fails too."""
        leaf, _section = _split()
        self.assertEqual(
            sorted(LEAF_DIFFERS), sorted(leaf),
            "the set of flags whose config-file key and portal key have different leaf names "
            "changed. Renaming a config key is a published-interface change on a repo with forks, "
            "so a new entry here needs a decision rather than a tidy-up.")
        for env, pair in sorted(LEAF_DIFFERS.items()):
            with self.subTest(env=env):
                self.assertEqual(pair, leaf[env])

    def test_the_taxonomy_only_group_has_not_grown(self) -> None:
        """The larger, more defensible half: same leaf, two sections."""
        _leaf, section = _split()
        self.assertEqual(
            SECTION_ONLY, len(section),
            "the number of flags sharing a leaf name under two different sections is %d, recorded "
            "as %d" % (len(section), SECTION_ONLY))

    def test_the_storage_key_keeps_the_older_word_on_purpose(self) -> None:
        """Called out so it is not 'corrected'.

        The key says oplog where the variable says WAL because the previous variable name is still
        honoured by the engine, and the shipped config file says so on the same line.
        """
        path = os.path.join(os.path.dirname(TOOLS), "config", "temporalstore.toml")
        if not os.path.exists(path):  # pragma: no cover - config file absent in a partial checkout
            self.skipTest("config/temporalstore.toml is not in this checkout")
        with open(path, encoding="utf-8", errors="replace") as handle:
            body = handle.read()
        self.assertIn("index_dump_oplog_gap_bytes", body)
        self.assertIn(
            "TS_INDEX_DUMP_OPLOG_GAP_BYTES is the previous variable name", body,
            "the line explaining why the storage key keeps the older word is gone; without it the "
            "key reads as a stale rename and somebody will 'fix' it")


if __name__ == "__main__":
    unittest.main()

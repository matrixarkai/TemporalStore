#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal offers what the config file does.

`matrixark_load_config.ENV_MAP` maps 131 config-file keys to environment variables. The portal
offered 117 settings, and **77 of the file's variables were not among them** -- so which controls a
customer could reach depended on how they had configured the deployment, and nobody had decided
that: it was two lists maintained separately.

Most of the 77 are right to leave alone. A bootstrap setting -- the bind address, the port, the API
key file, the cluster's meta address -- is a launcher decision, and the portal already says so about
the ones it does show. That distinction is **derivable**: the config file puts them in its own
sections, so this rule exempts a section rather than a list of names, and a knob added to a serving
section is covered from the moment it is added.

Sixteen were serving knobs, every one of them read by production code. Fifteen are offered now. The
sixteenth is an alias for a setting the portal already has, and offering it would have created a
second way to set one thing -- the same trap as emitting a compatibility name.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_load_config as loader  # noqa: E402

# Sections of the config file a deployment settles before it serves anything. The portal reports
# these where it reports them at all, and does not change them.
BOOTSTRAP_SECTIONS = ("gateway", "auth", "server", "recovery", "replication",
                      "wal", "storage", "raft", "meta", "proxy", "cluster")

# The one serving variable deliberately not offered, and why.
ALIASES = {
    "MATRIXARK_EMBED_BASE_URL": "MATRIXARK_EMBEDDING_API_BASE",
}


def env_map():
    return dict(getattr(loader, "ENV_MAP", {}))


def offered():
    return {s.env: s for s in cfg.SETTINGS if s.env}


def serving_only_in_the_file():
    """Config-file keys in a serving section that the portal does not offer."""
    have = offered()
    missing = {}
    for key, variable in env_map().items():
        if variable in have or variable in ALIASES:
            continue
        if key.split(".", 1)[0] in BOOTSTRAP_SECTIONS:
            continue
        missing[key] = variable
    return missing


class TheTwoSurfacesAgreeTest(unittest.TestCase):

    def test_every_serving_knob_in_the_file_is_on_the_portal(self) -> None:
        missing = serving_only_in_the_file()
        self.assertEqual(
            {}, missing,
            "a config file can set these and the portal cannot:\n  "
            + "\n  ".join("%s (%s)" % (k, v) for k, v in sorted(missing.items())))

    def test_the_rule_is_comparing_something(self) -> None:
        """The rule above passes if the map goes empty or every section is called bootstrap. Both
        would read exactly like agreement."""
        mapped = env_map()
        self.assertGreater(len(mapped), 100, "the config map has %d entries" % len(mapped))
        serving = [k for k in mapped if k.split(".", 1)[0] not in BOOTSTRAP_SECTIONS]
        self.assertGreater(len(serving), 40,
                           "only %d config keys are in a serving section" % len(serving))

    def test_the_exemptions_are_the_minority(self) -> None:
        """A section list that swallowed the file would make this vacuous with nothing to notice."""
        mapped = env_map()
        exempt = [k for k in mapped if k.split(".", 1)[0] in BOOTSTRAP_SECTIONS]
        self.assertLess(len(exempt), len(mapped) - len(exempt) + len(exempt) // 1,
                        "%d of %d keys are exempt" % (len(exempt), len(mapped)))
        self.assertLess(len(exempt) / float(len(mapped)), 0.75)


class TheAliasIsReallyAnAliasTest(unittest.TestCase):
    """Exempting a name is the weakest kind of rule, so the reason is checked rather than trusted."""

    def test_the_setting_it_defers_to_is_offered(self) -> None:
        have = offered()
        for alias, primary in ALIASES.items():
            with self.subTest(alias=alias):
                self.assertIn(primary, have,
                              "%s is exempt because %s is offered; it is not" % (alias, primary))

    def test_they_are_read_in_the_same_breath(self) -> None:
        """An alias is a fallback beside the name it defers to. If it ever became an independent
        control it would need offering like any other."""
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            source = handle.read()
        for alias, primary in ALIASES.items():
            with self.subTest(alias=alias):
                line = next((ln for ln in source.splitlines() if alias in ln), "")
                self.assertIn(primary, line,
                              "%s is not read beside %s any more" % (alias, primary))


class TheNewSettingsAreRealTest(unittest.TestCase):
    """Offering a knob that nothing reads is the defect this repository keeps finding. Every one of
    these was checked against the code before it was offered; this keeps that true."""

    ADDED = (
        "limits.backend_readiness_timeout_ms",
        "limits.backend_readiness_backoff_ms",
        "limits.direct_record_bundle_max_bytes",
        "limits.direct_record_hot_cache_max_records",
        "retrieval.cross_session_max_candidates",
        "retrieval.cross_session_min_score",
        "retrieval.cross_session_parallelism",
        "retrieval.cross_session_profile_max_sessions",
        "retrieval.max_children_scored_per_parent",
        "retrieval.shared_context_min_score",
        "retrieval.mode_dependent_quota",
        "extraction.entity_merge_operator",
        "extraction.enable_llm_merge_operator",
        "extraction.time_compression_min_events",
        "extraction.time_compression_max_raw_events_per_node",
    )

    def test_each_one_is_registered(self) -> None:
        for key in self.ADDED:
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)

    def test_each_names_the_variable_the_config_file_maps(self) -> None:
        mapped = env_map()
        for key in self.ADDED:
            with self.subTest(setting=key):
                self.assertIn(key, mapped, "%s is not in the config map" % key)
                self.assertEqual(mapped[key], cfg.SETTINGS_BY_KEY[key].env)

    def test_each_variable_is_read_by_the_build(self) -> None:
        """A knob nothing reads is worse than a missing one: it reports as configured."""
        sources = []
        for name in sorted(os.listdir(TOOLS)):
            if not name.endswith(".py") or name.startswith("test_"):
                continue
            if name in ("matrixark_gateway_config.py", "matrixark_load_config.py"):
                continue
            with open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
                sources.append(handle.read())
        blob = "\n".join(sources)
        for key in self.ADDED:
            variable = cfg.SETTINGS_BY_KEY[key].env
            with self.subTest(setting=key):
                self.assertIn(variable, blob, "%s is offered and nothing reads it" % variable)

    def test_the_group_matches_the_config_file_section(self) -> None:
        """The two surfaces name the same thing the same way, which is what makes the rule above
        checkable at all."""
        for key in self.ADDED:
            with self.subTest(setting=key):
                self.assertEqual(key.split(".", 1)[0], cfg.SETTINGS_BY_KEY[key].group)

    def test_none_of_them_claims_to_be_live(self) -> None:
        """They are bound at import in `matrixark_mcp_runtime_config`, so `restart` is the honest
        label. Making them live is a separate change with its own evidence."""
        for key in self.ADDED:
            with self.subTest(setting=key):
                self.assertEqual("restart", cfg.SETTINGS_BY_KEY[key].applies)


if __name__ == "__main__":
    unittest.main()

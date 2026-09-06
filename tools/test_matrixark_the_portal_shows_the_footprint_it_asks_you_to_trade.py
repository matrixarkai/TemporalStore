#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal shows the footprint it asks you to trade.

Six settings ask an operator to trade memory against speed in their own words -- the page and block
index caches call it "a direct trade of footprint against lookup latency", `generate_embeddings`
prices itself at "~13% of resident memory", `share_repeated_values` quotes 761 MB against 1.1 GB --
and the portal reported no footprint at all. Nothing anywhere did: not the page, not the overview,
not `/v1/metrics`. A control whose stated unit is invisible on the same page is a decision the
customer is asked to make blind.

Two things here are easier to get wrong than to get right, so both are pinned:

* **A dash is not a zero.** An engine that publishes no footprint and an engine holding nothing
  differ only in what reaches the page, and "0.0 MB" is a number an operator can act on wrongly.
* **Worker figures do not add up.** Resident sets share pages, so summing four workers produces a
  figure larger than the machine is using. The panel names the worker count and offers no total.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_v1_gateway as gateway  # noqa: E402

PORTAL = os.path.join(TOOLS, "portal")
CRATE = os.path.abspath(os.path.join(TOOLS, os.pardir, "crates", "temporalstore-rust", "src"))

# The engine's own format, shard-labelled exactly as `prometheus_metrics.rs` emits it.
SAMPLE = "\n".join([
    "# HELP temporalstore_cache_bytes Cache bytes by shard and tier.",
    "# TYPE temporalstore_cache_bytes gauge",
    'temporalstore_cache_bytes{shard_id="1",tier="memory"} 12345',
    'temporalstore_cache_bytes{shard_id="2",tier="memory"} 23456',
    'temporalstore_cache_bytes{shard_id="1",tier="disk"} 999',
    'temporalstore_cache_bytes{shard_id="1",tier="compression_saved"} 4096',
    'temporalstore_storage_slot_bytes{shard_id="1",slot="0",kind="logical"} 1000000',
    'temporalstore_storage_slot_bytes{shard_id="1",slot="1",kind="logical"} 200000',
    'temporalstore_storage_slot_bytes{shard_id="1",slot="0",kind="physical"} 250000',
    'temporalstore_storage_slot_bytes{shard_id="1",slot="1",kind="physical"} 50000',
    'temporalstore_storage_backend{backend="shared",replication="none"} 1',
])

# The settings that name the trade. Derived from their own words, not listed by hand, so a setting
# that starts talking about memory joins the rule automatically.
TRADE_WORDS = ("resident memory", "trade of footprint", "peak memory", "hold more memory",
               "% of resident memory")


class CountingConfig:
    """A cfg whose connection factory records how often anything asked the engine."""

    def __init__(self) -> None:
        self.calls = 0

    def blob_connection_factory(self, _cfg):
        self.calls += 1
        raise OSError("no engine here")


class TheEngineFootprintIsParsedTest(unittest.TestCase):

    def test_cache_memory_is_summed_across_shards(self) -> None:
        found = gateway._engine_footprint(SAMPLE)
        self.assertTrue(found["available"])
        self.assertEqual(12345 + 23456, found["cache_memory_bytes"])

    def test_the_other_tiers_are_kept_apart(self) -> None:
        """Summing disk into memory would report resident memory the process is not holding, and
        the cache settings would then look far more expensive than they are."""
        found = gateway._engine_footprint(SAMPLE)
        self.assertEqual(999, found["cache_disk_bytes"])
        self.assertEqual(4096, found["cache_compression_saved_bytes"])
        for tier, value in (("disk", 999), ("compression_saved", 4096)):
            with self.subTest(tier=tier):
                self.assertNotEqual(12345 + 23456 + value, found["cache_memory_bytes"],
                                    "the %s tier was counted as memory" % tier)

    def test_the_store_is_summed_by_kind(self) -> None:
        found = gateway._engine_footprint(SAMPLE)
        self.assertEqual(1200000, found["store_logical_bytes"])
        self.assertEqual(300000, found["store_physical_bytes"])

    def test_the_ratio_is_the_compression_actually_achieved(self) -> None:
        """Logical and physical travel together because neither alone shows it."""
        self.assertEqual(4.0, gateway._engine_footprint(SAMPLE)["store_compression_ratio"])

    def test_a_malformed_line_is_skipped_not_fatal(self) -> None:
        broken = SAMPLE + "\n" + 'temporalstore_cache_bytes{shard_id="3",tier="memory"} bananas'
        self.assertEqual(12345 + 23456, gateway._engine_footprint(broken)["cache_memory_bytes"])


class AnUnpublishedFootprintIsNotAZeroTest(unittest.TestCase):
    """The case most likely to be got wrong, and the one an operator would act on."""

    def test_an_engine_publishing_none_of_it_says_so(self) -> None:
        for text in (None, "", "# nothing this build publishes"):
            with self.subTest(text=text):
                found = gateway._engine_footprint(text)
                self.assertFalse(found["available"])

    def test_and_reports_no_bytes_at_all_rather_than_zero_bytes(self) -> None:
        found = gateway._engine_footprint("# nothing this build publishes")
        for key in found:
            self.assertFalse(key.endswith("_bytes"),
                             "%s reported as a number when nothing published it" % key)

    def test_the_ratio_is_absent_when_it_would_be_meaningless(self) -> None:
        """A ratio against a zero denominator is not a compression figure, and 0.0 beside
        'compression' reads as 'none', which is a different claim."""
        only_logical = 'temporalstore_storage_slot_bytes{shard_id="1",slot="0",kind="logical"} 10'
        found = gateway._engine_footprint(only_logical)
        self.assertTrue(found["available"])
        self.assertNotIn("store_compression_ratio", found)

    def test_the_positive_control(self) -> None:
        """The floor: a parser that found nothing anywhere would satisfy every test above."""
        self.assertTrue(gateway._engine_footprint(SAMPLE)["available"])


class TheWorkerReportsItsOwnMemoryTest(unittest.TestCase):

    def test_it_reports_a_number_and_where_it_came_from(self) -> None:
        found = gateway._worker_resident()
        self.assertIn(found["source"], ("/proc/self/status", "ru_maxrss", "unavailable"))
        if found["source"] != "unavailable":
            self.assertGreater(found["peak_bytes"], 0)

    def test_the_units_are_bytes(self) -> None:
        """`ru_maxrss` is kilobytes on Linux and BYTES on macOS, so a unit slip is off by 1024x and
        looks entirely plausible. Cross-checking the two sources catches it: they measure nearly
        the same thing, so a factor of 1024 between them is not a rounding difference.
        """
        found = gateway._worker_resident()
        if found["source"] != "/proc/self/status":
            self.skipTest("no /proc on this platform, so there is nothing to cross-check against")
        import resource

        rusage_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.assertGreater(found["peak_bytes"], rusage_kb * 1024 / 4)
        self.assertLess(found["peak_bytes"], rusage_kb * 1024 * 4)

    def test_the_worker_count_travels_with_it(self) -> None:
        """Resident sets share pages. The count is what stops a reader summing them."""
        summary = gateway._footprint_summary(None, SAMPLE)
        self.assertIn("workers", summary["worker"])
        self.assertGreaterEqual(summary["worker"]["workers"], 1)


class TheEngineIsAskedOnceTest(unittest.TestCase):
    """The overview needs the backend AND the footprint out of one response."""

    def test_neither_reader_fetches_when_handed_the_text(self) -> None:
        counting = CountingConfig()
        gateway._probe_storage_backend(counting, SAMPLE)
        gateway._footprint_summary(counting, SAMPLE)
        self.assertEqual(0, counting.calls)

    def test_the_parameter_is_not_ignored(self) -> None:
        """The floor: a reader that always fetched would still pass the test above if the fetch
        were free, and one that never fetched would pass it while being useless on its own."""
        counting = CountingConfig()
        gateway._footprint_summary(counting)
        self.assertEqual(1, counting.calls)

    def test_the_backend_probe_still_reads_the_same_text(self) -> None:
        found = gateway._probe_storage_backend(CountingConfig(), SAMPLE)
        self.assertEqual("shared", (found or {}).get("backend"))

    def test_the_overview_hands_both_readers_one_response(self) -> None:
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            source = handle.read()
        block = source[source.index('path == "/v1/admin/overview"'):]
        block = block[:block.index('"config": config_snapshot')]
        self.assertIn("engine_metrics = _engine_metrics_text(cfg)", block)
        self.assertIn("_probe_storage_backend(cfg, engine_metrics)", block)
        self.assertIn("_footprint_summary(cfg, engine_metrics", block)


class TheSeriesAreTheOnesTheEngineEmitsTest(unittest.TestCase):
    """Ask the artefact. A renamed series would leave this panel blank on every deployment, which
    reads as a deployment holding nothing rather than as a parser looking for the wrong name."""

    def series_in_the_crate(self) -> set:
        if not os.path.isdir(CRATE):
            self.skipTest("the engine crate is not in this checkout")
        found = set()
        for root, _dirs, files in os.walk(CRATE):
            for name in files:
                if not name.endswith(".rs"):
                    continue
                with open(os.path.join(root, name), encoding="utf-8", errors="replace") as handle:
                    found.update(re.findall(r"temporalstore_[a-z_]+", handle.read()))
        return found

    def test_every_series_this_parses_is_one_the_engine_emits(self) -> None:
        emitted = self.series_in_the_crate()
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            parsed = set(re.findall(r'"(temporalstore_[a-z_]+)"', handle.read()))
        self.assertTrue(parsed, "the gateway names no engine series at all")
        for name in sorted(parsed):
            with self.subTest(series=name):
                self.assertIn(name, emitted)

    def test_the_two_this_panel_needs_are_among_them(self) -> None:
        """The floor: the rule above passes on an empty intersection if the panel stopped naming
        any series, which is exactly what a silent rename would look like."""
        emitted = self.series_in_the_crate()
        for name in ("temporalstore_cache_bytes", "temporalstore_storage_slot_bytes"):
            self.assertIn(name, emitted)


class TheScrapeCarriesItTooTest(unittest.TestCase):
    """A figure that exists only on a page cannot be alerted on or watched over time, which is
    most of what a footprint is for."""

    def test_the_metrics_route_exports_the_worker_gauge(self) -> None:
        with open(os.path.join(TOOLS, "matrixark_v1_gateway.py"), encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("matrixark_gateway_worker_resident_bytes", source)
        self.assertIn("matrixark_gateway_workers", source)


class ThePageDrawsItTest(unittest.TestCase):
    """The renderer exists whether or not anything calls it, and a figure in the wrong cell looks
    the same in a diff as one in the right cell. So the page is run."""

    def run_harness(self, mode: str) -> str:
        node = subprocess.run(
            ["node", "footprint_panel_harness.js", "setup_portal.html", mode],
            cwd=PORTAL, capture_output=True, text=True, timeout=600)
        return node.stdout + node.stderr

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_a_published_footprint_is_drawn(self) -> None:
        out = self.run_harness("published")
        self.assertIn("all ok", out, out)

    def test_an_absent_one_is_a_dash(self) -> None:
        out = self.run_harness("absent")
        self.assertIn("all ok", out, out)

    def test_several_workers_are_named_and_not_summed(self) -> None:
        out = self.run_harness("multiworker")
        self.assertIn("all ok", out, out)


class TheSettingsThatAskForTheTradeTest(unittest.TestCase):
    """Why this panel exists, kept honest: if no setting asked for the trade any more, the panel
    would be decoration and this suite should say so rather than passing quietly."""

    def settings_naming_memory(self):
        return [s for s in cfg.SETTINGS
                if any(word in (s.label + " " + s.help).lower() for word in TRADE_WORDS)]

    def test_several_settings_still_price_themselves_in_memory(self) -> None:
        found = self.settings_naming_memory()
        self.assertGreaterEqual(len(found), 3,
                                "only %d settings name the trade: %s"
                                % (len(found), [s.key for s in found]))

    def test_the_index_caches_are_among_them(self) -> None:
        keys = {s.key for s in self.settings_naming_memory()}
        self.assertIn("storage_engine.page_index_cache_bytes", keys)


if __name__ == "__main__":
    unittest.main()

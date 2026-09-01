#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The live status strip: on every page, fed by one stream.

Status used to be per-page. The stream carried the encoding backlog, the running import, traffic and
warnings, and two of seven pages subscribed -- so "is my upload done" depended on being on the right
page, and the page you upload from was not one of them.

The risk in fixing that is a second connection per page. The gateway holds a task per stream, so a
page opening two costs twice the server work to show one strip, and nothing on screen would say so.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg, drive  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(HERE, "portal")

PAGES = ("/v1/admin", "/v1/admin/setup", "/v1/admin/catalog", "/v1/admin/explore",
         "/v1/admin/ingestion", "/v1/admin/portal", "/v1/admin/api")


def _read(name: str) -> str:
    with open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        return handle.read()


def _html_files() -> list:
    return sorted(f for f in os.listdir(PORTAL) if f.endswith(".html"))


class StripOnEveryPageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = gw.make_v1_app(_FakeServer(), _cfg())

    def test_every_served_page_carries_the_strip(self) -> None:
        # Including the two hand-maintained pages, which get it through the same nav injection as
        # the links -- otherwise the pages that predate the generator quietly lack it.
        for path in PAGES:
            with self.subTest(path=path):
                _st, _h, body = drive(self.app, method="GET", path=path)
                text = body.decode("utf-8")
                self.assertIn('id="liveStrip"', text)
                self.assertIn("The shared live strip", text)

    def test_the_strip_appears_exactly_once_per_page(self) -> None:
        # The builder runs repeatedly over the hand-maintained pages. A script appended rather than
        # replaced would stack, and each copy would open its own stream.
        for name in _html_files():
            with self.subTest(page=name):
                text = _read(name)
                self.assertEqual(1, text.count('id="liveStrip"'))
                self.assertEqual(1, text.count("The shared live strip"))

    def test_rebuilding_does_not_stack_copies(self) -> None:
        # Asserted against the mechanism rather than the current output: the helper must replace.
        source = _read(os.path.join("build_portal_pages.py"))
        self.assertIn("def _with_nav_js", source)
        block = source[source.index("def _with_nav_js"):source.index("def inject(")]
        self.assertIn("NAV_JS_MARKER in text", block)


class OneConnectionPerPageTest(unittest.TestCase):
    """A page must open at most one stream."""

    def test_the_strip_stands_down_when_the_page_runs_its_own_stream(self) -> None:
        source = _read(os.path.join("build_portal_pages.py"))
        start = source.index("NAV_JS = r'''")
        script = source[start:source.index("'''", start + 20)]
        # The guard, and that it precedes the connect.
        self.assertIn("if (window.__matrixarkLive) { return; }", script)
        self.assertLess(script.index("if (window.__matrixarkLive) { return; }"),
                        script.index("fetch(\"/v1/admin/events\""),
                        "the strip connects before checking whether the page already has a stream")

    def test_a_page_with_its_own_stream_claims_it_and_feeds_the_strip(self) -> None:
        # Claiming without feeding would leave the strip permanently blank on exactly the two pages
        # that have the data already.
        source = _read(os.path.join("build_portal_pages.py"))
        self.assertEqual(2, source.count('window.__matrixarkLive = "page"'))
        self.assertEqual(2, source.count("window.__matrixarkLiveFrame(frame)"))

    def test_the_claim_is_synchronous(self) -> None:
        # The page's script runs before the strip's, but its stream starts only once a fetch
        # resolves. Claiming there would be too late and both would connect.
        source = _read(os.path.join("build_portal_pages.py"))
        for index in range(2):
            claim = source.index('window.__matrixarkLive = "page"',
                                 0 if index == 0 else claim + 1)
            head = source.rindex("(function () {", 0, claim)
            between = source[head:claim]
            with self.subTest(occurrence=index):
                self.assertNotIn("function startLive", between,
                                 "the claim sits inside a function that runs later")

    def test_each_page_serves_one_strip_script(self) -> None:
        for name in _html_files():
            with self.subTest(page=name):
                self.assertEqual(1, _read(name).count("window.__matrixarkLive = \"strip\""))


class StripHonestyTest(unittest.TestCase):
    """What it says when it does not know."""

    def setUp(self) -> None:
        source = _read(os.path.join("build_portal_pages.py"))
        start = source.index("NAV_JS = r'''")
        self.script = source[start:source.index("'''", start + 20)]

    def test_an_unaskable_backend_reads_as_unknown_not_as_an_empty_backlog(self) -> None:
        # The frame omits `embedding` when the backend could not be asked. Rendering that as
        # "0 waiting" is the reassuring answer to a question nobody managed to ask.
        self.assertIn('encoding <b>unknown</b>', self.script)

    def test_nothing_is_drawn_before_the_first_frame(self) -> None:
        # A strip showing zeros before it has heard anything describes a quiet healthy deployment
        # for one that is unreachable.
        page = _read("catalog_portal.html")
        self.assertIn('id="liveStrip" hidden', page)
        self.assertIn("strip.hidden = false", self.script)

    def test_a_hidden_tab_drops_its_connection(self) -> None:
        # The gateway holds a task per stream; a background tab holding one open costs the server
        # for nobody.
        self.assertIn("visibilitychange", self.script)
        self.assertIn("controller.abort()", self.script)

    def test_it_backs_off_rather_than_hammering_a_gateway_that_is_down(self) -> None:
        self.assertIn("Math.min(backoff * 2, 30000)", self.script)

    def test_it_stays_quiet_without_a_key(self) -> None:
        # Every page is inert without one; the strip must not be the thing that 401s in a loop.
        self.assertIn("if (!k) { setTimeout(open, 2000); return; }", self.script)


class StripLinksTest(unittest.TestCase):
    def test_every_segment_links_to_a_page_that_exists(self) -> None:
        # The strip doubles as a way in: a number you cannot click is a number you have to go and
        # find. A link to a page that does not exist is worse than no link.
        source = _read(os.path.join("build_portal_pages.py"))
        strip = source[source.index("LIVE_STRIP = "):source.index("def nav(active)")]
        hrefs = set()
        for chunk in strip.split('href="')[1:]:
            hrefs.add(chunk.split('"')[0].split("#")[0])
        app = gw.make_v1_app(_FakeServer(), _cfg())
        for href in sorted(hrefs):
            with self.subTest(href=href):
                status, _h, _b = drive(app, method="GET", path=href)
                self.assertEqual(200, status)


class EmbeddingCadenceTest(unittest.TestCase):
    """The backlog is read at the pace it is changing."""

    def test_a_draining_store_is_read_often_enough_to_be_seen_moving(self) -> None:
        # A figure that sits still for half a minute while it is actually falling reads as stuck,
        # which is the opposite of what a live view is for.
        fast = gw._embedding_refresh_interval({"pending": 120, "deferred_tasks": 0})
        self.assertLessEqual(fast, 5.0)

    def test_an_idle_store_is_not_re_derived_every_few_seconds(self) -> None:
        # Answering walks the record log. For a store with nothing pending the figure is a
        # constant, and every connected browser would be paying for it.
        idle = gw._embedding_refresh_interval({"pending": 0, "deferred_tasks": 0})
        self.assertGreaterEqual(idle, 30.0)

    def test_deferred_work_counts_as_draining(self) -> None:
        # Nothing is "pending" while a deferred task has yet to produce it, and a customer watching
        # is watching the same thing either way.
        self.assertLessEqual(
            gw._embedding_refresh_interval({"pending": 0, "deferred_tasks": 3}), 5.0)

    def test_unknown_is_read_again_soon_rather_than_left_for_half_a_minute(self) -> None:
        # The frame omits `embedding` when the backend could not be asked. That is the case where
        # the next answer matters most.
        self.assertLessEqual(gw._embedding_refresh_interval(None), 5.0)

    def test_the_fast_pace_is_slower_than_the_frame_tick(self) -> None:
        # Refreshing more often than frames are sent would do the expensive read for frames nobody
        # receives.
        self.assertGreaterEqual(gw.EVENT_EMBEDDING_REFRESH_DRAINING_S, gw.EVENT_TICK_S)


class StripRenderTest(unittest.TestCase):
    """The shipped script, run against real frames.

    Every other test here asserts that the source SAYS the right thing. A strip that contains the
    word "unknown" and never reaches that branch greps identically and is a different product. This
    runs it, in the page as shipped, through a DOM stub -- five elements and four properties, which
    is all it touches, and cheaper to keep working than a dependency would be.
    """

    @classmethod
    def setUpClass(cls) -> None:
        import json
        import shutil
        import subprocess

        node = shutil.which("node")
        if not node:
            raise unittest.SkipTest("node is not available to run the page's own script")
        harness = os.path.join(PORTAL, "live_strip_harness.js")
        page = os.path.join(PORTAL, "catalog_portal.html")
        result = subprocess.run([node, harness, page], capture_output=True, text=True,
                                timeout=60)
        if result.returncode != 0:
            raise AssertionError("the strip script would not run: " + result.stderr[:800])
        cls.out = json.loads(result.stdout)

    def test_nothing_is_shown_before_a_frame_arrives(self) -> None:
        self.assertTrue(self.out["before_any_frame"]["stripHidden"])

    def test_a_draining_backlog_says_how_many_are_waiting(self) -> None:
        state = self.out["draining"]
        self.assertIn("652", state["enc"])
        self.assertIn("waiting to embed", state["enc"])
        self.assertIn("busy", state["encClass"])

    def test_a_running_import_says_how_far_along_it_is(self) -> None:
        state = self.out["draining"]
        self.assertIn("137", state["imp"])
        self.assertIn("400", state["imp"])

    def test_an_idle_store_reports_the_total_rather_than_a_backlog(self) -> None:
        state = self.out["all_encoded_idle"]
        self.assertIn("embedded", state["enc"])
        self.assertNotIn("waiting", state["enc"])
        self.assertIsNone(state["imp"], "an idle deployment showed an import segment")

    def test_an_unreachable_backend_reads_as_unknown_not_as_zero_waiting(self) -> None:
        # The frame omits `embedding` entirely when the backend could not be asked. Reporting
        # "0 waiting" would be the reassuring answer to a question nobody managed to ask.
        state = self.out["backend_unreachable"]
        self.assertIn("unknown", state["enc"])
        self.assertIn("warn", state["encClass"])

    def test_an_empty_store_is_not_confused_with_an_unreachable_one(self) -> None:
        # Both have nothing to report about a backlog and they mean opposite things.
        self.assertIn("0", self.out["empty_store"]["enc"])
        self.assertNotIn("unknown", self.out["empty_store"]["enc"])

    def test_failures_waiting_for_a_retry_are_surfaced(self) -> None:
        # Otherwise they are visible only to whoever opens the Ingestion page.
        state = self.out["failures_waiting"]
        self.assertIn("7", state["imp"])
        self.assertIn("retry", state["imp"])
        self.assertIn("warn", state["impClass"])

    def test_errors_colour_the_traffic_segment(self) -> None:
        self.assertIn("warn", self.out["backend_unreachable"]["reqClass"])
        self.assertNotIn("warn", self.out["draining"]["reqClass"])

    def test_counts_read_as_english(self) -> None:
        # The singular case is the store with exactly one request, not the one with three.
        self.assertEqual("<b>1</b> request", self.out["empty_store"]["req"])
        self.assertIn("<b>3</b> requests", self.out["backend_unreachable"]["req"])
        self.assertIn("<b>1</b> warning", self.out["failures_waiting"]["warn"])
        self.assertNotIn("warnings", self.out["failures_waiting"]["warn"])
        self.assertIn("5,120", self.out["draining"]["req"])

    def test_a_hidden_segment_does_not_keep_a_stale_colour(self) -> None:
        # It would come back still coloured for a state that has passed.
        self.assertNotIn("warn", self.out["all_encoded_idle"]["impClass"])
        self.assertNotIn("busy", self.out["all_encoded_idle"]["impClass"])


if __name__ == "__main__":
    unittest.main()

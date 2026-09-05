#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal says when the gateway accepts anonymous requests.

Authentication is off out of the box, deliberately, so the API works with no configuration. The
gateway says so once, at startup, in the log::

    MatrixArk gateway is running WITHOUT authentication (dev default). Anyone who can reach this
    address has full anonymous access and there is NO tenant isolation.

Nobody reading the portal sees a log line. The configuration snapshot carries a ``warnings`` list
whose purpose, in its own docstring, is *"the difference between a misconfigured deployment that
looks fine and one an operator can see is misconfigured"* -- and it covered extraction and embedding
degrading to a local path, but not the deployment being open to anyone who can reach it. That is the
most consequential state a deployment can be in and the only one the panel did not mention.

The strip's warning count is the length of that list, so this reaches all seven pages, and its
segment already links to the panel listing them.

**It says nothing when it does not know.** The snapshot has no ``GatewayConfig``, and ``require_auth``
can be set by the config dict as well as the environment, so the posture is recorded where it is
already decided -- at startup, by the function that decides whether to log the warning. A process
that never asked reports nothing: "we did not check" must not read as "you are safe".
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402


def warnings_now():
    return gw._model_config_snapshot().get("warnings") or []


def about_auth(warnings):
    return [w for w in warnings if "anonymous" in w]


class _PostureTest(unittest.TestCase):

    def setUp(self) -> None:
        previous = dict(gw._AUTH_POSTURE)
        warned = dict(gw._AUTH_WARNED)

        def restore() -> None:
            gw._AUTH_POSTURE.clear()
            gw._AUTH_POSTURE.update(previous)
            gw._AUTH_WARNED.clear()
            gw._AUTH_WARNED.update(warned)

        self.addCleanup(restore)
        gw._AUTH_POSTURE.clear()
        gw._AUTH_WARNED["done"] = False

    def observe(self, require_auth: bool) -> None:
        """What the gateway does at startup, with the posture this deployment has."""
        from test_matrixark_v1_gateway import _cfg
        gw._warn_if_auth_disabled(_cfg(require_auth=require_auth))


class AnOpenGatewaySaysSoTest(_PostureTest):

    def test_the_warnings_list_is_reachable_at_all(self) -> None:
        """A floor: every assertion below is about this list's contents."""
        self.observe(require_auth=True)
        self.assertIsInstance(warnings_now(), list)

    def test_an_open_gateway_is_named_in_the_warnings(self) -> None:
        self.observe(require_auth=False)
        found = about_auth(warnings_now())
        self.assertEqual(1, len(found), warnings_now())
        self.assertIn("no tenant isolation", found[0])

    def test_it_says_what_to_do_about_it(self) -> None:
        """A warning a reader cannot act on is a warning they learn to scroll past."""
        self.observe(require_auth=False)
        said = about_auth(warnings_now())[0]
        self.assertIn("MATRIXARK_REQUIRE_AUTH=1", said)
        self.assertIn("MATRIXARK_ACCESS_MODE=enforced", said)
        self.assertIn("restart", said)

    def test_a_closed_gateway_is_not_warned_about(self) -> None:
        """The other half. A warning that is always there is one nobody reads."""
        self.observe(require_auth=True)
        self.assertEqual([], about_auth(warnings_now()), warnings_now())

    def test_a_process_that_never_asked_says_nothing(self) -> None:
        """Absence of evidence. The snapshot has no config of its own, and guessing from the
        environment would be wrong for a deployment configured by the config dict -- and a wrong
        guess here reads as "you are safe"."""
        gw._AUTH_POSTURE.clear()
        self.assertEqual([], about_auth(warnings_now()), warnings_now())


class TheCountThatRidesEveryPageTest(_PostureTest):

    def test_the_strip_counts_it(self) -> None:
        """The number in the strip is the length of this list, so an open deployment is visible
        from every page rather than only from the one panel that lists them."""
        self.observe(require_auth=True)
        gw._LIVE_SHARED = None
        closed = gw._shared_live_parts()["warnings"]
        self.observe(require_auth=False)
        gw._LIVE_SHARED = None
        open_ = gw._shared_live_parts()["warnings"]
        self.addCleanup(setattr, gw, "_LIVE_SHARED", None)
        self.assertEqual(closed + 1, open_,
                         "the strip's count did not move when the gateway turned out to be open")


class TheStartupWarningStillWorksTest(_PostureTest):

    def test_it_still_logs_once(self) -> None:
        """Recording the posture must not cost the log line, nor make it repeat."""
        import logging
        records = []

        class Catch(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        handler = Catch()
        gw._LOG.addHandler(handler)
        self.addCleanup(gw._LOG.removeHandler, handler)

        self.observe(require_auth=False)
        self.observe(require_auth=False)
        said = [r for r in records if "WITHOUT authentication" in r]
        self.assertEqual(1, len(said), records)

    def test_it_stays_quiet_when_auth_is_on(self) -> None:
        import logging
        records = []

        class Catch(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        handler = Catch()
        gw._LOG.addHandler(handler)
        self.addCleanup(gw._LOG.removeHandler, handler)

        self.observe(require_auth=True)
        self.assertEqual([], [r for r in records if "WITHOUT authentication" in r])

    def test_the_posture_is_recorded_either_way(self) -> None:
        """Recorded in both branches: a process that checked and found auth ON is evidence too, and
        without it the closed case would be indistinguishable from never having asked."""
        self.observe(require_auth=True)
        self.assertIs(True, gw._AUTH_POSTURE.get("require_auth"))


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A fault in the page is not reported as a fault in the deployment.

Twenty-one places answered a failed call with "Could not reach the gateway." Three different
things arrive at those lines:

* a rejection carrying a **status** -- the deployment answered, and said no;
* a rejection from **fetch itself** -- the request never left;
* a **throw after the answer arrived** -- this page failing to show what it was given.

Ten sites told the first apart. None told the third apart from the second, and the third is the
one that cost two sessions an hour: a helper defined in the wrong script block raised a
ReferenceError inside a render, and the screen reported a gateway that had just answered as
unreachable. Seven more sites were written ``.catch(function () {``, discarding the error, so even
a status they held was reported as unreachable.

Three were worse again. They handle 401 and 403 by name and treat everything else as unreachable,
so a render throw did not merely print the wrong sentence -- it turned the **live strip** to
"gateway unreachable" about a deployment that was answering. The strip is what a reader glances at
to decide whether the deployment is alive.

The classifier is RUN here, not read. What it answers is the whole change; a copy of its rules in
this file could drift from the page without a word.
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
HARNESS = os.path.join(PORTAL, "why_failed_harness.js")
UNREACHABLE = "Could not reach the gateway."


def pages() -> list:
    return sorted(f for f in os.listdir(PORTAL) if f.endswith("_portal.html"))


def read(name: str) -> str:
    with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        return handle.read()


class TheClassifierAnswersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            raise unittest.SkipTest("node is not available")
        out = subprocess.run(["node", HARNESS, os.path.join(PORTAL, "overview_portal.html")],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            raise AssertionError(out.stderr[-900:])
        cls.result = json.loads(out.stdout)

    def said(self, case: str) -> str:
        return self.result["said"][case]

    def test_a_status_is_the_deployment_answering(self) -> None:
        """A mapped status is answered with its sentence, which carries no digits -- a first
        draft looked for "503" in it and failed on a classifier that was working."""
        said = self.said("a_status_the_deployment_answered")
        self.assertNotIn(UNREACHABLE, said)
        self.assertIn("not ready to serve", said)
        self.assertNotIn("could not show it", said, "a status is not this page's fault")

    def test_a_page_may_still_word_a_status_its_own_way(self) -> None:
        """The upload panel says "that file is larger" rather than "that request is larger". The
        classifier must not take that away from it."""
        self.assertIn("file", self.said("a_status_with_an_override"))

    def test_a_request_that_never_left_says_so(self) -> None:
        """All three browsers word it differently, and none of them is this page's fault."""
        for case in ("the_request_never_left", "the_request_never_left_firefox",
                     "the_request_never_left_safari", "nothing_at_all"):
            with self.subTest(case=case):
                self.assertEqual(UNREACHABLE, self.said(case))

    def test_a_throw_while_showing_the_answer_does_not_blame_the_gateway(self) -> None:
        """The one that was indistinguishable, and the exact throw that made it matter."""
        said = self.said("the_page_threw_while_showing")
        self.assertNotIn(UNREACHABLE, said)
        self.assertIn("could not show it", said)
        self.assertIn("__matrixarkWhen", said, "the message it caught is not repeated")
        self.assertIn("not in the deployment", said)

    def test_an_unreadable_answer_is_not_an_unreachable_one(self) -> None:
        said = self.said("a_bad_body")
        self.assertNotIn(UNREACHABLE, said)
        self.assertIn("JSON", said)

    def test_the_predicate_and_the_sentence_agree(self) -> None:
        """They are asked separately -- by the message and by the live strip -- so they have to
        come from one rule, or the page can say the gateway is down while explaining that it is
        not."""
        arrived = self.result["neverArrived"]
        self.assertFalse(arrived["a_status"])
        self.assertFalse(arrived["a_render_throw"])
        self.assertTrue(arrived["fetch_failure"])
        self.assertTrue(arrived["nothing_at_all"])


class NoPageSaysItItselfTest(unittest.TestCase):

    def test_the_sentence_is_written_in_one_place(self) -> None:
        """Every page carries the shared block, so the sentence appears once per page for that.

        A second copy on a page is a catch answering for itself, which is how the wording got out
        of step with what actually happened in the first place.
        """
        for name in pages():
            text = read(name)
            extra = text.count(UNREACHABLE) - text.count("return \"" + UNREACHABLE + "\"")
            # An XHR's onerror IS a network failure and says so correctly; it is the one caller
            # that already knows which of the three it has.
            extra -= len(re.findall(r"onerror = function[^}]*?" + re.escape(UNREACHABLE),
                                    text, re.S))
            self.assertEqual(0, extra,
                             "%s writes the sentence itself %d times" % (name, extra))

    def test_the_strip_only_calls_it_down_when_it_never_arrived(self) -> None:
        """The live strip is the page's loudest claim. It must not make it about a render fault."""
        for name in pages():
            text = read(name)
            for match in re.finditer(r'conn\(\s*"down"', text):
                before = text[max(0, match.start() - 260):match.start()]
                # The stream's own retry state is the exception, and a real one: "reconnecting in
                # 4s" is reported by the strip client when the socket is genuinely gone, with no
                # request and no answer to classify.
                if ".catch(" not in before:
                    continue
                self.assertIn(
                    "__matrixarkNeverArrived", text[match.start() - 260:match.start() + 260],
                    "%s sets the strip to down from a catch without asking whether the request "
                    "ever arrived" % name)

    def test_the_pages_actually_use_the_classifier(self) -> None:
        """The positive control. Every assertion above passes on a portal that reports nothing at
        all, which is exactly what deleting the catches would leave behind."""
        used = sum(read(name).count("__matrixarkWhyFailed(") for name in pages())
        self.assertGreaterEqual(used, 20, "the classifier is called %d times" % used)


if __name__ == "__main__":
    unittest.main()

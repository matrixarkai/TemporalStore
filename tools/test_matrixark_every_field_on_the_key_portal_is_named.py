#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every field on the key portal is announced by its own label.

The page carried seventeen labels in its markup and not one of them was attached to a control --
the only portal page where that was true, while the others bind most of theirs.

Read off the page's own accessibility tree, that meant the admin key field announced itself as
"sk_live_... / mk_... admin-scoped key". With no label attached the browser falls back to the
placeholder, so the field that takes a credential was described by an example of one. Worse, this
page names two fields "Tenant ID" and two "Account ID" -- one pair for the connection, one for the
key being created -- so unbound they were four controls carrying two names between them with
nothing to tell them apart. And clicking a label focused nothing, which costs more here than
elsewhere because these labels carry the explanatory text and so are wide, obviously clickable
targets.

Three ways a control can be named, all accepted here:

  * a label with `for` naming it,
  * a label that wraps it, which associates it without an attribute,
  * for a set of controls rather than one, `role="group"` with `aria-labelledby` -- `for` takes a
    single id, so the preset buttons and the scope checkboxes are named that way.

The count of controls checked is asserted, because a selector that stopped matching would leave
every check below quantified over nothing and passing.
"""
from __future__ import annotations

import io
import os
import unittest
from html.parser import HTMLParser

TOOLS = os.path.dirname(os.path.abspath(__file__))
PAGE = os.path.join(TOOLS, "portal", "api_key_portal.html")

CONTROLS = {"input", "select", "textarea"}
# Static controls carrying an id today. A floor, so an empty match cannot pass as success.
CONTROL_COUNT = 15


class Markup(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.label_depth = None
        self.labels = []          # (for, id, wrapped-control-ids)
        self.controls = {}        # id -> tag
        self.labelled_by = set()  # ids named in an aria-labelledby
        self._wrapped = []

    def handle_starttag(self, tag, attrs) -> None:
        d = dict(attrs)
        if d.get("aria-labelledby"):
            self.labelled_by.update(d["aria-labelledby"].split())
        if tag in CONTROLS:
            if d.get("id"):
                self.controls[d["id"]] = tag
            if self.label_depth is not None and d.get("id"):
                self._wrapped.append(d["id"])
        if tag == "label":
            self.label_depth = self.depth
            self._wrapped = []
            self._for = d.get("for")
            self._id = d.get("id")
        if tag not in {"input", "br", "img", "meta", "link", "hr"}:
            self.depth += 1

    def handle_endtag(self, tag) -> None:
        if tag not in {"input", "br", "img", "meta", "link", "hr"}:
            self.depth -= 1
        if tag == "label" and self.label_depth is not None:
            self.labels.append((self._for, self._id, list(self._wrapped)))
            self.label_depth = None


def markup() -> Markup:
    with io.open(PAGE, encoding="utf-8") as handle:
        body = handle.read().split("<script>")[0]
    parsed = Markup()
    parsed.feed(body)
    return parsed


class EveryFieldIsNamedTest(unittest.TestCase):

    def setUp(self) -> None:
        self.page = markup()

    def test_the_page_still_has_the_fields_these_checks_are_about(self) -> None:
        self.assertGreaterEqual(len(self.page.controls), CONTROL_COUNT,
                                "expected at least %d controls with ids, found %r"
                                % (CONTROL_COUNT, sorted(self.page.controls)))

    def test_every_control_is_named_by_a_label(self) -> None:
        named = set()
        for target, _, wrapped in self.page.labels:
            if target:
                named.add(target)
            named.update(wrapped)
        unnamed = sorted(set(self.page.controls) - named - self.page.labelled_by)
        self.assertEqual([], unnamed,
                         "these fields have no label attached, so a screen reader falls back to "
                         "the placeholder and clicking the label focuses nothing: %r" % unnamed)

    def test_no_label_points_at_a_field_that_does_not_exist(self) -> None:
        missing = sorted(t for t, _, _ in self.page.labels
                         if t and t not in self.page.controls)
        self.assertEqual([], missing, "labels naming absent controls: %r" % missing)

    def test_no_two_labels_claim_the_same_field(self) -> None:
        targets = [t for t, _, _ in self.page.labels if t]
        duplicates = sorted({t for t in targets if targets.count(t) > 1})
        self.assertEqual([], duplicates, "more than one label claims: %r" % duplicates)

    def test_every_label_does_a_job(self) -> None:
        """A label attached to nothing is decoration that announces itself as a label."""
        idle = [(t, i) for t, i, wrapped in self.page.labels
                if not t and not wrapped and (i is None or i not in self.page.labelled_by)]
        self.assertEqual([], idle, "labels attached to nothing: %r" % idle)

    def test_the_two_repeated_names_are_told_apart(self) -> None:
        """Tenant ID and Account ID each appear twice; each pair must reach different fields."""
        for pair in (("ctxTenant", "ckTenant"), ("ctxAccount", "ckAccount")):
            targets = [t for t, _, _ in self.page.labels if t in pair]
            self.assertEqual(sorted(pair), sorted(targets),
                             "%r are not separately labelled, so they announce identically" % (pair,))


if __name__ == "__main__":
    unittest.main()

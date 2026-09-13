#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A field on the operator page is held by something, or it is named here.

The page is not a list of every variable. `test_an_addressed_flag_is_offered` says so from the
other side -- *"the property is not that every flag is on the portal; most flags are internal and
should stay that way"* -- but nothing asked the question in this direction, so the page could grow
a field that no document, no test, no config file and no operator ever asked for, and the only way
to find out was to remove one and see what broke.

Six things hold a field. A field held by none of them is not automatically wrong: `UNHELD` records
each one with the reason it stays, and the set is asserted EXACTLY, so a new unheld field fails
here and one that becomes held fails too.

MEASURED AGAIN BEFORE MERGING, because the numbers below were taken when the page was larger and
this file sat unmerged while three cuts landed:

    fields carrying an env name                             140
    the shipped config file sets it in a serving section     57   <- the hard floor
    a test names its portal key                              67
    a hand-written document names it                         37
    it is default-ON and the page is the only off-selector   26
    its help carries a measured recommendation               15
    its read carries advice addressed to an operator          1
    held by at least one                                    131
    unheld                                                    9   <- every one recorded below

The addressed-reader criterion holds ONE field. That is not a broken criterion -- it is the
corrected one, and the correction is the point: read loosely it held 121, because it was asking
whether the flag is read in production at all rather than whether the READ carries advice. A
criterion holding one field is doing less work than its floor suggests, and it is kept because the
loose version is the trap this file exists to record.

## What each criterion is careful about, and why

Five of the six were written loosely first, and each loose reading held most of the page -- which
measures the population, not the property. The corrections are the content of this file:

* **A test naming the ENV VAR does not hold a field.** Removing a `Setting` removes the OFFERING,
  never the variable: every reader keeps working and the env var keeps being read. Only a test
  naming the portal KEY is a test about the offering. 91 becomes 67.
* **A GENERATED document does not hold a field.** `docs/ops/temporalstore-engine-flags.md` lists
  every variable the engine reads, from the source. A machine listing naming a variable is not
  documentation of an operator control. 52 becomes 37.
* **A default-ON switch is held only when the page is the ONLY off-selector.** That is the rule
  `test_python_flag_switches.test_no_new_switch_is_unreachable` enforces; a switch a test or a
  launch profile can already turn off does not need the page to be reachable. 31 becomes 27.
* **"Its reader addresses an operator" is about the COMMENT above the read**, not about being read
  at all. Read-at-all is true of nearly every setting: 121 against 2.
* **A field whose HELP carries a measured recommendation is held by that help.** This one was
  missing rather than loose. For a portal field the advice is not in the code reading the flag --
  it is in the field, and that is the only surface carrying it. `embedding.vector_int8` says NOT
  RECOMMENDED with the measurement and names what to use instead; `embedding.dims` cites a
  298-pair benchmark where 512 dims beat the native 1024. Removing either deletes the finding.

## What this deliberately does NOT ask

Whether THIS deployment has a value stored for the key. It is a real reason to keep a field --
`apply_boot` iterates `SETTINGS`, so an undeclared key is never seeded and a deployment that set
one reverts to the default -- but it is a fact about a machine, not about the repository, and a
suite that reads `~/.matrixark/runtime_config.json` asserts against the box it runs on. Measured
once by hand instead: of 115 stored values on the reference deployment, **102 are byte-identical
to the declared default** (seeding those is a no-op) and the file's own history shows a SINGLE
write touching all 115 keys -- the page saves every field, not the ones someone changed. Seven
were decisions, and all seven are held here by something else anyway.
"""
from __future__ import annotations

import ast
import io
import os
import re
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

_NAME = re.compile(r"(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+")

#: Second person, or an explicit hand-over of the decision. The same vocabulary
#: `test_an_addressed_flag_is_offered` uses, spelled out here rather than imported: a test module
#: importing another test module is what `test_matrixark_no_cross_test_imports` refuses.
_ADDRESSED = re.compile(
    r"\bturn (?:it|this) (?:on|off)\b|\bset (?:this|it) (?:if|when|to)\b|\bworth it for\b"
    r"|\ban operator\b|\boperator's call\b|\bfor most deployments\b|\byour deployment\b"
    r"|\benable (?:this|it) (?:if|when)\b|\bleave (?:this|it) (?:on|off|alone)\b",
    re.IGNORECASE)

#: A help text that tells an operator what to do, with something behind it.
_ADVICE = re.compile(
    r"NOT RECOMMENDED|not recommended|\bPrefer\b|\brecommended\b|\bmeasured\b|\bbenchmark\b"
    r"|\bdo not\b|\bDO NOT\b|\bwrong\b|\b\d+(?:\.\d+)?%")

#: A document that says it was generated is a machine listing, not operator documentation.
_GENERATED = ("generated from", "generated by", "do not edit")

_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']'
    r'|os\.environ\[\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']'
    r'|\b\w*[Ee][Nn][Vv]\w*\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']')

#: `test_matrixark_the_portal_offers_what_the_config_file_does` binds the SERVING sections only,
#: exempting these because a bootstrap setting is a launcher decision rather than a page one.
_BOOTSTRAP_SECTIONS = frozenset({
    "gateway", "auth", "server", "recovery", "replication", "wal", "storage", "raft",
    "meta", "proxy", "cluster",
})

#: Floors. Each says what it is FOR and not what the tree currently holds, because a floor pinned
#: to a count fails the day the count moves for a legitimate reason -- which is exactly what
#: retiring knobs from this page did to the one in
#: `test_a_portal_bool_accepts_the_words_a_bool_is_written_with`.
EXPECTED_SETTING_FLOOR = 40
EXPECTED_HOLD_FLOOR = 20

#: Fields held by none of the six, with the reason each stays. Asserted EXACTLY in both
#: directions: a new unheld field fails here, and one that becomes held fails here too, because a
#: list allowed to go stale describes a page that no longer exists.
UNHELD = {
    # The tenant-knob family. `KNOBS_READ_BY_NOTHING` already decides these three the other way
    # and says why beside itself: a deployment may have one set, and a field that vanishes takes
    # its value out of view while leaving it in the file. That is a recorded decision.
    "MATRIXARK_MAX_EVENT_TEXT_CHARS":
        "badged read-by-nothing; the badge is the point, not an oversight",
    "MATRIXARK_MAX_SUMMARY_TEXT_CHARS":
        "badged read-by-nothing; the badge is the point, not an oversight",
    "MATRIXARK_SUMMARY_LEVELS":
        "badged read-by-nothing; the badge is the point, not an oversight",
    # Refused by name when they were tried:
    # test_matrixark_knobs_apply_live.test_the_wired_storage_knobs_are_advertised_live. Both are
    # wired to what gets stored, that suite measures each one flipping mid-flight, and the portal
    # has to say so. A field no criterion holds can still be one an operator needs to see.
    "MATRIXARK_EXTRACT_SEGMENTS":
        "wired to what gets stored; the apply-live suite requires the page to advertise it",
    "MATRIXARK_STORE_EVENT_SUMMARY_TEXT":
        "wired to what gets stored; the apply-live suite requires the page to advertise it",
    # The engine mirror. These are the storage_engine knobs whose declared default
    # `test_engine_settings_offer_the_engine_default` compares against the engine -- against
    # storage_config.rs for the const family, and against the read site for the rest. Removing the
    # field removes the comparison, which is the opposite of what that file is for.
    "TS_INDEX_DUMP_WAL_GAP_BYTES": "compared against storage_config.rs by the engine mirror",
    "TS_MAX_RETAINED_FINISHED_JOBS": "compared against its read site by the engine mirror",
    "TS_METRICS_MAX_SLOT_SERIES": "compared against its read site by the engine mirror",
    "MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT":
        "compared against its read site by the engine mirror",
}

#: Two criteria were wrong in the OTHER direction, and both were found by shipping a cut and being
#: refused by name rather than by reading the code again:
#:
#: * the shipped-config criterion asked whether the file sets a key ACTIVELY. The rule is what the
#:   LOADER maps: `test_matrixark_the_portal_offers_what_the_config_file_does` requires every
#:   serving key in `ENV_MAP` to be on the page, whether or not the shipped file sets it. The
#:   embed-drainer batch and interval are commented out in the file and still held. That criterion
#:   alone requires 57 of the fields here, which is the hard floor under this page.
#: * nothing here can see `test_matrixark_knobs_apply_live`, which requires the page to advertise
#:   a knob that is WIRED to what gets stored. Two of the entries above are only in this list
#:   because that suite refused them.
#:
#: So a field passing every check here is a CANDIDATE for removal, not a verdict on one. Cut it,
#: run the suites that read the settings module, and let whatever refuses say what it protects.


def _tracked(*globs):
    return subprocess.run(["git", "ls-files", *globs], cwd=REPO,
                          capture_output=True, text=True).stdout.split()


def _text(rel):
    try:
        with io.open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except OSError:
        return ""


def _settings():
    import matrixark_gateway_config as gwconfig
    return [s for s in gwconfig.SETTINGS if s.env]


def _config_file_serving() -> set:
    """env names the LOADER maps in a serving section -- set in the file or merely settable.

    The first version of this asked whether the shipped file sets the key ACTIVELY, and that is
    not the rule. `test_matrixark_the_portal_offers_what_the_config_file_does` reads
    `matrixark_load_config.ENV_MAP` and requires every serving key in it to be on the page, on the
    ground that a config file CAN set it and the portal must not be the weaker surface. A key
    commented out in the shipped file is still one the loader honours.

    Found by removing two of them -- the embed-drainer batch and interval, both commented out in
    the file -- and being refused by that test.
    """
    import matrixark_load_config as loader
    return {env for key, env in dict(getattr(loader, "ENV_MAP", {})).items()
            if key.split(".", 1)[0] not in _BOOTSTRAP_SECTIONS}


def _documented(hand_written_only=True) -> set:
    names = set()
    for rel in _tracked("docs/*"):
        if not rel.endswith((".md", ".rst", ".txt")):
            continue
        body = _text(rel)
        if hand_written_only and any(m in body[:1200].lower() for m in _GENERATED):
            continue
        names |= set(_NAME.findall(body))
    return names


def _addressed() -> set:
    """Flags whose read carries a comment block speaking to an operator."""
    out = set()
    for rel in _tracked("tools/*.py"):
        if os.path.basename(rel).startswith("test_"):
            continue
        body = _text(rel)
        lines = body.splitlines()
        for match in _READ.finditer(body):
            name = match.group(1) or match.group(2) or match.group(3)
            number = body.count("\n", 0, match.start()) + 1
            block, index = [], number - 2
            while index >= 0 and lines[index].lstrip().startswith("#"):
                block.append(lines[index].lstrip().lstrip("#").strip())
                index -= 1
            if _ADDRESSED.search(" ".join(reversed(block))):
                out.add(name)
    return out


def _named_by_a_test(by_key=True) -> set:
    body = "\n".join(_text(rel) for rel in _tracked("tools/test_*.py"))
    if not by_key:
        return set(_NAME.findall(body))
    return {s.env for s in _settings()
            if ('"%s"' % s.key) in body or ("'%s'" % s.key) in body}


def _default_on(only_selector=True) -> set:
    switches = [s for s in _settings()
                if s.kind == "bool" and str(s.default) in ("1", "true", "True")]
    if not only_selector:
        return {s.env for s in switches}
    elsewhere = "\n".join(_text(rel) for rel in _tracked(
        "tools/test_*.py", "config/*", "scripts/*", "tools/*.sh", "docs/*"))
    out = set()
    for setting in switches:
        selector = re.compile(
            re.escape(setting.env) + r"""\s*[=:]\s*["']?(0|false|no|off)\b""", re.IGNORECASE)
        if not selector.search(elsewhere):
            out.add(setting.env)
    return out


def _advised() -> set:
    return {s.env for s in _settings() if _ADVICE.search(getattr(s, "help", "") or "")}


def _holds() -> dict:
    return {
        "the shipped config file sets it in a serving section": _config_file_serving(),
        "a hand-written document names it": _documented(),
        "its read carries advice addressed to an operator": _addressed(),
        "a test names its portal key": _named_by_a_test(),
        "it is default-ON and the page is the only off-selector": _default_on(),
        "its help carries a measured recommendation": _advised(),
    }


class APortalFieldIsHeldBySomethingTest(unittest.TestCase):

    def test_the_page_is_there_to_be_measured(self) -> None:
        """A floor. Every assertion below passes over an empty page."""
        settings = _settings()
        self.assertGreater(
            len(settings), EXPECTED_SETTING_FLOOR,
            "only %d settings carry an env name, so this file is about nothing" % len(settings))

    def test_every_criterion_finds_something(self) -> None:
        """And a floor per criterion: one that matches nothing stops holding anything, silently."""
        thin = sorted(name for name, names in _holds().items() if not names)
        self.assertEqual(
            [], thin,
            "these criteria matched no flag at all, so they hold nothing and the list below "
            "grows for the wrong reason: %s" % "; ".join(thin))

    def test_no_criterion_holds_the_whole_page(self) -> None:
        """The trap that made an earlier reading of this useless.

        Four of the six were written loosely first, and each loose version held most of the page.
        A criterion that holds nearly everything is measuring the population, not the property, and
        it makes the unheld list look empty when it is not. These are the loose readings, kept so
        the difference is visible rather than remembered.
        """
        settings = _settings()
        envs = {s.env for s in settings}
        for name, names in _holds().items():
            with self.subTest(criterion=name):
                self.assertLess(
                    len(envs & names), len(settings) * 3 // 4,
                    "%r holds %d of %d fields. Check it is asking the question the tree asks, not "
                    "a looser one -- a test naming the ENV VAR, or any document, or any default-ON "
                    "switch, each held most of the page and none of those is the rule."
                    % (name, len(envs & names), len(settings)))

    def test_the_loose_readings_really_are_looser(self) -> None:
        """A positive control for the corrections above: each strict form must hold strictly less.

        Without this, a correction that quietly stopped applying would read as agreement.
        """
        pairs = (
            ("a test names its portal key", _named_by_a_test(True), _named_by_a_test(False)),
            ("a hand-written document names it", _documented(True), _documented(False)),
            ("default-ON with no other off-selector", _default_on(True), _default_on(False)),
        )
        envs = {s.env for s in _settings()}
        for name, strict, loose in pairs:
            with self.subTest(criterion=name):
                self.assertLess(
                    len(envs & strict), len(envs & loose),
                    "%r holds as much as its loose form (%d), so the correction is not applying"
                    % (name, len(envs & loose)))

    def test_every_field_is_held_or_recorded(self) -> None:
        holds = _holds()
        held = set().union(*holds.values())
        self.assertGreater(
            len(held), EXPECTED_HOLD_FLOOR,
            "the six criteria together hold %d fields, which is too few to be believed"
            % len(held))
        unheld = sorted(s.env for s in _settings() if s.env not in held)
        self.assertEqual(
            sorted(UNHELD), unheld,
            "the operator page has a field nothing holds -- no serving line in the shipped config, "
            "no hand-written document, no sentence addressed to an operator near a read, no test "
            "naming its key, no advice in its own help, and not a default-ON switch this page is "
            "the only way to turn off. Remove it, or record it above with the reason it stays. "
            "Removing a Setting removes the OFFERING, never the variable.")

    def test_every_recorded_field_says_why(self) -> None:
        thin = sorted(env for env, reason in UNHELD.items() if len(reason.strip()) < 30)
        self.assertEqual(
            [], thin,
            "a field kept with no reason beside it is a skip list, and the next reader has to "
            "redo the analysis to find out whether it can go: %s" % "; ".join(thin))


if __name__ == "__main__":
    unittest.main()

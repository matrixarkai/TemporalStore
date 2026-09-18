#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An alert rule that cannot load never fires, and nothing here would have said so.

The rule files are read in three places and none of them reads them as RULES. One regexes the raw
text for series names, to answer "is this series charted or alerted anywhere". Another checks the
stack mounts the files. A third reads one alert's prose. So a rule with an unbalanced expression, a
`for` nobody can parse, or no severity passes every existing check, loads as broken or not at all,
and is silent for exactly as long as nobody looks.

That is the failure mode this whole body of work is about: a surface that reports health it cannot
observe. An alerting rule is that surface at one remove -- it reports nothing, and nothing is what
a healthy deployment also reports.

**What this cannot check.** Whether the expression is semantically valid PromQL: that needs
promtool, which is not on this machine, and a test that silently skips when a tool is absent is
worse than no test. What it checks is the structure a hand edit actually breaks -- delimiters,
durations, the fields a rule needs to be routable -- and it says plainly that it stops there.
"""
from __future__ import annotations

import io
import os
import re
import unittest

try:
    import yaml
except ImportError:  # pragma: no cover - pyyaml is present in this suite's environment
    yaml = None

TOOLS = os.path.dirname(os.path.abspath(__file__))
RULES_DIR = os.path.join(TOOLS, "temporalstore-prometheus")

# Prometheus durations. `for: 30` (no unit) is a load error, and is the easy thing to type.
DURATION = re.compile(r"^\d+(ms|s|m|h|d|w|y)$")

# Closed, because severity routes the alert. A typo sends it nowhere, which looks identical to a
# quiet deployment.
SEVERITIES = {"critical", "warning", "info"}


def rule_files() -> list:
    """The rule files the stack LOADS, not the ones that happen to sit in one directory.

    Listing `RULES_DIR` missed a third of them. `prometheus.yml` loads three files and the compose
    mounts the third from `docs/ops/temporalstore-alerts.yml`, outside this directory -- so 38 of
    the 82 alerting rules were never checked here, and `test_no_two_alerts_share_a_name` could not
    see across the boundary that the duplicate it was written for actually straddled.

    Derived from the same two documents the mount guard beside this reads. Deliberately not
    imported from it: a test importing another test file is its own hazard and there is a guard
    against it. Both derivations read `prometheus.yml` and the compose, so neither can drift
    without the other failing on the same edit.
    """
    prom = os.path.join(RULES_DIR, "prometheus.yml")
    compose = os.path.join(RULES_DIR, "docker-compose.yml")
    if not (os.path.isfile(prom) and os.path.isfile(compose)):
        return []
    with io.open(prom, encoding="utf-8") as handle:
        config = handle.read()
    if "rule_files:" not in config:
        return []
    block = config.split("rule_files:", 1)[1].split("scrape_configs:", 1)[0]
    listed = re.findall(r"-\s+(\S+\.ya?ml)\s*$", block, re.MULTILINE)
    with io.open(compose, encoding="utf-8") as handle:
        sources = dict(re.findall(r"-\s+(\S+?):(/etc/prometheus/[^\s:]+)", handle.read()))
    out = []
    for container_path in listed:
        for source, target in sources.items():
            if target != container_path:
                continue
            resolved = os.path.normpath(os.path.join(RULES_DIR, source))
            if os.path.isfile(resolved) and resolved not in out:
                out.append(resolved)
    return sorted(out)


def every_rule() -> list:
    """(file, group, rule) for every alerting rule in every rule file."""
    out = []
    for path in rule_files():
        with io.open(path, encoding="utf-8") as handle:
            document = yaml.safe_load(handle)
        for group in ((document or {}).get("groups") or []):
            for rule in (group.get("rules") or []):
                if rule.get("alert"):
                    out.append((os.path.basename(path), group.get("name", "?"), rule))
    return out


class EveryAlertIsWellFormedTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        if yaml is None:
            raise unittest.SkipTest("pyyaml is not available")
        cls.rules = every_rule()

    def test_the_files_it_reads_are_the_files_the_stack_loads(self) -> None:
        """The floor on the DENOMINATOR, which is what was wrong here.

        Every check below passes perfectly over a short list, and a short list is what a mount
        that stops resolving produces -- silently, because a file that is listed and not mounted
        simply contributes no rules.
        """
        found = rule_files()
        self.assertGreaterEqual(len(found), 3,
                                "only %d of the loaded rule files resolved: %s"
                                % (len(found), [os.path.basename(p) for p in found]))
        outside = [p for p in found if os.path.dirname(p) != RULES_DIR]
        self.assertTrue(outside,
                        "every resolved rule file is inside %s; the one the stack mounts from "
                        "docs/ops is missing, which is the case this check exists for"
                        % os.path.basename(RULES_DIR))

    def test_there_are_rules_to_check(self) -> None:
        """The vacuity guard. Every assertion below passes perfectly over an empty list, and a
        moved directory or a renamed extension is exactly what produces one."""
        self.assertGreaterEqual(len(self.rules), 20,
                                "found %d rules in %r" % (len(self.rules), rule_files()))
        self.assertGreaterEqual(len(rule_files()), 2)

    def test_each_one_has_an_expression(self) -> None:
        for name, group, rule in self.rules:
            with self.subTest(alert=rule["alert"]):
                self.assertTrue(str(rule.get("expr") or "").strip(),
                                "%s/%s has no expr" % (name, group))

    def test_each_expression_has_balanced_delimiters(self) -> None:
        """What a hand edit breaks. An unbalanced paren makes the file fail to load, which takes
        every OTHER rule in it down too -- so one careless edit silences the whole file."""
        for name, group, rule in self.rules:
            expr = str(rule.get("expr") or "")
            with self.subTest(alert=rule["alert"]):
                self.assertEqual(expr.count("("), expr.count(")"), "unbalanced () in %s" % name)
                self.assertEqual(expr.count("{"), expr.count("}"), "unbalanced {} in %s" % name)
                self.assertEqual(0, expr.count('"') % 2, "odd quote count in %s" % name)

    def test_each_for_is_a_duration(self) -> None:
        """`for: 30` is a load error. `for: 30m` is what was meant, and the two differ by one
        character that a reader's eye passes straight over."""
        for name, group, rule in self.rules:
            if "for" not in rule:
                continue
            with self.subTest(alert=rule["alert"]):
                self.assertRegex(str(rule["for"]), DURATION,
                                 "%s: for=%r is not a Prometheus duration" % (name, rule["for"]))

    def test_each_one_carries_a_severity_that_routes(self) -> None:
        for name, group, rule in self.rules:
            severity = (rule.get("labels") or {}).get("severity")
            with self.subTest(alert=rule["alert"]):
                self.assertIn(severity, SEVERITIES,
                              "%s: severity=%r is not one of %s, so it routes nowhere"
                              % (name, severity, sorted(SEVERITIES)))

    def test_each_one_says_what_is_happening(self) -> None:
        """An alert that fires with no description is a page at 3am with a name on it."""
        for name, group, rule in self.rules:
            annotations = rule.get("annotations") or {}
            with self.subTest(alert=rule["alert"]):
                for key in ("summary", "description"):
                    self.assertTrue(str(annotations.get(key) or "").strip(),
                                    "%s: no annotations.%s" % (name, key))

    def test_each_expression_names_a_series(self) -> None:
        """An expression with no metric in it cannot be about anything. Matched loosely -- any
        snake_case identifier that is not a PromQL keyword -- because the two rule files cover
        different metric namespaces and hardcoding either prefix would make this vacuous for the
        other, which is how the first version of this check reported twenty-four false findings."""
        keywords = {"by", "on", "without", "group_left", "group_right", "offset", "bool",
                    "and", "or", "unless", "ignoring"}
        for name, group, rule in self.rules:
            expr = str(rule.get("expr") or "")
            # Strip PromQL function calls, which are followed by "(".
            identifiers = {token for token in re.findall(r"\b([a-z][a-z0-9_]*_[a-z0-9_]+)\b", expr)
                           if token not in keywords}
            with self.subTest(alert=rule["alert"]):
                self.assertTrue(identifiers, "%s: the expression names no metric" % name)

    def test_no_two_alerts_share_a_name(self) -> None:
        """Two rules with one name is two pages that cannot be told apart in a notification."""
        seen: dict = {}
        for name, group, rule in self.rules:
            seen.setdefault(rule["alert"], []).append("%s/%s" % (name, group))
        duplicates = {alert: where for alert, where in seen.items() if len(where) > 1}
        self.assertEqual({}, duplicates)


if __name__ == "__main__":
    unittest.main()

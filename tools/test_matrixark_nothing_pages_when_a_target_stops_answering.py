#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Nothing paged when a target stopped answering, and every other alert went quiet with it.

Eighty-two alerting rules were loaded by this stack across three files, and every one of them was
written against a metric some process emits. That made all eighty-two conditional on the same
unstated thing: the target has to be answering for the rule to have anything to evaluate.
Prometheus evaluates an expression over a vector that is not there to an empty result, and an empty
result is not a firing alert. So when a process stops being scraped, every rule about it goes quiet
-- and quiet is what healthy looks like.

`MatrixArkGatewayNoTraffic` is where that lands hardest. It exists to catch "clients are not
reaching the edge at all, which no error-rate alert can tell you", and it reads

    sum by (instance) (rate(matrixark_gateway_requests_total[15m])) == 0

Five minutes after the gateway dies, that series goes stale, the vector empties, and the alert
whose entire job is to notice silence goes silent. Its `for: 30m` makes it worse: the gateway has
to be up and idle for thirty continuous minutes, so a gateway that dies during a quiet period never
reaches the threshold at all.

`up` is the one series Prometheus writes itself, per target, per scrape. It is the only thing left
to alert on once a target is gone. Before this, no rule in any of the three files mentioned it --
the nearest was `absent(temporalstore_client_validation_up)`, which watches a harness metric, not a
scrape.

What this checks is that every job `prometheus.yml` scrapes has a rule watching its `up`. What it
does not check is whether that rule is semantically valid PromQL; that needs promtool, which is not
on this machine, and the guard beside this one says the same thing about the same limit.
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
REPO = os.path.dirname(TOOLS)
STACK = os.path.join(TOOLS, "temporalstore-prometheus")
PROM = os.path.join(STACK, "prometheus.yml")
COMPOSE = os.path.join(STACK, "docker-compose.yml")


def _read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


# The two parsers below are deliberately not imported from the mount guard next door, which has
# its own copies: a test importing another test file is its own hazard and there is a guard
# against it. They are small, and they are re-derived here from the same two documents rather than
# shared, so a change to either document has to be made true in both places or one of the two
# tests fails.
def scrape_jobs() -> list:
    """The job names `prometheus.yml` declares under `scrape_configs`."""
    text = _read(PROM)
    if "scrape_configs:" not in text:
        return []
    block = text.split("scrape_configs:", 1)[1]
    return re.findall(r"^\s*-\s+job_name:\s*(\S+)\s*$", block, re.MULTILINE)


def loaded_rule_files() -> list:
    """Absolute paths of the rule files the stack actually loads.

    `prometheus.yml` names container paths; the compose file says where each one comes from. A
    rule file that is listed but not mounted is the mount guard's subject, not this one -- here it
    simply contributes no rules, which is exactly what it would do in the container.
    """
    text = _read(PROM)
    if "rule_files:" not in text:
        return []
    block = text.split("rule_files:", 1)[1].split("scrape_configs:", 1)[0]
    listed = re.findall(r"-\s+(\S+\.ya?ml)\s*$", block, re.MULTILINE)
    sources = dict(re.findall(r"-\s+(\S+?):(/etc/prometheus/[^\s:]+)", _read(COMPOSE)))
    out = []
    for container_path in listed:
        for source, target in sources.items():
            if target == container_path:
                resolved = os.path.normpath(os.path.join(STACK, source))
                if os.path.isfile(resolved):
                    out.append(resolved)
    return out


def every_expression() -> list:
    """(file, alert name, expression) for every alerting rule the stack loads."""
    out = []
    for path in loaded_rule_files():
        document = yaml.safe_load(_read(path))
        for group in ((document or {}).get("groups") or []):
            for rule in (group.get("rules") or []):
                if rule.get("alert"):
                    out.append((os.path.basename(path), rule["alert"],
                                " ".join(str(rule.get("expr") or "").split())))
    return out


def _watches_up_for(expression: str, job: str) -> bool:
    """Does this expression select `up` constrained to this job?

    Matched on the selector rather than on the whole expression, because the two forms that matter
    -- `up{job="x"} == 0` and `absent(up{job="x"})` -- share nothing else, and a rule may well
    carry both.
    """
    for selector in re.findall(r"\bup\{([^}]*)\}", expression):
        found = re.search(r'job\s*(=~|=)\s*"([^"]*)"', selector)
        if not found:
            continue
        operator, value = found.group(1), found.group(2)
        if value == job:
            return True
        # Only `=~` is a regular expression. Treating a `=` value as one would let a literal that
        # happens to contain a metacharacter match a job it does not actually select.
        if operator == "=~" and re.fullmatch(value, job):
            return True
    return False


class EveryScrapedJobHasAnAlertForNotAnsweringTest(unittest.TestCase):

    def test_every_job_is_watched(self) -> None:
        expressions = every_expression()
        for job in scrape_jobs():
            with self.subTest(job=job):
                watching = [name for _, name, expr in expressions
                            if _watches_up_for(expr, job)]
                self.assertTrue(
                    watching,
                    "nothing alerts on up{job=\"%s\"}, so every rule about that target goes "
                    "quiet the moment it stops answering -- and quiet is the healthy state" % job)

    def test_the_scrape_failure_and_the_missing_job_are_both_covered(self) -> None:
        """`up{job="x"} == 0` cannot catch a job with no targets: there is no `up` to compare.

        That is not a hypothetical distinction -- the second case is what an edit to
        `scrape_configs` produces, and it is silent in exactly the same way.
        """
        expressions = every_expression()
        for job in scrape_jobs():
            relevant = [expr for _, _, expr in expressions if _watches_up_for(expr, job)]
            joined = " ".join(relevant)
            with self.subTest(job=job):
                self.assertIn("== 0", joined, "%s: nothing catches a failed scrape" % job)
                self.assertIn("absent(", joined, "%s: nothing catches a job with no targets" % job)

    def test_the_parse_found_the_jobs_and_the_rules(self) -> None:
        """The floor, on the SCAN rather than on the fix.

        Both assertions above pass perfectly over an empty job list, and an empty job list is what
        a renamed key or a restructured `scrape_configs` produces. The rule count is here for the
        same reason: a mount that stops resolving makes every file contribute nothing, and the
        loop above would then find every job unwatched -- or, if the job list emptied too, would
        find nothing at all and say so.
        """
        jobs = scrape_jobs()
        self.assertGreaterEqual(len(jobs), 3, "prometheus.yml declares %d scrape jobs" % len(jobs))
        expressions = every_expression()
        self.assertGreaterEqual(len(expressions), 50,
                                "only %d alerting rules were parsed" % len(expressions))
        self.assertGreaterEqual(len(loaded_rule_files()), 3,
                                "only %d rule files resolved" % len(loaded_rule_files()))


class TheAlertThatWatchesSilenceCannotWatchItAloneTest(unittest.TestCase):
    """The rule this whole file came from, named so a future edit cannot quietly undo it."""

    def test_no_traffic_is_paired_with_a_scrape_alert(self) -> None:
        by_name = {name: expr for _, name, expr in every_expression()}
        self.assertIn("MatrixArkGatewayNoTraffic", by_name,
                      "the alert this pairing exists for is gone")
        # It is written against a gateway series, so it cannot fire once the gateway is unscraped.
        self.assertIn("matrixark_gateway_requests_total", by_name["MatrixArkGatewayNoTraffic"])
        self.assertNotIn("up{", by_name["MatrixArkGatewayNoTraffic"])
        # Which is only acceptable because something else is watching the scrape itself.
        self.assertTrue(
            [name for name, expr in by_name.items()
             if _watches_up_for(expr, "matrixark_gateway")],
            "MatrixArkGatewayNoTraffic reports silence and is silent when the gateway is gone; "
            "nothing else watches the scrape")


if __name__ == "__main__":
    unittest.main()

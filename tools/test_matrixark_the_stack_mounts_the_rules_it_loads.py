#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The monitoring stack mounts every rule file it loads.

`tools/temporalstore-prometheus/prometheus.yml` names three files under `rule_files`. The compose
file beside it mounted **one**:

* `matrixark-gateway-alerts.yml` sits in that very directory and was not mounted;
* `temporalstore-engine-alerts.yml` does not exist under that name anywhere in the repository.

Prometheus does not skip a rule file it cannot read -- it fails its configuration load. So this was
not two missing alert groups, it was a stack that does not come up, and the alerts nobody would have
got are the ones that matter most: retrieval running on hash vectors, extraction making no model
call, live configuration warnings.

The engine rules are mounted from `docs/ops/temporalstore-alerts.yml`, which is where the engine
dashboard's rules live and what `prometheus.yml`'s own comment says the entry pairs with. Mounted
under the name it loads rather than copied, so the portal serves and the stack loads one document.

The two files that share the name `temporalstore-alerts.yml` are NOT duplicates and neither is
stale: the one in this directory is the local one-box group (service down, vars-exporter scrape
errors, ingestion dead letters), and the one in `docs/ops` is the engine and cluster set of eight
groups. Both are loaded now, under distinct names.
"""
from __future__ import annotations

import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
STACK = os.path.join(TOOLS, "temporalstore-prometheus")
PROM = os.path.join(STACK, "prometheus.yml")
COMPOSE = os.path.join(STACK, "docker-compose.yml")


def rule_files() -> list:
    """The container paths `prometheus.yml` lists under `rule_files`."""
    with open(PROM, encoding="utf-8") as handle:
        text = handle.read()
    if "rule_files:" not in text:
        return []
    block = text.split("rule_files:", 1)[1].split("scrape_configs:", 1)[0]
    return re.findall(r"-\s+(\S+\.ya?ml)\s*$", block, re.MULTILINE)


def mounts() -> dict:
    """{container path: source path relative to the compose file}."""
    with open(COMPOSE, encoding="utf-8") as handle:
        text = handle.read()
    found = {}
    for source, target in re.findall(r"-\s+(\S+?):(/etc/prometheus/[^\s:]+)", text):
        found[target] = source
    return found


class EveryRuleFileIsMountedTest(unittest.TestCase):

    def test_each_one_has_a_mount(self) -> None:
        mounted = mounts()
        missing = [path for path in rule_files() if path not in mounted]
        self.assertEqual([], missing,
                         "prometheus.yml loads these and the compose mounts nothing at them: %s"
                         % missing)

    def test_each_mount_points_at_a_file_that_exists(self) -> None:
        """A mount whose source is absent gives the container an empty directory, which Prometheus
        reads as a rule file it cannot parse."""
        mounted = mounts()
        for path in rule_files():
            source = mounted.get(path)
            if source is None:
                continue  # the test above owns that case
            resolved = os.path.normpath(os.path.join(STACK, source))
            with self.subTest(rule_file=path):
                self.assertTrue(os.path.isfile(resolved),
                                "%s mounts %s, which does not exist" % (path, source))

    def test_the_parse_found_the_rule_files(self) -> None:
        """The floor: an empty list makes both rules above pass without reading anything."""
        found = rule_files()
        self.assertGreaterEqual(len(found), 3,
                                "prometheus.yml lists %d rule files" % len(found))

    def test_the_parse_found_the_mounts(self) -> None:
        """A floor on the PARSE, not on the fix: it must find that the compose mounts something
        into the container, whatever that turns out to be. Asserting a count that only holds after
        the change would make this test a second copy of the rule above."""
        found = mounts()
        self.assertTrue(found, "the compose parse found no /etc/prometheus mounts at all")
        self.assertIn("/etc/prometheus/prometheus.yml", found,
                      "the config itself is always mounted; the parse missed it")


class TheTwoSameNamedAlertFilesAreDifferentTest(unittest.TestCase):
    """Both are loaded, under distinct names. If they ever became copies of each other, mounting
    both would be pointless and this should say so rather than quietly loading one twice."""

    LOCAL = os.path.join(STACK, "temporalstore-alerts.yml")
    ENGINE = os.path.join(REPO, "docs", "ops", "temporalstore-alerts.yml")

    def read(self, path: str) -> str:
        with open(path, encoding="utf-8") as handle:
            return handle.read()

    def test_they_are_not_the_same_document(self) -> None:
        self.assertNotEqual(self.read(self.LOCAL), self.read(self.ENGINE))

    def test_they_declare_different_groups(self) -> None:
        def groups(text):
            return set(re.findall(r"^\s*-\s+name:\s*(\S+)", text, re.MULTILINE))
        local, engine = groups(self.read(self.LOCAL)), groups(self.read(self.ENGINE))
        self.assertTrue(local, "the local alert file declares no groups")
        self.assertTrue(engine, "the engine alert file declares no groups")
        self.assertEqual(set(), local & engine,
                         "these share group names, so loading both would define a group twice")


if __name__ == "__main__":
    unittest.main()

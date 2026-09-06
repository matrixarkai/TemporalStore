# SPDX-License-Identifier: Apache-2.0
"""A merge gate that filters pull requests by base branch does not cover a stacked one.

`on: pull_request: branches:` filters on the BASE branch. A stacked pull request is based on
the branch below it rather than on main, so a gate written that way never ran on one. That
alone would be visible -- a missing check is at least a missing check. What made it silent is
the retarget: when the branch below merges, GitHub moves the pull request onto main and does
NOT re-run the `pull_request` workflows. The checks from its feature-branch days stay, still
green, and the pull request reports CLEAN and merges having never been seen by the gate.

Measured on this repository before the fix: over the entire life of one stacked branch, across
three pushes, the only workflows that ever ran were Auto-approve and Governance -- the two
without a base filter. The suite ratchet, the readiness contract and the Rust build never ran,
and it merged into main.

So the invariant is not "these workflows exist" but "these workflows are reachable from every
pull request", and the two are not the same statement.
"""

import io
import os
import unittest

WORKFLOWS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         ".github", "workflows")

# The jobs whose result is a reason not to merge. Governance is here too: it already had no
# base filter, so it doubles as the control showing the assertion can be satisfied.
GATES = (
    "python-suite-ratchet.yml",
    "oss-readiness.yml",
    "rust-ci.yml",
    "governance.yml",
)


def _triggers(name: str) -> dict:
    import yaml
    with io.open(os.path.join(WORKFLOWS, name), encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    # `on` is a YAML 1.1 boolean, so PyYAML hands back the key True rather than the string.
    # Reading only document["on"] finds nothing and every assertion below would pass vacuously.
    return document.get(True, document.get("on")) or {}


def _pull_request(name: str):
    trigger = _triggers(name).get("pull_request")
    return {} if trigger is None else trigger


class EveryGateIsReachableFromEveryPullRequest(unittest.TestCase):

    def test_the_workflow_files_are_all_there(self) -> None:
        # A floor: without this, a wrong directory makes every test below pass on an empty set.
        present = sorted(f for f in os.listdir(WORKFLOWS) if f.endswith((".yml", ".yaml")))
        self.assertGreaterEqual(len(present), len(GATES), present)
        for name in GATES:
            self.assertIn(name, present)

    def test_each_gate_runs_on_a_pull_request_at_all(self) -> None:
        for name in GATES:
            self.assertIn("pull_request", _triggers(name),
                          "%s has no pull_request trigger, so it gates nothing" % name)

    def test_no_gate_filters_pull_requests_by_base_branch(self) -> None:
        for name in GATES:
            self.assertNotIn(
                "branches", _pull_request(name),
                "%s filters pull requests by base branch, so a stacked pull request merges "
                "without it and a retarget will not bring it back" % name)

    def test_no_gate_filters_pull_requests_by_path(self) -> None:
        # Same silent-skip shape by a different key: a path filter on a required check leaves it
        # expected and never delivered, and on an optional one it just quietly does not run.
        for name in GATES:
            self.assertNotIn("paths", _pull_request(name),
                             "%s filters pull requests by path" % name)
            self.assertNotIn("paths-ignore", _pull_request(name),
                             "%s filters pull requests by path" % name)

    def test_the_push_triggers_keep_their_filters(self) -> None:
        # The fix is "stop filtering pull requests", not "stop filtering". Push runs on the
        # branch itself, where a base filter means what it says and is worth keeping; if this
        # ever goes to zero the change was applied by deleting filters wholesale.
        # Counting how many still filter is too loose: with four gates, a floor of three passes
        # while one of them silently loses its filter. Name every one instead.
        for name in GATES:
            push = _triggers(name).get("push")
            self.assertIsInstance(push, dict, "%s has no push trigger" % name)
            self.assertTrue(push.get("branches"),
                            "%s runs on a push to any branch" % name)


class TheReadingItselfWorks(unittest.TestCase):
    """The assertions above are all negative, so they pass hardest against a parser that
    returns nothing. These give them a positive control."""

    def test_a_base_filter_is_reported_when_one_is_there(self) -> None:
        import tempfile
        import yaml
        document = {"on": {"pull_request": {"branches": ["main"]},
                           "push": {"branches": ["main"]}},
                    "jobs": {}}
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "sample.yml")
            with io.open(path, "w", encoding="utf-8", newline="\n") as handle:
                yaml.safe_dump(document, handle)
            global WORKFLOWS
            saved, WORKFLOWS = WORKFLOWS, directory
            try:
                self.assertIn("branches", _pull_request("sample.yml"))
                self.assertIn("pull_request", _triggers("sample.yml"))
            finally:
                WORKFLOWS = saved

    def test_the_real_files_parse_into_something(self) -> None:
        for name in GATES:
            self.assertTrue(_triggers(name), "%s parsed to nothing" % name)


if __name__ == "__main__":
    unittest.main()

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every shell copy of the flag vocabulary answers the same way.

Five shell scripts carry their own `matrixark_flag_on`. They are copies on purpose: each runs
standalone in deployments where a missing shared file would be a failure to start, and the function
is pure and six lines long. What copies do is drift, and a flag vocabulary that drifts is not a
cosmetic problem -- `TS_PROFILE_ENV_ONLY` was compared against the literal `"1"`, so `=true`, `=yes`
and `=on` read as off, and off there means the profile launches a datanode the caller did not ask
for. The caller's own node then dies with `Address already in use` and its exports never reach the
process that ends up serving, which reads as a flag that does not work.

So the copies are held level by BEHAVIOUR, not by text: each one is extracted and run against the
whole vocabulary, in both default positions, and they must all return the same verdict for every
value. A copy that adds a spelling, drops one, or stops lowercasing shows up here as a
disagreement, whatever its source looks like.

Only the function is extracted -- sourcing any of these files whole would run a launcher.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
TOOLS = ROOT / "tools"

#: Every spelling worth asking about, plus the shapes that must fall through to the default.
VALUES = ("1", "true", "TRUE", "True", "yes", "YES", "on", "On", "ON",
          "0", "false", "FALSE", "no", "NO", "off", "OFF",
          # The same words carrying whitespace. A shell export, a systemd Environment= line and a
          # heredoc all leave it behind, and the helper matched none of them until it trimmed --
          # so every one of these fell through to the DEFAULT, which is the opposite of what the
          # operator wrote whenever the default disagreed with them.
          "1 ", " 1", "  1  ", "true ", " true", "yes ", "ON ",
          "0 ", " 0", "  0  ", "false ", " off", "off ", "NO ",
          "", "   ", "2", "garbage", "y", "n", "enabled")

#: value -> the answer it must give, whatever the default is. Whitespace-only and unrecognised
#: values are deliberately absent: those fall back to the default, which is the point of the
#: `*)` branch and is checked by the agreement test rather than here.
TRIMMED_MEANING = {
    "1": True, "true": True, "TRUE": True, "yes": True, "on": True, "ON": True,
    "1 ": True, " 1": True, "  1  ": True, "true ": True, " true": True, "yes ": True,
    "ON ": True,
    "0": False, "false": False, "FALSE": False, "no": False, "off": False, "OFF": False,
    "0 ": False, " 0": False, "  0  ": False, "false ": False, " off": False, "off ": False,
    "NO ": False,
}

_DEF = re.compile(r"^matrixark_flag_on\(\)\s*\{.*?^\}", re.MULTILINE | re.DOTALL)

_HARNESS = """
%s
for v in %s; do
  if matrixark_flag_on "$v" %s; then printf 'on\n'; else printf 'off\n'; fi
done
"""


def _copies() -> dict:
    """path stem -> the text of its matrixark_flag_on definition."""
    found = {}
    for path in sorted(TOOLS.glob("*.sh")):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        match = _DEF.search(text)
        if match:
            found[path.name] = match.group(0)
    return found


def _verdicts(body: str, default: str):
    quoted = " ".join("'%s'" % v.replace("'", "'\''") for v in VALUES)
    script = _HARNESS % (body, quoted, default)
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        return None, result.stderr.strip().splitlines()[-1:] or ["<no stderr>"]
    lines = result.stdout.split()
    if len(lines) != len(VALUES):
        return None, ["expected %d verdicts, got %d" % (len(VALUES), len(lines))]
    return dict(zip(VALUES, lines)), None


class EveryShellCopyOfTheFlagVocabularyAgreesTest(unittest.TestCase):

    def test_there_are_copies_to_compare(self) -> None:
        """Vacuity floor on the SCAN. One copy, or none, and every comparison below passes by
        having nothing to compare -- which reads exactly like five copies agreeing."""
        found = _copies()
        self.assertGreaterEqual(
            len(found), 2,
            "found %d shell copies of matrixark_flag_on (%s). If they were consolidated into one "
            "shared file that is good news and this test has nothing left to hold level -- retire "
            "it. If the pattern stopped matching, it is passing for the wrong reason."
            % (len(found), ", ".join(sorted(found)) or "none"))

    def test_the_vocabulary_separates_on_from_off(self) -> None:
        """Positive control. A helper that answered `on` to everything would satisfy every
        agreement check below, because they only ask whether the copies MATCH."""
        found = _copies()
        name, body = sorted(found.items())[0]
        verdicts, err = _verdicts(body, "1")
        self.assertIsNone(err, "%s could not be run: %s" % (name, err))
        self.assertEqual("on", verdicts["true"], "%s does not read 'true' as on" % name)
        self.assertEqual("off", verdicts["false"], "%s does not read 'false' as off" % name)

    def test_every_copy_returns_the_same_verdict_for_every_value(self) -> None:
        found = _copies()
        for default in ("0", "1"):
            answers = {}
            for name, body in sorted(found.items()):
                verdicts, err = _verdicts(body, default)
                self.assertIsNone(err, "%s could not be run: %s" % (name, err))
                answers[name] = verdicts
            baseline_name, baseline = sorted(answers.items())[0]
            for name, verdicts in sorted(answers.items()):
                differing = {v: (baseline[v], verdicts[v])
                             for v in VALUES if baseline[v] != verdicts[v]}
                with self.subTest(copy=name, default=default):
                    self.assertEqual(
                        {}, differing,
                        "%s disagrees with %s (default %s) on: %s. A flag spelled one way reaches "
                        "one script and not another."
                        % (name, baseline_name, default,
                           ", ".join("%r: %s vs %s" % (v, a, b)
                                     for v, (a, b) in sorted(differing.items()))))

    def test_a_value_carrying_whitespace_still_means_what_it_says(self) -> None:
        """The agreement test cannot see this: five copies agreeing on the wrong answer agree.

        Every one of these fell through to the `*)` branch before the helper trimmed, so the
        answer was the DEFAULT rather than the word the operator wrote -- `0 ` left a default-on
        flag ON, and `1 ` left a default-off flag OFF.
        """
        found = _copies()
        self.assertTrue(found, "no copies to check")
        for name, body in sorted(found.items()):
            for default in ("0", "1"):
                verdicts, err = _verdicts(body, default)
                self.assertIsNone(err, "%s could not be run: %s" % (name, err))
                for value, expected in sorted(TRIMMED_MEANING.items()):
                    with self.subTest(copy=name, default=default, value=value):
                        self.assertEqual(
                            "on" if expected else "off", verdicts[value],
                            "%s reads %r as %s with default %s. Whitespace survives a shell "
                            "export, a systemd Environment= line and a heredoc, and the value "
                            "means the opposite of what it was read as."
                            % (name, value, verdicts[value], default))

    def test_the_env_only_switch_goes_through_the_vocabulary(self) -> None:
        """The read that prompted this. `TS_PROFILE_ENV_ONLY` decides whether sourcing the profile
        starts a datanode; it used to hear only the digit."""
        path = TOOLS / "deploy_profile_common.sh"
        if not path.exists():
            self.skipTest("deploy_profile_common.sh is absent from this checkout")
        text = path.read_text(encoding="utf-8", errors="replace")
        self.assertIn(
            'matrixark_flag_on "${TS_PROFILE_ENV_ONLY:-}"', text,
            "the env-only switch no longer goes through matrixark_flag_on. Compared against a "
            "literal it hears only that literal, and every other spelling launches a datanode the "
            "caller did not ask for.")
        match = _DEF.search(text)
        self.assertIsNotNone(match, "deploy_profile_common.sh calls the helper but does not "
                                    "define it, and it is not sourced from anywhere")
        verdicts, err = _verdicts(match.group(0), "0")
        self.assertIsNone(err, "the helper there could not be run: %s" % err)
        for spelling in ("1", "true", "yes", "on", "ON"):
            with self.subTest(value=spelling):
                self.assertEqual("on", verdicts[spelling],
                                 "TS_PROFILE_ENV_ONLY=%s would still launch a node" % spelling)
        for spelling in ("0", "false", "no", "off", "", "garbage"):
            with self.subTest(value=spelling):
                self.assertEqual("off", verdicts[spelling],
                                 "TS_PROFILE_ENV_ONLY=%r now suppresses the launch; it did not "
                                 "before, and that is a behaviour change, not a vocabulary fix"
                                 % spelling)


if __name__ == "__main__":
    unittest.main()

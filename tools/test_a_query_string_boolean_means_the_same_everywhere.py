"""A query-string boolean means the same thing on every endpoint.

The gateway defines `_env_bool(value, default)` at the top of the file with the vocabulary this tree
uses -- {"1","true","yes","on"} -- and a None-means-default rule. Four query reads spelled their own
shorter list instead, and one of them compared without lowercasing, so `?overrides=TRUE` was ignored
while `?probe=TRUE` worked.

This is a caller-facing surface. An operator setting a deployment flag can read the page; someone
sending a request just gets a different answer and no reason for it.

The test scans the gateway for boolean comparisons made against a query-parameter value and requires
the accepted set to be complete -- any list that takes "true" must take "on", and any that takes
"false" must take "off". `_env_bool` satisfies this by construction, so routing a read through it
passes; spelling a new short list does not.

The scan is floored: a pattern that stopped matching would find no comparisons, therefore no
offenders, and read exactly like a clean result.
"""

import ast
import pathlib
import re
import unittest

GATEWAY = pathlib.Path(__file__).resolve().parent / "matrixark_v1_gateway.py"

TRUE_WORDS = {"1", "true", "yes", "on", "t", "y"}
FALSE_WORDS = {"0", "false", "no", "off", "f", "n"}
QUERY_HINT = re.compile(r"\b(query|params|qs|parsed|_qc)\b")


def _boolean_comparisons_near_a_query_read():
    """(function, line, accepted-words, source) for each inline boolean set in a query context."""
    text = GATEWAY.read_text(encoding="utf-8")
    tree = ast.parse(text)
    lines = text.splitlines()
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        end = getattr(node, "end_lineno", node.lineno)
        if not QUERY_HINT.search("\n".join(lines[node.lineno - 1:end])):
            continue
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Compare) or len(inner.ops) != 1:
                continue
            if not isinstance(inner.ops[0], ast.In):
                continue
            target = inner.comparators[0] if inner.comparators else None
            if not isinstance(target, (ast.Set, ast.List, ast.Tuple)):
                continue
            values = {e.value.strip().lower() for e in target.elts
                      if isinstance(e, ast.Constant) and isinstance(e.value, str)}
            if not values or not values <= (TRUE_WORDS | FALSE_WORDS):
                continue
            found.append((node.name, inner.lineno, frozenset(values),
                          lines[inner.lineno - 1].strip()))
    return found


class AQueryStringBooleanMeansTheSameEverywhereTest(unittest.TestCase):

    def test_the_gateway_still_has_the_shared_helper(self):
        """Everything below assumes the complete vocabulary lives in one place in this file."""
        text = GATEWAY.read_text(encoding="utf-8")
        self.assertIn("def _env_bool(", text)
        self.assertIn('{"1", "true", "yes", "on"}', text,
                      "_env_bool no longer carries the complete vocabulary, so routing reads "
                      "through it no longer means what this file assumes")

    def test_the_scan_can_still_see_comparisons(self):
        """A pattern that stopped matching finds no offenders and reads as a clean result.

        There is at least one legitimate inline set in this file -- `truthy` inside
        `_finalize_requested`, which already spells the complete vocabulary -- so the scan finding
        NOTHING means the scan is broken, not that the file is clean.
        """
        found = _boolean_comparisons_near_a_query_read()
        self.assertGreater(
            len(found), 0,
            "the scan found no boolean comparisons at all in the gateway; the AST shape it matches "
            "has changed and every assertion below would pass over nothing")

    def test_every_accepted_set_is_complete(self):
        offenders = []
        for name, lineno, values, src in _boolean_comparisons_near_a_query_read():
            takes_true = "true" in values
            takes_false = "false" in values
            if takes_true and "on" not in values and not (values & FALSE_WORDS):
                offenders.append((name, lineno, sorted(values), src, 'takes "true" but not "on"'))
            elif takes_false and "off" not in values and not (values & TRUE_WORDS):
                offenders.append((name, lineno, sorted(values), src, 'takes "false" but not "off"'))
        self.assertFalse(
            offenders,
            "a query-string boolean accepts a word but not its synonym:\n"
            + "\n".join("    %s:%d  {%s}  -- %s\n        %s"
                        % (n, l, ", ".join(v), why, src)
                        for n, l, v, src, why in offenders)
            + "\n\nUse _env_bool(value, default) rather than spelling a set inline. A caller "
              "sending ?x=on gets a different answer from one sending ?x=1, with nothing to say "
              "why.")

    def test_no_query_comparison_forgets_to_lowercase(self):
        """`?overrides=TRUE` was ignored while `?probe=TRUE` worked, because one read compared the
        stripped value without lowering it. A set containing only lowercase words cannot match an
        uppercase value, so the missing `.lower()` is silent."""
        text = GATEWAY.read_text(encoding="utf-8")
        offenders = []
        for lineno, line in enumerate(text.splitlines(), 1):
            if "params.get(" not in line and "_qc(" not in line:
                continue
            if ".strip()" in line and ".lower()" not in line and " in (" in line:
                offenders.append((lineno, line.strip()))
        self.assertFalse(
            offenders,
            "a query value is compared against lowercase words without being lowered:\n"
            + "\n".join("    %d: %s" % (l, s) for l, s in offenders))


if __name__ == "__main__":
    unittest.main()

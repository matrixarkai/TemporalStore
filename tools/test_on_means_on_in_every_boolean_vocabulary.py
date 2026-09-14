"""A boolean vocabulary that accepts "true" must accept "on".

``matrixark_mcp_env`` defines the vocabulary this tree uses::

    TRUE_VALUES  = {"1", "true", "yes", "on"}
    FALSE_VALUES = {"0", "false", "no", "off"}

and 43 modules import from it. A set literal spelled inline that leaves out "on" is not a style
difference. The test is a membership check, so an operator who writes ``=on`` -- a word that means
exactly one thing -- gets ``"on" in {"1","true","yes"}``, which is False. The flag reads as OFF.

That is worse than a no-op wherever the default is ON, because the operator turned something off by
trying to turn it on, and worse again on a ``REQUIRE_*`` flag, where the failure is open: writing
``MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK=on`` stopped requiring the native pack.

Nine such sites existed across five modules, and two of those modules already imported ``env_bool``
from ``matrixark_mcp_env`` in the same file -- which is what makes it an oversight rather than a
decision.

The scan is asserted not to be vacuous: it counts the vocabularies it compared and fails if that
population collapses, because a sweep that silently stops matching reads exactly like a clean one.
"""

import ast
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

TRUE_WORDS = {"1", "true", "yes", "on", "t", "y"}
FALSE_WORDS = {"0", "false", "no", "off", "f", "n"}

# "file:line-ish site" -> why this one is allowed to stay asymmetric.
#
# Keyed by module and the identifier the site belongs to rather than a line number, so ordinary
# edits above it do not invalidate the record.
RECORDED_EXEMPTIONS = {
    "matrixark_access.py": (
        "The DSN query parameter `ssl_disabled`, not an environment flag. Accepting \"on\" here "
        "would change behaviour in the permissive direction -- `?ssl_disabled=on` currently leaves "
        "TLS ON, and honouring the word would turn TLS OFF for any deployment whose DSN already "
        "spells it that way. That is a security decision for a person to make, not a vocabulary "
        "tidy-up, so it is recorded rather than changed."),
}


def _vocabularies():
    """(module, lineno, values) for every inline set literal made only of boolean words."""
    found = []
    modules = 0
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        modules += 1
        for node in ast.walk(tree):
            if not isinstance(node, ast.Set):
                continue
            elements = [e for e in node.elts
                        if isinstance(e, ast.Constant) and isinstance(e.value, str)]
            if len(elements) != len(node.elts) or not elements:
                continue
            values = {e.value.strip().lower() for e in elements}
            if len(values) < 2 or not values <= (TRUE_WORDS | FALSE_WORDS):
                continue
            found.append((path.name, node.lineno, frozenset(values)))
    return modules, found


def _asymmetric(found):
    out = []
    for name, lineno, values in found:
        truthy = values & TRUE_WORDS
        falsy = values & FALSE_WORDS
        if "true" in values and "on" not in values and not falsy:
            out.append((name, lineno, values, 'accepts "true" but not "on"'))
        elif "false" in values and "off" not in values and not truthy:
            out.append((name, lineno, values, 'accepts "false" but not "off"'))
    return out


class OnMeansOnTest(unittest.TestCase):

    def test_the_scan_still_finds_vocabularies(self):
        """Without this the real test passes by comparing nothing, and reads like a clean result."""
        modules, found = _vocabularies()
        self.assertGreater(modules, 100,
                           "parsed only %d modules under %s" % (modules, TOOLS))
        self.assertGreater(len(found), 20,
                           "found only %d inline boolean vocabularies -- the AST shape being "
                           "matched has probably changed" % len(found))

    def test_every_vocabulary_that_takes_true_also_takes_on(self):
        _modules, found = _vocabularies()
        bad = [(n, l, v, why) for n, l, v, why in _asymmetric(found)
               if n not in RECORDED_EXEMPTIONS]
        self.assertFalse(
            bad,
            "these vocabularies reject a word that means what they accept:\n"
            + "\n".join("    %s:%d  {%s}  -- %s" % (n, l, ", ".join(sorted(v)), why)
                        for n, l, v, why in bad)
            + "\n\nUse TRUE_VALUES / FALSE_VALUES from matrixark_mcp_env instead of spelling the "
              "set inline. An operator writing `=on` must not turn the flag off.")

    def test_a_recorded_exemption_still_describes_something_real(self):
        """An exemption for a site that is no longer asymmetric is a licence nobody is using."""
        _modules, found = _vocabularies()
        still = {n for n, _l, _v, _w in _asymmetric(found)}
        stale = sorted(set(RECORDED_EXEMPTIONS) - still)
        self.assertFalse(
            stale,
            "these modules are recorded as exempt but hold no asymmetric vocabulary any more:\n"
            + "\n".join("    %s" % n for n in stale)
            + "\n\nDrop them from RECORDED_EXEMPTIONS -- an exemption that describes nothing is "
              "a hole left open for a reason that has gone.")

    def test_an_exemption_says_why(self):
        for name, reason in sorted(RECORDED_EXEMPTIONS.items()):
            self.assertGreaterEqual(
                len(reason.split()), 12,
                "%s is exempt with a reason too short to justify it: %r" % (name, reason))

    def test_the_canonical_vocabulary_is_the_one_being_pointed_at(self):
        """If TRUE_VALUES itself lost "on", every site fixed by importing it would regress."""
        import matrixark_mcp_env
        self.assertIn("on", matrixark_mcp_env.TRUE_VALUES)
        self.assertIn("off", matrixark_mcp_env.FALSE_VALUES)
        self.assertTrue(matrixark_mcp_env.TRUE_VALUES.isdisjoint(matrixark_mcp_env.FALSE_VALUES))


if __name__ == "__main__":
    unittest.main()

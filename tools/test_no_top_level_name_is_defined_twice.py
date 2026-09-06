# SPDX-License-Identifier: Apache-2.0
"""A module must not define the same top-level name twice.

Python takes the last definition. An earlier one is not an alternative, an override or a fallback:
it never runs, and nothing can reach it. Read in a file it looks exactly like live code, and it
diverges from the definition that replaced it, so it goes on collecting edits that have no effect.

`matrixark_mcp_local_adapter` held four budget functions this way -- 668 lines. Each had a full
implementation, and then, 300 lines later, a one-line delegate to the module the work had moved to.
The full implementations had drifted from the copies that superseded them in BOTH directions: the
dead `auto_memory_selection_policy_budget_tokens` zeroed two policies for a scope query and skipped
zero-fraction policies entirely, which the live one cannot express; the live one grew a
feature-profile branch the dead one never got. Neither was the complete one. Reading either as
current would have been wrong.

There is one shape that is not this defect, and it is derived rather than listed:

    class _HookStoreReader:            # abstract; raises NotImplementedError
        ...
    class _HookStoreReader(_HookStoreReader):
        ...

The second definition names the first in its bases, so the first is still reachable -- as the base
of the second. Anything evaluated at definition time can do this (bases, decorators, parameter
defaults), so the check asks whether the later definition mentions the earlier name in one of
those, rather than keeping a list of blessed files. A list would need editing every time the idiom
is used again, and an exemption nobody re-derives is how the next real duplicate gets waved past.
"""
from __future__ import annotations

import ast
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SELF = Path(__file__).name

#: 167 modules when this was written. A floor, so a scan that stops reaching the tree fails here
#: rather than passing with nothing to look at.
EXPECTED_MODULE_FLOOR = 120

Definition = "ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef"


def _modules() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "tools/*.py"], cwd=REPO, capture_output=True, text=True).stdout.split()
    return [REPO / rel for rel in listed if Path(rel).name != SELF]


def extends_the_earlier(later, name: str) -> bool:
    """Does the later definition reach the earlier one through something evaluated at def time?

    Bases, decorators and parameter defaults are all evaluated while the definition is being made,
    at which point the previous binding is still in place. A mention inside the BODY does not count
    -- that resolves at call time, to the later definition itself, which is ordinary recursion.
    """
    spots = list(getattr(later, "bases", [])) + list(later.decorator_list)
    args = getattr(later, "args", None)
    if args is not None:
        spots += list(args.defaults) + [d for d in args.kw_defaults if d is not None]
    return any(isinstance(inner, ast.Name) and inner.id == name
               for spot in spots for inner in ast.walk(spot))


def collect_shadowed() -> list[tuple[str, str, list[int], int]]:
    """(module, name, the lines each copy starts on, lines that cannot run)."""
    shadowed = []
    for path in _modules():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except (SyntaxError, OSError):
            continue
        by_name: dict[str, list] = {}
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                by_name.setdefault(node.name, []).append(node)
        for name, copies in by_name.items():
            if len(copies) < 2:
                continue
            if any(extends_the_earlier(later, name) for later in copies[1:]):
                continue
            dead = sum(c.end_lineno - c.lineno + 1 for c in copies[:-1])
            shadowed.append((path.name, name, [c.lineno for c in copies], dead))
    return shadowed


class NoTopLevelNameIsDefinedTwiceTest(unittest.TestCase):

    def test_the_scan_reaches_the_tree(self) -> None:
        modules = _modules()
        self.assertGreaterEqual(
            len(modules), EXPECTED_MODULE_FLOOR,
            "the scan sees %d modules, below the floor of %d -- it is not reading the tree, so a "
            "pass below means nothing" % (len(modules), EXPECTED_MODULE_FLOOR))

    def test_the_scan_still_finds_duplicates_and_still_exempts_the_idiom(self) -> None:
        """Mechanism control, and the only case in the tree that exercises the exemption.

        `matrixark_http._HookStoreReader` is defined twice, deliberately, the second naming the
        first as its base. It must be SEEN as a duplicate and then EXEMPTED. If it stops being
        seen, the scan has stopped finding duplicates and the rule below is vacuous; if it stops
        being exempted, the exemption has broken and the rule is about to reject a valid idiom.
        """
        tree = ast.parse((REPO / "tools/matrixark_http.py").read_text(encoding="utf-8"))
        copies = [n for n in tree.body
                  if isinstance(n, ast.ClassDef) and n.name == "_HookStoreReader"]
        self.assertEqual(
            2, len(copies),
            "matrixark_http no longer defines _HookStoreReader twice, so nothing in the tree "
            "exercises the exemption -- point this control at whatever uses the idiom now, or "
            "drop the exemption if nothing does")
        self.assertTrue(
            extends_the_earlier(copies[1], "_HookStoreReader"),
            "the second _HookStoreReader no longer names the first in its bases")
        self.assertNotIn(
            "_HookStoreReader", {name for _, name, _, _ in collect_shadowed()},
            "the exemption stopped applying to the idiom it was derived for")

    def test_the_exemption_reads_definition_time_and_not_the_body(self) -> None:
        """Positive control on the classifier, on both sides of the line it draws."""
        base = ast.parse("class C(C):\n    pass\n").body[0]
        self.assertTrue(extends_the_earlier(base, "C"), "a base no longer counts")

        decorated = ast.parse("@C\ndef C():\n    pass\n").body[0]
        self.assertTrue(extends_the_earlier(decorated, "C"), "a decorator no longer counts")

        default = ast.parse("def C(x=C):\n    pass\n").body[0]
        self.assertTrue(extends_the_earlier(default, "C"), "a parameter default no longer counts")

        recursive = ast.parse("def C(n):\n    return C(n - 1)\n").body[0]
        self.assertFalse(
            extends_the_earlier(recursive, "C"),
            "a call in the body resolves to the later definition itself -- treating recursion as "
            "an exemption would exempt every self-calling function that gets redefined")

    def test_no_module_defines_the_same_top_level_name_twice(self) -> None:
        shadowed = collect_shadowed()
        detail = ["%s defines %s at lines %s; only the last one runs, %d lines cannot"
                  % (module, name, lines, dead) for module, name, lines, dead in shadowed]
        self.assertEqual(
            [], detail,
            "a definition is shadowed by a later one with the same name, so it never runs while "
            "still reading as live code:\n  " + "\n  ".join(detail))


if __name__ == "__main__":
    unittest.main()

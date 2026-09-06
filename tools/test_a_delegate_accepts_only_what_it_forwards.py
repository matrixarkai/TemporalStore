# SPDX-License-Identifier: Apache-2.0
"""A delegate must not accept an argument it does not forward.

`matrixark_mcp_local_adapter` holds a family of one-line functions that forward to a module the
work was split into. One of them accepted a keyword the target does not have and dropped it with
`del`, so a caller could pass a carefully computed value and have it discarded in silence. The
caller in `matrixark_local_adapter_retrieve` did exactly that: it computed the outcome-query flag
with a question-type test and a query-text test, passed it in, and the delegate deleted it. The
call read as if it were configuring the budget. It was not.

Nothing had gone wrong, because the target derives that flag for itself. That is what makes the
shape worth a check rather than a one-line fix: a discarded argument is invisible at the call site,
survives review, and the test that covered it passed for either value.

A delegate here is a module-level function whose body is a single `return <imported>(...)`.

So: for every delegate, every parameter it accepts must appear in the forwarded call. Note what
this deliberately does NOT compare -- the target's parameter NAMES. The first version of this file
did, and reported three renames as defects: `_tenant_of_scope_key(scope_key)` forwards to a target
whose parameter is called `scope`, which is a rename, not a discard. A keyword the target does not
take is a TypeError the moment the delegate runs, so it needs no guard; a parameter that never
reaches the call is silent, which is why this one is worth writing.

The scan is AST-only and does not import anything, so a module with a heavy or circular import
still gets checked.
"""
from __future__ import annotations

import ast
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SELF = Path(__file__).name

#: 10 delegates when this was written, across 7 modules. A floor, so a scanner that stops matching
#: the delegate shape fails here rather than passing with nothing to check.
EXPECTED_DELEGATE_FLOOR = 8


def _library_modules() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "tools/*.py"], cwd=REPO, capture_output=True, text=True).stdout.split()
    return [REPO / rel for rel in listed
            if not Path(rel).name.startswith("test_") and Path(rel).name != SELF]


def _module_tree(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except (SyntaxError, OSError):
        return None


def _import_aliases(tree: ast.Module) -> dict[str, tuple[str, str]]:
    """local name -> (module basename, name in that module), for `from X import a as b`."""
    aliases: dict[str, tuple[str, str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            source = node.module.rsplit(".", 1)[-1]
            for alias in node.names:
                if alias.name != "*":
                    aliases[alias.asname or alias.name] = (source, alias.name)
    return aliases


def _parameters(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    names = {a.arg for a in fn.args.posonlyargs + fn.args.args + fn.args.kwonlyargs}
    return names - {"self", "cls"}


def _accepts_anything(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """A **kwargs delegate forwards whatever it is given; there is nothing to compare."""
    return fn.args.kwarg is not None or fn.args.vararg is not None


def _forwarding_target(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """The name a delegate forwards to, or None if this is not a delegate.

    A delegate is a function whose body is a docstring and/or `del` statements followed by a single
    `return <Name>(...)`. The `del` statements are included deliberately: discarding a parameter and
    then forwarding is the exact shape this file exists to catch, and skipping those bodies would
    make the check blind to its own subject.
    """
    body = [s for s in fn.body
            if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant)
                    and isinstance(s.value.value, str))
            and not isinstance(s, ast.Delete)]
    if len(body) != 1 or not isinstance(body[0], ast.Return):
        return None
    call = body[0].value
    if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name):
        return None
    return call.func.id


def _forwarded_names(call: ast.Call) -> set[str]:
    """Every name that reaches the target, however it is passed.

    Walks each argument rather than reading its top level, so a parameter forwarded inside an
    expression -- `args or {}`, `int(limit)`, `dict(ranking)` -- counts as forwarded. Being wrong
    in that direction stays quiet about a parameter that is used; the opposite would report every
    delegate that tidies an argument on the way through.
    """
    names: set[str] = set()
    for node in list(call.args) + [k.value for k in call.keywords]:
        for inner in ast.walk(node):
            if isinstance(inner, ast.Name):
                names.add(inner.id)
    return names


def collect_delegates() -> list[tuple[str, str, set[str], set[str]]]:
    """(module, delegate name, parameters it accepts, names it forwards)."""
    found = []
    for path in _library_modules():
        tree = _module_tree(path)
        if tree is None:
            continue
        aliases = _import_aliases(tree)
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if _accepts_anything(node):
                continue
            target_name = _forwarding_target(node)
            if target_name is None or target_name not in aliases:
                continue
            call = [s for s in node.body if isinstance(s, ast.Return)][0].value
            found.append((path.stem, node.name, _parameters(node), _forwarded_names(call)))
    return found


class ADelegateAcceptsOnlyWhatItForwardsTest(unittest.TestCase):

    def test_the_scan_finds_delegates_to_check(self) -> None:
        """A guard that finds nothing passes hardest, so assert the extent before the rule."""
        delegates = collect_delegates()
        self.assertGreaterEqual(
            len(delegates), EXPECTED_DELEGATE_FLOOR,
            "the delegate scan found %d, below the floor of %d -- either the delegates were "
            "consolidated away (lower the floor and say so) or the shape stopped matching, in "
            "which case the rule below is checking nothing"
            % (len(delegates), EXPECTED_DELEGATE_FLOOR))

    def test_a_known_delegate_is_seen(self) -> None:
        """Mechanism control: the scan must actually reach the family this file came from."""
        seen = {(module, name) for module, name, _, _ in collect_delegates()}
        self.assertIn(
            ("matrixark_mcp_local_adapter", "auto_memory_layer_budget_tokens"), seen,
            "the scan no longer sees the budget delegates in matrixark_mcp_local_adapter, so a "
            "pass here says nothing about them")
        self.assertNotIn(
            ("matrixark_mcp_local_adapter", "zz_no_delegate_is_called_this"), seen,
            "the scan reports a delegate that does not exist")

    def test_the_rule_reads_a_discard_and_not_a_rename(self) -> None:
        """Positive control on the comparison itself, and on the case that must NOT be reported."""
        discards = ast.parse(
            "def d(*, budget, outcome_query=False):\n"
            "    del outcome_query\n"
            "    return target(budget=budget)\n").body[0]
        call = [s for s in discards.body if isinstance(s, ast.Return)][0].value
        self.assertEqual({"outcome_query"}, _parameters(discards) - _forwarded_names(call),
                         "the rule no longer reads a discarded parameter")

        renames = ast.parse(
            "def d(scope_key):\n"
            "    return target(scope_key)\n").body[0]
        call = [s for s in renames.body if isinstance(s, ast.Return)][0].value
        self.assertEqual(set(), _parameters(renames) - _forwarded_names(call),
                         "forwarding positionally under a different name is a rename, not a "
                         "discard, and reporting it would be the bug this rule was rewritten to "
                         "remove")

        tidied = ast.parse(
            "def d(args=None):\n"
            "    return target(args or {})\n").body[0]
        call = [s for s in tidied.body if isinstance(s, ast.Return)][0].value
        self.assertEqual(set(), _parameters(tidied) - _forwarded_names(call),
                         "a parameter tidied on the way through is still forwarded")

    def test_no_delegate_accepts_an_argument_it_does_not_forward(self) -> None:
        offenders = []
        for module, name, accepted, forwarded in collect_delegates():
            dropped = sorted(accepted - forwarded)
            if dropped:
                offenders.append("%s.%s accepts %s and does not pass %s on"
                                 % (module, name, dropped,
                                    "it" if len(dropped) == 1 else "them"))
        self.assertEqual(
            [], offenders,
            "a delegate accepts an argument it never forwards, so a caller can pass a computed "
            "value and have it discarded in silence:\n  " + "\n  ".join(offenders))


if __name__ == "__main__":
    unittest.main()

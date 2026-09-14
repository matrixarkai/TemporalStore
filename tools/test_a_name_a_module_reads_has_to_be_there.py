# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A name a module reads has to be there.

`matrixark_mcp_core_resource_io` called `compact_ws` on the failure path of `_aws_cli_s3_cp` and
never imported it. When an `aws s3 cp` actually failed, the line that builds the error out of the
subprocess's stderr raised `NameError: name 'compact_ws' is not defined` instead -- so the caller
got a confusing failure about a helper rather than "upload failed: the bucket does not exist".

`matrixark_local_adapter_summaries` is the one that matters most. Its dirty-summary refresh writes
an audit record under `if ENABLE_SUMMARY_REFRESH_AUDIT:`, and that record read a `version_hash` that
appeared exactly once in the whole file -- as that read. The flag defaults off
(`MATRIXARK_SUMMARY_REFRESH_AUDIT`), so turning on a documented audit knob made the refresh raise.
Its sibling writer in `matrixark_mcp_summary_runtime` computes the value inline, and that is what
the copy here now does.

The rest sat on the same kind of ground: `hashlib.sha256()` in a module that never imports
`hashlib`, three runner scripts calling `probe_reader` on the branch taken when a reader endpoint is
unreachable, a debug trace naming five things nothing binds. Every one is on a path nothing takes
until something else has already gone wrong, or behind a flag that is off -- which is exactly why
they survived. The happy path never names them, and neither does any test.

Two things this check has to get right, and each one inverted the answer while it was wrong:

* It must ask the LOADED module, not the source. These modules take names by `import *`, and a
  source-only scan calls every star-imported name unbound -- hundreds of false reports burying the
  real ones.
* It must collect what a scope binds BEFORE judging any read in it. A nested function may read a
  name its enclosing function assigns further down the file; Python binds that at call time. A
  single-pass walker reported five such names as missing, every one of them perfectly bound.

Annotations are skipped: under `from __future__ import annotations` they are never evaluated, and
every module checked here imports successfully, so a name missing from one cannot be being
evaluated.
"""

from __future__ import annotations

import ast
import builtins
import json
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

_BUILTINS = set(dir(builtins)) | {
    "__file__", "__name__", "__doc__", "__spec__", "__package__", "__builtins__", "__loader__",
    "__debug__",
}

#: Read, not bound, and deliberately left that way -- each with the reason.
#:
#: `idle_commit_schedule` is defined NOWHERE in the tree. The nearest name,
#: `idle_commit_scheduled_task_record`, takes different arguments, so binding it would be inventing
#: behaviour rather than restoring it. Its module is unreachable from production.
#:
#: The five in the debug trace are a rename that did not finish, and the finish is a guess:
#: `resource_path` sits beside a `pdf_path` bound two lines above, `resource_type` beside fixtures
#: that carry their own, and `import_message` and `extracted_resource_fact_events` have no
#: candidate at all. Guessing them would put invented values into a trace whose whole purpose is to
#: report what happened.
RECORDED_UNBOUND = {
    ("matrixark_mcp_local_ingest", "idle_commit_schedule"),
    ("run_matrixark_message_pdf_debug_trace", "extracted_resource_fact_events"),
    ("run_matrixark_message_pdf_debug_trace", "import_message"),
    ("run_matrixark_message_pdf_debug_trace", "resource_path"),
    ("run_matrixark_message_pdf_debug_trace", "resource_type"),
}

_PROBE = """
import sys, json
sys.path.insert(0, {tools!r})
out = {{}}
for mod, names in {payload!r}:
    try:
        m = __import__(mod)
    except Exception as exc:
        out[mod] = {{"__error__": "%s: %s" % (type(exc).__name__, exc)}}
        continue
    missing = [n for n in names if not hasattr(m, n)]
    if missing:
        out[mod] = missing
print(json.dumps(out))
"""


def _shallow_bindings(body) -> set:
    """Every name a scope binds, without descending into nested function or class bodies.

    Collected before any read in that scope is judged: a nested function may read a name assigned
    further down, and Python resolves that at call time, not in source order.
    """
    bound: set = set()

    def walk(node) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                bound.add(child.name)          # the name it binds, not what is inside it
                continue
            if isinstance(child, ast.Lambda):
                continue
            if isinstance(child, (ast.Import, ast.ImportFrom)):
                for alias in child.names:
                    bound.add((alias.asname or alias.name).split(".")[0])
                continue
            if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
                bound.add(child.id)
            if isinstance(child, (ast.Global, ast.Nonlocal)):
                bound.update(child.names)
            if isinstance(child, ast.ExceptHandler) and child.name:
                bound.add(child.name)
            walk(child)

    for statement in body:
        walk(statement)
    return bound


class _FreeNames(ast.NodeVisitor):
    """Names read that no enclosing scope binds."""

    def __init__(self) -> None:
        self.free: dict = {}
        self._stack = [set()]

    def _bind(self, name: str) -> None:
        self._stack[-1].add(name)

    def _bound(self, name: str) -> bool:
        return any(name in frame for frame in self._stack)

    def visit_FunctionDef(self, node) -> None:
        self._bind(node.name)
        for decorator in node.decorator_list:
            self.visit(decorator)
        args = node.args
        for default in list(args.defaults) + [d for d in args.kw_defaults if d]:
            self.visit(default)
        self._stack.append(set())
        for arg in list(args.args) + list(args.posonlyargs) + list(args.kwonlyargs):
            self._bind(arg.arg)
        if args.vararg:
            self._bind(args.vararg.arg)
        if args.kwarg:
            self._bind(args.kwarg.arg)
        for name in _shallow_bindings(node.body):
            self._bind(name)
        for statement in node.body:
            self.visit(statement)
        self._stack.pop()
        # node.returns and each arg.annotation are deliberately not visited

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Lambda(self, node) -> None:
        self._stack.append(set())
        for arg in list(node.args.args) + list(node.args.kwonlyargs):
            self._bind(arg.arg)
        self.visit(node.body)
        self._stack.pop()

    def visit_ClassDef(self, node) -> None:
        self._bind(node.name)
        for decorator in node.decorator_list:
            self.visit(decorator)
        for base in node.bases:
            self.visit(base)
        self._stack.append(set())
        for name in _shallow_bindings(node.body):
            self._bind(name)
        for statement in node.body:
            self.visit(statement)
        self._stack.pop()

    def _comprehension(self, node) -> None:
        self._stack.append(set())
        for generator in node.generators:
            self.visit(generator.iter)
            for target in ast.walk(generator.target):
                if isinstance(target, ast.Name):
                    self._bind(target.id)
            for condition in generator.ifs:
                self.visit(condition)
        for field in ("elt", "key", "value"):
            child = getattr(node, field, None)
            if child is not None:
                self.visit(child)
        self._stack.pop()

    visit_ListComp = visit_SetComp = visit_GeneratorExp = visit_DictComp = _comprehension

    def visit_AnnAssign(self, node) -> None:
        if node.target is not None:
            self.visit(node.target)
        if node.value is not None:
            self.visit(node.value)

    def visit_Import(self, node) -> None:
        for alias in node.names:
            self._bind((alias.asname or alias.name).split(".")[0])

    def visit_ImportFrom(self, node) -> None:
        for alias in node.names:
            self._bind(alias.asname or alias.name)

    def visit_Global(self, node) -> None:
        for name in node.names:
            self._bind(name)

    visit_Nonlocal = visit_Global

    def visit_ExceptHandler(self, node) -> None:
        if node.type:
            self.visit(node.type)
        if node.name:
            self._bind(node.name)
        for statement in node.body:
            self.visit(statement)

    def visit_Name(self, node) -> None:
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self._bind(node.id)
        elif not self._bound(node.id) and node.id not in _BUILTINS:
            self.free.setdefault(node.id, node.lineno)


def _candidates() -> dict:
    """module -> {name: line}, for global names its own source never binds."""
    found = {}
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        walker = _FreeNames()
        for name in _shallow_bindings(tree.body):
            walker._bind(name)
        for statement in tree.body:
            walker.visit(statement)
        if walker.free:
            found[path.stem] = walker.free
    return found


def _unbound():
    """Candidates still missing once the module is imported for real."""
    candidates = _candidates()
    payload = sorted((module, sorted(names)) for module, names in candidates.items())
    if not payload:
        return set(), {}, 0, {}
    code = _PROBE.format(tools=str(TOOLS), payload=payload)
    result = subprocess.run([sys.executable, "-B", "-c", code],
                            capture_output=True, text=True, timeout=1800)
    lines = result.stdout.strip().splitlines()
    if not lines:
        raise AssertionError("the import probe produced nothing: %s"
                             % (result.stderr.strip().splitlines() or ["<none>"])[-1])
    got = json.loads(lines[-1])
    unbound, failures = set(), {}
    for module, missing in got.items():
        if isinstance(missing, dict):
            failures[module] = missing.get("__error__", "?")
            continue
        for name in missing:
            unbound.add((module, name))
    return unbound, failures, len(payload), candidates


class ANameAModuleReadsHasToBeThereTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.unbound, cls.failures, cls.checked, cls.candidates = _unbound()

    def test_the_scan_had_modules_to_check(self) -> None:
        """Vacuity floor on the SCAN. With no candidates the result set is empty, and an empty
        result set is exactly what success looks like here -- the two are indistinguishable."""
        self.assertGreater(
            self.checked, 20,
            "only %d modules had a global name their own source does not bind. Modules here take "
            "names by `import *`, so that number is normally in the hundreds; if it is not, the "
            "walker is binding everything and this file reports clean whatever is in the tree."
            % self.checked)

    def test_the_walker_still_resolves_closures(self) -> None:
        """Positive control on the WALKER, in the direction that produces false alarms. A nested
        function reading a name its enclosing scope assigns further down is bound at call time; a
        walker that forgets this reports working code as broken, which is how this check gets
        switched off."""
        source = (
            "def outer():\n"
            "    def inner():\n"
            "        return later_value\n"
            "    later_value = 1\n"
            "    return inner()\n"
        )
        tree = ast.parse(source)
        walker = _FreeNames()
        for name in _shallow_bindings(tree.body):
            walker._bind(name)
        for statement in tree.body:
            walker.visit(statement)
        self.assertNotIn(
            "later_value", walker.free,
            "the walker calls a name free that its enclosing scope binds further down. Every "
            "report it makes is now suspect, and the recorded set below will not match.")

    def test_the_walker_still_sees_a_genuinely_missing_name(self) -> None:
        """The other half of the control: a walker that binds everything finds nothing at all."""
        tree = ast.parse("def f():\n    return nothing_binds_this\n")
        walker = _FreeNames()
        for name in _shallow_bindings(tree.body):
            walker._bind(name)
        for statement in tree.body:
            walker.visit(statement)
        self.assertIn(
            "nothing_binds_this", walker.free,
            "the walker no longer reports a name nothing binds, so it cannot find the defect "
            "this file exists for.")

    def test_every_candidate_module_could_be_imported(self) -> None:
        """A module that fails to import is never checked, and drops out of the result silently --
        the same shape as a truncated search reading like a clean one."""
        self.assertEqual(
            {}, self.failures,
            "these modules could not be imported, so their reads were never checked: %s"
            % "; ".join("%s (%s)" % item for item in sorted(self.failures.items())))

    def test_no_read_names_something_that_is_not_there(self) -> None:
        new = self.unbound - RECORDED_UNBOUND
        self.assertEqual(
            set(), new,
            "these reads name something the loaded module does not have, and raise NameError when "
            "the line runs: %s. Error paths and flag-gated branches are where this hides -- "
            "nothing takes them until something else is wrong, or until an operator turns a knob."
            % ", ".join("%s reads %s" % pair for pair in sorted(new)))

    def test_every_recorded_departure_is_still_a_departure(self) -> None:
        """The other direction. A recorded exception that gets fixed has to fail this, or it sits
        there granting an exemption to nothing."""
        resolved = RECORDED_UNBOUND - self.unbound
        self.assertEqual(
            set(), resolved,
            "%s no longer unbound. Good news: drop it from RECORDED_UNBOUND so the entry stops "
            "granting an exemption nothing needs."
            % ", ".join("%s.%s" % pair for pair in sorted(resolved)))


if __name__ == "__main__":
    unittest.main()

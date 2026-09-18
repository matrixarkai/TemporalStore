#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A guarded import asks its module for names the module actually defines.

Every module-level import in this tree is written

    try:
        from tools.matrixark_x import alpha, beta
    except ImportError:
        from matrixark_x import alpha, beta

because a module is loaded as `tools.X` by some entry points and as bare `X` by others.
`test_a_sibling_import_inside_a_function_needs_both_spellings` guards the SHAPE of that idiom --
that both spellings are written. This guards what the two spellings ASK FOR.

THE DEFECT THIS EXISTS FOR. ``from X import name`` raises ``ImportError`` when the module is
there and the NAME is not -- the same exception a missing module raises. So a handler written for
"this deployment does not have that module" also swallows "somebody renamed that function", and
the fallback becomes the only path with nothing said.

What makes it hard to see by reading is that the fallback is usually *fine*. It sets a sentinel,
imports a stdlib parser, or defines a stand-in -- so the module loads, the suite passes, and the
only symptom is a feature quietly not happening. The optional storage backends in
`matrixark_mcp_core_resource_io` are the sharpest case: a renamed constant there leaves
``_obj_backend = None``, ``obj_ok`` False, and every attachment written to a different tier with
no error raised anywhere.

So this asks the question the handler cannot: for each guarded import of a module this repository
ships, does that module define every name being asked of it? Today, for all of them, yes.

## Reading a module's surface, and the cycle that makes it subtle

``import *`` re-exports names the starred module itself imported, so the surface of
`matrixark_mcp_core` is mostly supplied by the dozen ``from matrixark_mcp_core_<part> import *``
lines at its end. A collector that reads only what a module writes literally calls 68 of these
absent when none of them is -- `test_the_star_expansion_is_doing_something` pins that.

But those modules point BOTH ways, and that is the trap. `matrixark_mcp_core_resource_io` opens
by importing four names from core, and core ends by star-importing resource_io. Count a
submodule's import list while resolving its parent and every name the submodule asks the parent
for appears to exist in the parent -- a misspelt one included, which is precisely what this file
looks for. A planted rename passed unnoticed until `_resolving` was added below.
"""
from __future__ import annotations

import ast
import io
import os
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: A floor on the scan, not on the finding. 753 statements match today; well under that catches a
#: scan that stopped matching the idiom, which would make the zero below mean nothing at all.
MINIMUM_GUARDED_IMPORTS = 400


def _strip_tools_prefix(module: str | None) -> str:
    """`tools.matrixark_x` and `matrixark_x` name the same file; `tools` alone names none."""
    module = module or ""
    if module == "tools":
        return ""
    return module[len("tools."):] if module.startswith("tools.") else module


def _handler_catches_import_error(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return True
    types = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    return any(isinstance(t, ast.Name) and t.id in ("ImportError", "Exception", "BaseException")
               for t in types)


def _module_top_level_names(root: str, module: str, cache: dict,
                            _resolving=frozenset(), expand: bool = True) -> set | None:
    """Every name `module` binds at import, with `from X import *` expanded through.

    None when the repository does not ship the module -- the case the handler is actually FOR,
    and the only one this file has nothing to say about.

    `_resolving` is the chain of modules whose surface is being computed, and an import FROM one
    of those does not count towards it. See the cycle described in the module docstring: without
    this, `matrixark_mcp_core` inherits every name `matrixark_mcp_core_resource_io` asks it for.
    """
    if module in _resolving:
        return set()                      # a cycle; the other half of it supplies the names
    top = not _resolving
    if top and expand and module in cache:
        return cache[module]
    path = os.path.join(root, module + ".py")
    if not os.path.exists(path):
        return None
    try:
        with io.open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
    except (SyntaxError, OSError):
        return None

    chain = _resolving | {module}
    names: set = set()
    stars: set = set()

    def take(statements) -> None:
        for node in statements:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        names.add(target.id)
                    elif isinstance(target, (ast.Tuple, ast.List)):
                        names.update(e.id for e in target.elts if isinstance(e, ast.Name))
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
            elif isinstance(node, ast.ImportFrom):
                source = _strip_tools_prefix(node.module)
                if any(a.name == "*" for a in node.names):
                    stars.add(source)
                elif source not in chain:
                    names.update(a.asname or a.name for a in node.names)
            elif isinstance(node, ast.Import):
                names.update(a.asname or a.name.split(".")[0] for a in node.names)
            elif isinstance(node, (ast.Try, ast.If)):
                # Both arms bind: that is the whole point of the idiom, and an `if` at module
                # level here is a capability check whose branches each supply the same name.
                take(node.body)
                take(getattr(node, "orelse", []))
                for handler in getattr(node, "handlers", []):
                    take(handler.body)

    take(tree.body)
    for starred in stars if expand else ():
        if starred and "." not in starred:
            more = _module_top_level_names(root, starred, cache, chain)
            if more:
                names |= more
    if top and expand:
        cache[module] = names
    return names


def _guarded_from_imports(root: str, files):
    """(file, line, module, names) for each `from <module> import ...` under an ImportError."""
    for rel in files:
        try:
            with io.open(os.path.join(root, rel), encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (SyntaxError, OSError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Try):
                continue
            if not any(_handler_catches_import_error(h) for h in node.handlers):
                continue
            for statement in ast.walk(node):
                if not isinstance(statement, ast.ImportFrom):
                    continue
                module = _strip_tools_prefix(statement.module)
                if not module or "." in module:
                    continue                       # a package or a third-party dependency
                wanted = {a.name for a in statement.names if a.name != "*"}
                if wanted:
                    yield rel, statement.lineno, module, wanted


def _production_files(root: str):
    return sorted(f for f in os.listdir(root)
                  if f.endswith(".py") and not f.startswith("test_"))


def _missing(root: str, files, expand_stars: bool = True):
    """Every guarded import asking a shipped module for a name it does not define."""
    cache: dict = {}
    out, examined = [], 0
    for rel, line, module, wanted in _guarded_from_imports(root, files):
        have = _module_top_level_names(root, module, cache, expand=expand_stars)
        if have is None:
            continue                               # not shipped here: the handler's real case
        examined += 1
        absent = sorted(wanted - have)
        if absent:
            out.append("%s:%d asks %s for %s" % (rel, line, module, ", ".join(absent)))
    return examined, out


class AGuardedImportAsksForNamesThatExistTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.files = _production_files(TOOLS)
        cls.examined, cls.absent = _missing(TOOLS, cls.files)

    def test_the_scan_still_matches_the_idiom(self) -> None:
        """Vacuity floor. A scan matching nothing reports no missing names either."""
        self.assertGreater(
            self.examined, MINIMUM_GUARDED_IMPORTS,
            "only %d guarded imports of a shipped module matched, below the floor of %d. The "
            "idiom has changed shape or the scan has stopped reading it, and the check below is "
            "then passing on an empty set." % (self.examined, MINIMUM_GUARDED_IMPORTS))

    def test_every_guarded_import_asks_for_a_name_the_module_defines(self) -> None:
        self.maxDiff = None
        self.assertEqual(
            [], self.absent,
            "a guarded import asks for a name its module does not define. `from X import name` "
            "raises ImportError for the missing NAME exactly as it does for a missing MODULE, so "
            "the handler beside it swallows this and the fallback becomes the only path with "
            "nothing said. Either restore the name or move the import out from under the "
            "handler:\n  " + "\n  ".join(self.absent))

    def test_the_scan_would_notice_a_missing_name(self) -> None:
        """The control, run through the same two functions the check above uses.

        A planted rename must be reported and the legal spellings beside it must not, or the zero
        is a scan that cannot see anything rather than a tree that is clean. The fourth fixture is
        the cycle from the module docstring, in miniature: it is the one that used to pass.
        """
        with tempfile.TemporaryDirectory() as root:
            def write(name: str, body: str) -> None:
                with io.open(os.path.join(root, name), "w", encoding="utf-8") as handle:
                    handle.write(body)

            write("mod_a.py", "ALPHA = 1\ndef beta():\n    pass\n"
                              "from mod_b import *\nfrom cyclic import *\n")
            write("mod_b.py", "GAMMA = 2\n")
            write("ok_direct.py",
                  "try:\n    from tools.mod_a import ALPHA, beta\n"
                  "except ImportError:\n    from mod_a import ALPHA, beta\n")
            write("ok_through_a_star.py",
                  "try:\n    from tools.mod_a import GAMMA\n"
                  "except ImportError:\n    from mod_a import GAMMA\n")
            write("ok_module_not_shipped.py",
                  "try:\n    from mod_nowhere import anything\n"
                  "except ImportError:\n    anything = None\n")
            write("cyclic.py",
                  "try:\n    from tools.mod_a import ALPHA, epsilon\n"
                  "except ImportError:\n    ALPHA = None\n")

            examined, absent = _missing(root, _production_files(root))
            # Five, not four: both arms of a dual-import block are separate statements and
            # either can ask for the wrong name, so the scan reads each of them. The unshipped
            # module contributes none.
            self.assertEqual(5, examined, "the fixture's own denominator moved")
            self.assertEqual(1, len(absent), absent)
            self.assertIn("cyclic.py", absent[0])
            self.assertIn("epsilon", absent[0])

    def test_the_star_expansion_is_doing_something(self) -> None:
        """Without it the same scan reports names that are plainly there.

        `matrixark_mcp_core` publishes most of its surface through `from ..._packing import *` and
        a dozen more like it. A collector reading only what a module writes literally calls those
        absent -- so if switching the expansion off changes nothing, the expansion has stopped
        running and the zero above is measuring a collector that over-collects instead.
        """
        _, naive = _missing(TOOLS, self.files, expand_stars=False)
        self.assertGreater(
            len(naive), 0,
            "turning the star expansion off produced the same answer as leaving it on, so it is "
            "not what makes the main check true and one of the two is not doing what it says.")


if __name__ == "__main__":
    unittest.main()

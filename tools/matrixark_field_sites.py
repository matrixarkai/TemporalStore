#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Count the places that read and write a record field, by AST.

Before changing what a record stores, the question that decides whether the change is tractable is
"how many places touch this field?" -- and answering it by eye is unreliable. Three changes in a row
were shipped here on the belief that one routine wrote a field, each time because a comment or a
call graph said so; the field was written in four places, then five, then six.

A grep does not answer it either: it counts imports, definitions, comments and docstrings alongside
real accesses, and it cannot tell a READ from a WRITE. That distinction is the one that matters,
because a write-side change needs every writer and a read-side change needs every reader, and the
two counts are usually nothing alike.

    $ python3 tools/matrixark_field_sites.py storage_record_kind
    storage_record_kind
      reads   9   across 5 module(s)
      writes 13   across 6 module(s)

    $ python3 tools/matrixark_field_sites.py --detail source_roles
    ... every site, with the expression it is read from

What counts as what:

* a READ is ``x.get("field")`` or ``x["field"]`` -- and the expression ``x`` is reported, because
  reading a name off a request is a different thing from reading it off a stored record
* a WRITE is the field appearing as a key in a dict literal, or ``x["field"] = ...``

Tests live in ``test_matrixark_field_sites.py``.
"""
from __future__ import annotations

import argparse
import ast
import collections
import os
import sys

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))


def _subject(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # noqa: BLE001 - a subject we cannot render is still a site.
        return "?"


def field_sites(field: str, *, root: str | None = None, include_tests: bool = False) -> dict:
    """Every read and write of ``field`` under ``root``, as ast nodes rather than text matches."""
    root = root or TOOLS_DIR
    reads: list[tuple[str, int, str]] = []
    writes: list[tuple[str, int, str]] = []

    for name in sorted(os.listdir(root)):
        if not name.endswith(".py"):
            continue
        if not include_tests and name.startswith("test_"):
            continue
        path = os.path.join(root, name)
        try:
            with open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue

        # `ast.walk` yields every node, so an assignment TARGET is reached twice: once through the
        # Assign that makes it a write, and once on its own, where it looks like a read. Collect the
        # targets first and skip them by identity.
        assigned = {
            id(target)
            for node in ast.walk(tree) if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Subscript)
        }

        for node in ast.walk(tree):
            if id(node) in assigned:
                continue
            # x.get("field") / x.get("field", default)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
               and node.func.attr in {"get", "setdefault", "pop"} and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and first.value == field:
                    where = reads if node.func.attr == "get" else writes
                    where.append((name, node.lineno, _subject(node.func.value)))
                continue
            # x["field"] -- a write when it is an assignment target, a read otherwise
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Subscript) \
                       and isinstance(target.slice, ast.Constant) \
                       and target.slice.value == field:
                        writes.append((name, target.lineno, _subject(target.value)))
                continue
            if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant) \
               and node.slice.value == field:
                reads.append((name, node.lineno, _subject(node.value)))
                continue
            # {"field": ...}
            if isinstance(node, ast.Dict):
                for key in node.keys:
                    if isinstance(key, ast.Constant) and key.value == field:
                        writes.append((name, node.lineno, "dict literal"))

    return {"field": field, "reads": reads, "writes": writes}


def _render(result: dict, *, detail: bool) -> str:
    reads, writes = result["reads"], result["writes"]
    read_mods = {r[0] for r in reads}
    write_mods = {w[0] for w in writes}
    lines = [result["field"],
             f"  reads  {len(reads):4}   across {len(read_mods)} module(s)",
             f"  writes {len(writes):4}   across {len(write_mods)} module(s)"]
    if detail:
        for label, sites in (("read", reads), ("write", writes)):
            for module, lineno, subject in sorted(sites):
                lines.append(f"    {label:5} {module}:{lineno}   on: {subject[:60]}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("field", nargs="+", help="record field name(s) to count")
    parser.add_argument("--detail", action="store_true", help="list every site")
    parser.add_argument("--root", default=None, help="directory to scan (default: tools/)")
    parser.add_argument("--include-tests", action="store_true")
    args = parser.parse_args(argv)

    for field in args.field:
        result = field_sites(field, root=args.root, include_tests=args.include_tests)
        print(_render(result, detail=args.detail))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A launcher default that is not the code default has to be on a list.

`test_a_shell_default_agrees_with_the_shipped_config` asks this of `config/temporalstore.toml`.
That covers 14 of the 145 variables the launchers export with a default. The other 131 have no
config entry at all, and the comparison that matters for them is against the default written in the
CODE -- the engine's `unwrap_or`, or a Python reader's second argument.

`MATRIXARK_RUST_PROXY_ASYNC_STORAGE` is why. The engine defaults it off and says in a comment beside
the line that async "must never be the deployed front-door default"; three launchers export `true`.
`test_the_deployed_durability_default_is_the_opposite_one` pins that one flag. This asks the same
question of the whole launcher surface, so the next one does not need its own file.

**None of these are bugs on their own.** A launcher choosing a deployment profile is what a launcher
is for. What is worth holding still is the SET: a new disagreement should arrive with a sentence
saying why, and one that stops disagreeing should be struck off rather than left describing a tree
that has moved on.

Three exclusions, each counted and asserted rather than quietly applied, because an exclusion that
grows until nothing is left reads exactly like a clean tree:

* variables named in the shipped config -- the other guard's territory;
* a code default of `""` -- that is the absence of a default, and a launcher supplying a value
  there is the launcher doing its job;
* a Python default that is not a literal (a name or an expression), which cannot be compared as
  text without evaluating it.
"""

from __future__ import annotations

import ast
import pathlib
import re
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
ROOT = TOOLS.parent
CONFIG = ROOT / "config" / "temporalstore.toml"
CRATES = ROOT / "crates"

#: `NAME="${NAME:-default}"`, with or without `export`.
_EXPORT = re.compile(r'(?:export\s+)?([A-Z][A-Z0-9_]*)="\$\{\1:-([^}"]*)\}"')

#: `env::var("NAME") ... .unwrap_or(default)`, within one statement.
#:
#: 400 characters, not 120: the async-storage read spans four lines and about 135 characters
#: between the `var(` and its `unwrap_or`, and a 120-character window silently dropped it -- which
#: is the whole reason the control below names that flag. `[^;]` is what keeps the window inside
#: one statement rather than reaching an unrelated `unwrap_or` further down the file.
_RUST = re.compile(r'var\(\s*"([A-Z][A-Z0-9_]*)"\s*\)[^;]{0,400}?unwrap_or\(\s*([^)\s]+)\s*\)')

_READERS = {"env_bool", "env_int", "env_str", "get"}

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}

#: The flag whose contradiction is known, documented and separately pinned. If the scan stops
#: finding it, the scan has stopped working -- and a scan that has stopped working reports a clean
#: tree, which is indistinguishable from the real thing without this.
CONTROL_FLAG = "MATRIXARK_RUST_PROXY_ASYNC_STORAGE"

#: Launcher default, code default, and why the two differ.
#:
#: Keyed by (variable, launcher) because one variable can be exported by several launchers with
#: different intent, and collapsing them would let one launcher's change hide behind another's.
RECORDED: dict = {
    ("MATRIXARK_RUST_PROXY_ASYNC_STORAGE", "matrixark_claude_hook.sh"):
        "the engine inherits the durable library default (every write fsync-committed before it "
        "is acked) and says beside the line that async must never be the deployed front-door "
        "default; the agent launchers opt in for throughput. The contradiction is held still by "
        "test_the_deployed_durability_default_is_the_opposite_one, which carries the argument.",
    ("MATRIXARK_RUST_PROXY_ASYNC_STORAGE", "matrixark_codex_dual_hook.sh"): "as above",
    ("MATRIXARK_RUST_PROXY_ASYNC_STORAGE", "matrixark_codex_rust_hook.sh"): "as above",
    ("MATRIXARK_DIRECT_WRITE_QUEUE", "matrixark_codex_dual_hook.sh"):
        "the dual hook runs a queued-write profile -- the same block exports "
        "MATRIXARK_HOOK_FAST_ASYNC_INGEST=1 and MATRIXARK_HOOK_STORAGE_ROUTE=shared_store_async -- "
        "while the adapter defaults the queue off so an embedder gets synchronous writes unless "
        "it asks otherwise.",
    ("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "matrixark_codex_dual_hook.sh"):
        "the same profile: with the queue on, the dual hook keeps it in TemporalStore rather than "
        "in process memory, so a hook that exits does not take the queued writes with it. The "
        "adapter's default is memory, which is the right default for a caller that has not asked "
        "for a queue at all.",
    ("MATRIXARK_RETRIEVAL_TIMEOUT_MS", "matrixark_mcp_rust_server.sh"):
        "the code default of 0 means NO stage budgeting -- default_stage_budgets applies a budget "
        "only when deadline_ms > 0 -- and the server launcher turns it on with a 20s deadline. So "
        "the deployed default has stage budgeting and the library default does not.",
}


def _clean(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip().strip('"').strip("'").strip()


def _same(left, right) -> bool:
    left, right = _clean(left), _clean(right)
    left_digits, right_digits = left.replace("_", ""), right.replace("_", "")
    if left_digits.isdigit() and right_digits.isdigit():
        return int(left_digits) == int(right_digits)      # Rust writes 40_000 for 40000
    if left.lower() in _TRUE and right.lower() in _TRUE:
        return True
    if left.lower() in _FALSE and right.lower() in _FALSE:
        return True
    return left.lower() == right.lower()


def _launcher_defaults() -> dict:
    found: dict = {}
    for path in sorted(TOOLS.glob("*.sh")):
        text = path.read_text(encoding="utf-8", errors="replace")
        for name, default in _EXPORT.findall(text):
            found.setdefault(name, {})[path.name] = default.strip()
    return found


def _python_defaults():
    """(literal defaults, names whose default is not a literal), read from the AST.

    Regex found a default of `0",` here once -- the pattern ran past the closing quote -- and a
    table built on that carries the parser's mistakes as recorded facts.
    """
    literal: dict = {}
    nonliteral: set = set()
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) < 2:
                continue
            func = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else None)
            if func not in _READERS:
                continue
            key = node.args[0]
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                continue
            name = key.value
            if not re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
                continue
            if not isinstance(node.args[1], ast.Constant):
                nonliteral.add(name)
                continue
            literal.setdefault(name, (node.args[1].value, path.name))
    return literal, nonliteral


def _rust_defaults() -> dict:
    found: dict = {}
    if not CRATES.exists():
        return found
    for path in CRATES.rglob("*.rs"):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for match in _RUST.finditer(text):
            found.setdefault(match.group(1), (match.group(2).strip().rstrip(","), path.name))
    return found


def _survey():
    launchers = _launcher_defaults()
    python_literal, python_nonliteral = _python_defaults()
    rust = _rust_defaults()
    config_text = CONFIG.read_text(encoding="utf-8", errors="replace") if CONFIG.exists() else ""
    in_config = {name for name in launchers if name in config_text}

    contradictions: dict = {}
    excluded = {"in_config": len(in_config), "no_code_default": 0, "interpolated": 0,
                "nonliteral": 0}
    for name, exports in sorted(launchers.items()):
        if name in in_config:
            continue
        if name in python_nonliteral and name not in rust:
            excluded["nonliteral"] += 1
            continue
        code = rust.get(name) or python_literal.get(name)
        if not code:
            continue
        code_value, where = code
        if _clean(code_value) in ("", "None", "null"):
            excluded["no_code_default"] += 1
            continue
        for launcher, shell_value in sorted(exports.items()):
            if "$" in shell_value:
                excluded["interpolated"] += 1
                continue
            if not _same(shell_value, code_value):
                contradictions[(name, launcher)] = (_clean(shell_value), _clean(code_value), where)
    return launchers, python_literal, rust, contradictions, excluded


class ALauncherDefaultThatIsNotTheCodeDefaultSaysSoTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        (cls.launchers, cls.python_defaults, cls.rust_defaults,
         cls.contradictions, cls.excluded) = _survey()

    def test_the_scan_still_reads_all_three_surfaces(self) -> None:
        """Vacuity floor. Any one of these going empty makes the comparison below pass by having
        nothing to compare, which reads exactly like agreement."""
        exports = sum(len(v) for v in self.launchers.values())
        self.assertGreater(exports, 100,
                           "only %d launcher exports with a default were parsed; the tree has far "
                           "more, so the shell pattern has stopped matching" % exports)
        self.assertGreater(len(self.python_defaults), 100,
                           "only %d Python defaults were parsed out of the AST"
                           % len(self.python_defaults))
        self.assertGreater(len(self.rust_defaults), 10,
                           "only %d Rust defaults were parsed; the window or the pattern has "
                           "stopped matching" % len(self.rust_defaults))

    def test_the_known_contradiction_is_still_found(self) -> None:
        """Named positive control. A 120-character window silently dropped this one while every
        other number stayed plausible -- 41 Rust defaults instead of 32 is not a figure anyone
        would question. The scan has to prove it can still see the case everybody agrees is there.
        """
        self.assertIn(
            CONTROL_FLAG, self.rust_defaults,
            "the scan no longer finds %s in the Rust source. It is known to be read there with a "
            "default of false, so this is the scan failing rather than the tree agreeing."
            % CONTROL_FLAG)
        self.assertEqual("false", _clean(self.rust_defaults[CONTROL_FLAG][0]),
                         "%s now reads %r in Rust rather than false; if the engine changed its "
                         "durability default that is a much larger change than this file"
                         % (CONTROL_FLAG, self.rust_defaults[CONTROL_FLAG][0]))
        found = [key for key in self.contradictions if key[0] == CONTROL_FLAG]
        self.assertTrue(found,
                        "%s is read with a default of false and exported true by launchers, and "
                        "the comparison reports no contradiction -- so the comparison is broken"
                        % CONTROL_FLAG)

    def test_the_exclusions_have_not_swallowed_the_surface(self) -> None:
        """An exclusion that grows until nothing is left reads exactly like a clean tree."""
        total = sum(self.excluded.values())
        considered = sum(len(v) for v in self.launchers.values())
        self.assertLess(
            total, considered / 2,
            "%d of %d launcher exports are being excluded (%s). The comparison is running on "
            "less than half the surface and its silence means less than it looks."
            % (total, considered, self.excluded))

    def test_no_new_launcher_default_contradicts_the_code_without_saying_why(self) -> None:
        new = sorted(set(self.contradictions) - set(RECORDED))
        self.assertEqual(
            [], new,
            "these launchers export a default the code does not agree with, and no entry says "
            "why: %s. A launcher choosing a deployment profile is fine -- add it to RECORDED with "
            "the reason, so the next reader knows it was decided rather than drifted."
            % "; ".join("%s in %s (launcher %r, code %r from %s)"
                        % (name, launcher, self.contradictions[(name, launcher)][0],
                           self.contradictions[(name, launcher)][1],
                           self.contradictions[(name, launcher)][2])
                        for name, launcher in new))

    def test_a_recorded_contradiction_that_now_agrees_is_struck_off(self) -> None:
        """The other direction. A list allowed to rot describes a tree that no longer exists."""
        resolved = sorted(set(RECORDED) - set(self.contradictions))
        self.assertEqual(
            [], resolved,
            "these are recorded as contradictions and no longer contradict: %s. Either the "
            "launcher moved to the code default or the code moved to the launcher's -- either way "
            "somebody decided something. Strike the entry."
            % "; ".join("%s in %s" % pair for pair in resolved))


if __name__ == "__main__":
    unittest.main()

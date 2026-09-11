#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two readers of one variable should agree about its STRING default.

`test_flag_readers_agree` asks this of booleans and `test_numeric_defaults_agree` of numbers. A
string was covered by neither, and drifts exactly the same way: two readers agree on every value
anyone sets and part company only for the deployment that leaves the variable alone -- which is most
of them, and the one nobody tests.

The worked example is why this file exists. `MATRIXARK_NAMESPACE` and `MATRIXARK_TABLE` resolved to
"matrixark" and "context" in two tools while `config/temporalstore.toml` declared `deploy_ns` and
`deploy_table` for those exact names, and every running process carried the second pair. Nothing in
the repository named the first pair at all. A backfill run without `--namespace` therefore addressed
a store the deployment does not read -- and reported having done so, because writing into an empty
namespace succeeds. Struck in mx#1056.

Most differences here are not that. A variable that names a PROVIDER or an AGENT legitimately
resolves differently per reader: an API base is per provider, and the two agent hooks are two
tenants on purpose. Each is listed below with what makes it legitimate, so the next one has to be
looked at rather than joining them quietly.

The set is asserted exactly: a NEW disagreement fails here, and a listed variable that stops
disagreeing fails too, because a list allowed to rot describes a tree that no longer exists.
"""
from __future__ import annotations

import ast
import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_NAME = re.compile("(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+")

#: Values that are really booleans or numbers wearing quotes. Their own guards own them.
_NOT_A_STRING = {"", "0", "1", "true", "false", "yes", "no", "on", "off",
                 "TRUE", "FALSE", "YES", "NO", "ON", "OFF", "True", "False"}

#: Variables whose readers legitimately choose different strings, and what makes it legitimate.
KNOWN_DISAGREEMENTS: Dict[str, str] = {
    "MATRIXARK_ACCESS_MODE":
        "the entry points are not peers on this: see test_the_two_asgi_fronts_disagree_on_purpose, "
        "which declares each front's default and the reason",
    "MATRIXARK_ACCOUNT_ID":
        "acct_agent and acct_codex -- the two agent hooks are two accounts, which is the point",
    "MATRIXARK_TENANT_ID":
        "tenant_agent and tenant_codex, the same separation one level down",
    "MATRIXARK_TEAM":
        "agent and codex, naming which hook is writing",
    "MATRIXARK_TEMPORALSTORE_PREFIX":
        "matrixark:agent-hook, matrixark:codex-hook and matrixark:mcp -- a key prefix that exists "
        "to keep the three writers apart",
    "MATRIXARK_USER_ID":
        "agent_user and default -- the agent hook names the user it writes as, the batch ingest "
        "tool names its own, the same per-writer separation as the three entries above. It became "
        "visible when the hook's read stopped nesting its literal behind a USERNAME fallback; the "
        "difference is older than that.",
    "MATRIXARK_EMBEDDING_API_BASE":
        "the OpenAI and Voyage endpoints, chosen in one function by which provider is configured",
    "MATRIXARK_EMBEDDING_API_KEY_ENV":
        "OPENAI_API_KEY and VOYAGE_API_KEY, picked beside the base above",
    "MATRIXARK_EXTRACTION_API_KEY_ENV":
        "OPENAI_API_KEY and ANTHROPIC_API_KEY, the same shape for the extraction provider",
    "MATRIXARK_UNDERSTANDING_PROVIDER":
        "deterministic and rules, which are two spellings of one behaviour rather than two "
        "behaviours: extraction_provider_effect answers 'rules' for both -- and for 'local' and "
        "'' -- so every path that dispatches on the effect agrees, and only the word each default "
        "uses differs. Visible since the resolution chain stopped nesting the core spelling inside "
        "a second argument this scan skips.",
    "MATRIXARK_HTTP_HOST":
        "0.0.0.0 for the gateway, which serves a network, and 127.0.0.1 for the two MCP entry "
        "points, whose HTTP mode is a local portal facade",
    "MATRIXARK_METADATA_BACKEND":
        "record_log is what a deployment gets; the SQL probe reads mysql to decide whether to "
        "demand a DSN before asserting the store it built is the SQL one",
    "MATRIXARK_TEMPORALSTORE_NAMESPACE":
        "deploy_ns everywhere except the scale-failover harness, which stands up its own store "
        "and addresses it by its own name",
    "MATRIXARK_TEMPORALSTORE_TABLE":
        "deploy_table, with the same harness exception",
}

#: 70 variables carried a string default when this was written.
EXPECTED_STRING_READ_FLOOR = 40


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


def _looks_numeric(value: str) -> bool:
    """A number wearing quotes. Its own guard owns it; see _NOT_A_STRING above."""
    return value.lstrip("+-").replace(".", "", 1).isdigit()


#: Disagreements that are NOT legitimate and are not fixed here. Kept apart from the list
#: above on purpose: filing a defect under what makes it legitimate is how it stops being
#: looked at. Each needs a decision, not a reason.
KNOWN_DEFECTS: Dict[str, str] = {
    "MATRIXARK_EMBEDDING_MODEL":
        "DEFECT, decision pending. matrixark_mcp_core defaults the sentence-transformers "
        "model to all-MiniLM-L6-v2 and matrixark_mcp_embeddings to multilingual-e5-large, "
        "and mcp_embeddings is the module that LOADS the encoder -- so vectors are produced "
        "by e5-large while thirteen modules label them MiniLM. Making the name truthful "
        "changes embedding_model_ref_for_name and orphans every embedding a populated store "
        "already holds, so it needs a backfill decision rather than an edit here. This entry "
        "exists so the disagreement is visible: it was invisible while the scan was "
        "line-oriented, because both reads are split across lines.",
    "MATRIXARK_BENCHMARK_EMBEDDING_MODEL":
        "DEFECT, small. One harness defaults to matrixark-local-hash-embedding and another to "
        "all-MiniLM-L6-v2, so two benchmark runs that set nothing measure different encoders "
        "under one variable and their numbers are not comparable. Harmless to a deployment, "
        "which is why it survived, and worth a look by whoever owns the harness.",
}

#: What the assertions below accept as already-known: legitimate by role, or a defect that
#: has been written down. Anything else is new and fails.
_LISTED: Dict[str, str] = dict(KNOWN_DISAGREEMENTS, **KNOWN_DEFECTS)


def _reads() -> Dict[str, List[Tuple[str, int, str]]]:
    """Every os.environ.get(NAME, "literal") in production source, keyed by variable.

    PARSED, not matched line by line. The previous scan ran a regex over ONE LINE AT A TIME,
    so every read split across lines was invisible to it -- and a long default is exactly what
    makes a formatter split the call:

        os.environ.get(
            "MATRIXARK_EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        )

    Two variables were hidden that way, one of them a recorded defect, from the guard whose
    whole purpose is to fail when a second default appears. A formatting choice should not
    decide what a guard can see.
    """
    found: Dict[str, List[Tuple[str, int, str]]] = collections.defaultdict(list)
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        # `get(NAME, "").strip() or get(OTHER, "").strip() or "default"` puts the default at the
        # END of the chain; the `""` is a placeholder that lets `.strip()` run and is one of the
        # values this scan ignores. Reading only the second argument therefore lost BOTH endpoints
        # of a variable whose two defaults are the whole reason it is listed. Same lesson as the
        # line-oriented scan in the docstring above, one spelling further on.
        for node in ast.walk(tree):
            if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or)):
                continue
            first_name = None
            for value in node.values:
                for child in ast.walk(value):
                    if not (isinstance(child, ast.Call) and child.args):
                        continue
                    child_func = child.func
                    if not (isinstance(child_func, ast.Attribute)
                            and child_func.attr in ("get", "getenv")):
                        continue
                    first = child.args[0]
                    if first_name is None and isinstance(first, ast.Constant) \
                            and isinstance(first.value, str):
                        first_name = first.value
                    break
            tail = node.values[-1]
            if first_name is None or not _NAME.fullmatch(first_name):
                continue
            if not (isinstance(tail, ast.Constant) and isinstance(tail.value, str)):
                continue
            if tail.value in _NOT_A_STRING or _looks_numeric(tail.value):
                continue
            found[first_name].append((path, node.lineno, tail.value))

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) != 2:
                continue
            # Deliberately NOT skipping calls inside an `or` chain: `get(NAME, "literal")` is that
            # variable's own default wherever it sits, and the `""` placeholders the chain head
            # uses are already discarded by _NOT_A_STRING below.
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr not in ("get", "getenv"):
                continue
            owner = func.value
            if isinstance(owner, ast.Attribute):
                is_env = owner.attr == "environ"
            elif isinstance(owner, ast.Name):
                is_env = owner.id in ("os", "environ")
            else:
                is_env = False
            if not is_env:
                continue
            key, default = node.args
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                continue
            if not _NAME.fullmatch(key.value):
                continue
            if not (isinstance(default, ast.Constant) and isinstance(default.value, str)):
                continue
            value = default.value
            if value in _NOT_A_STRING or _looks_numeric(value):
                continue
            found[key.value].append((path, key.lineno, value))
    return found


def _disagreeing() -> set:
    return {name for name, rows in _reads().items() if len({v for _, _, v in rows}) > 1}


class StringDefaultsAgreeTest(unittest.TestCase):

    def test_the_scan_still_finds_string_defaults(self) -> None:
        reads = _reads()
        self.assertGreaterEqual(
            len(reads), EXPECTED_STRING_READ_FLOOR,
            "found %d variables read with a string default, expected at least %d -- if the read "
            "shape changed, the assertions below run on an empty set"
            % (len(reads), EXPECTED_STRING_READ_FLOOR))

    def test_no_new_variable_disagrees_about_its_default(self) -> None:
        reads = _reads()
        new = sorted(_disagreeing() - set(_LISTED))
        detail = ["%s (%s)" % (name, ", ".join(sorted({v for _, _, v in reads[name]})))
                  for name in new]
        self.assertEqual(
            [], detail,
            "these are read with more than one string default, so a deployment that sets nothing "
            "gets a different value depending on which path asks: %s\nA name, a namespace or an "
            "endpoint that differs by reader is how a tool ends up addressing something nobody "
            "else does. Make them agree, or list it above with what makes it legitimate." % detail)

    def test_a_listed_variable_that_now_agrees_is_struck_off(self) -> None:
        stale = sorted(set(_LISTED) - _disagreeing())
        self.assertEqual(
            [], stale,
            "these are listed as disagreeing and no longer do: %s. Strike them off." % stale)

    def test_every_listed_variable_gives_a_reason(self) -> None:
        thin = sorted(name for name, why in _LISTED.items() if len(why.strip()) < 30)
        self.assertEqual([], thin, "listed without a reason worth reading: %s" % thin)


if __name__ == "__main__":
    unittest.main()

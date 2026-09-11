"""One setting, one number, across every surface that declares it.

`max_selected_refs` carried FOUR values at once: config/temporalstore.toml said 1000, two Python
modules defaulted to 64, the request builder wrote a bare 24 inline, and the engine used another 24
under a ceiling of 128. Which one a deployment got depended on which surface it came through, and
the existing numeric-defaults guard could not see it because that guard only scans
`os.environ.get(VAR, literal)` reads -- not a config file, not Rust, not an inline fallback.

This is the narrow guard for that one setting, checked where it is actually written.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)


def config_value():
    path = os.path.join(ROOT, "config", "temporalstore.toml")
    for line in open(path, encoding="utf-8"):
        match = re.match(r"\s*max_selected_refs\s*=\s*(\d+)", line)
        if match:
            return int(match.group(1))
    return None


def python_defaults():
    found = {}
    for name in ("matrixark_mcp_core.py", "matrixark_mcp_runtime_config.py"):
        body = open(os.path.join(HERE, name), encoding="utf-8").read()
        match = re.search(
            r'DEFAULT_MAX_SELECTED_REFS = int\(os\.environ\.get\("MATRIXARK_MAX_SELECTED_REFS",.*?"(\d+)"',
            body,
        )
        found[name] = int(match.group(1)) if match else None
    return found


def rust_default():
    path = os.path.join(ROOT, "crates", "temporalstore-rust", "src", "matrixark_rust_proxy_impl.rs")
    body = open(path, encoding="utf-8").read()
    match = re.search(r"const DEFAULT_MAX_SELECTED_REFS: usize = (\d+);", body)
    return int(match.group(1)) if match else None


def every_surface_declares_the_same_cap():
    config = config_value()
    check(config is not None, "config/temporalstore.toml no longer declares max_selected_refs")
    rust = rust_default()
    check(rust is not None, "the engine no longer declares DEFAULT_MAX_SELECTED_REFS")
    pythons = python_defaults()
    for name, value in pythons.items():
        check(value is not None, "%s no longer declares DEFAULT_MAX_SELECTED_REFS" % name)

    values = {"config": config, "rust": rust}
    values.update(pythons)
    distinct = {v for v in values.values() if v is not None}
    check(
        len(distinct) == 1,
        "max_selected_refs disagrees across surfaces: %s" % (values,),
    )


def the_request_builder_does_not_re_default_it():
    """The inline fallback is what the environment-scanning guard cannot see.

    `int(ranking.get(...) or args.get(...) or 24)` declares a fourth default in the middle of a
    dict literal. It must use the named constant instead, so there is nothing to drift.
    """
    body = open(os.path.join(HERE, "matrixark_temporal_direct_read.py"), encoding="utf-8").read()
    # Scan to the matching close paren rather than regexing it. A regex that stops at the first
    # ")" reads `int(ranking.get("max_selected_refs")` as the whole expression and then reports the
    # constant missing -- which is what the first version of this guard did, failing on correct
    # code.
    start = body.find('"max_selected_refs": int(')
    check(start != -1, "the engine-request builder no longer sets max_selected_refs")
    expression = None
    if start != -1:
        open_at = body.index("(", start + len('"max_selected_refs": int'))
        brace_depth = 0
        for index in range(open_at, len(body)):
            if body[index] == "(":
                brace_depth += 1
            elif body[index] == ")":
                brace_depth -= 1
                if brace_depth == 0:
                    expression = body[open_at + 1:index]
                    break
    if expression is not None:
        check(
            "DEFAULT_MAX_SELECTED_REFS" in expression,
            "the request builder re-defaults max_selected_refs with a literal: %r"
            % expression.strip()[:160],
        )
        bare = re.findall(r"\bor\s+(\d+)\b", expression)
        check(not bare, "the request builder still falls back to a bare literal: %s" % bare)


def a_caller_can_still_ask_for_everything():
    """1000 is a DEFAULT, not a ceiling: the ways to say 'all' must remain."""
    body = open(os.path.join(HERE, "matrixark_tenant_policy.py"), encoding="utf-8").read()
    check(
        "MATRIXARK_RETURN_ALL_CANDIDATES" in body,
        "the return-all knob is gone, so 'configure as all' has no surface",
    )
    engine = open(
        os.path.join(ROOT, "crates", "temporalstore-rust", "src", "matrixark_rust_proxy_impl.rs"),
        encoding="utf-8",
    ).read()
    check(
        ".clamp(1, 128)" not in engine,
        "the engine has an artificial ceiling again, so 'all' cannot exceed it",
    )


for test in (
    every_surface_declares_the_same_cap,
    the_request_builder_does_not_re_default_it,
    a_caller_can_still_ask_for_everything,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all cap-value checks pass")

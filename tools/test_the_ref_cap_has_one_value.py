"""One setting, one number, across every surface that declares it.

`max_selected_refs` carried FOUR values at once: config/temporalstore.toml said 1000, two Python
modules defaulted to 64, the request builder wrote a bare 24 inline, and the engine used another 24
under a ceiling of 128. Which one a deployment got depended on which surface it came through, and
the existing numeric-defaults guard could not see it because that guard only scans
`os.environ.get(VAR, literal)` reads -- not a config file, not Rust, not an inline fallback.

This is the narrow guard for that one setting, checked where it is actually written.
"""
import ast
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


SURFACES = ("matrixark_mcp_core.py", "matrixark_mcp_runtime_config.py")

CONSTANT = "DEFAULT_MAX_SELECTED_REFS"
VARIABLE = "MATRIXARK_MAX_SELECTED_REFS"
CAST = int

_DECLARES = re.compile(
    r'DEFAULT_MAX_SELECTED_REFS = int\(os\.environ\.get\("MATRIXARK_MAX_SELECTED_REFS",.*?"(\d+)"'
)

# ---------------------------------------------------------------------------------------------
# A surface satisfies this file two ways: it reads the environment variable itself, or it imports
# the constant from a module that does. The second is the STRONGER of the two -- one definition
# reached by import cannot drift from itself, where two kept in step can -- so following the import
# is not a relaxation of the question. What is still refused is a surface that binds the name to a
# literal, and a surface whose imports disagree about where the value comes from.
#
# The same twenty lines are in test_the_shard_size_has_one_value.py and
# test_the_score_threshold_has_one_value.py. That is deliberate rather than lazy: these three run
# as scripts, a guard importing another guard is what test_matrixark_no_cross_test_imports exists
# to stop, and a tools/ module only the tests import is exactly what
# test_a_module_only_tests_reach_is_not_live records as unreachable. Three copies of a rule is a
# real cost; a fourth entry in that list and a new cross-test edge were the larger ones.


def _body(name):
    return open(os.path.join(HERE, name), encoding="utf-8").read()


def _hardcodes(body):
    """The name bound to a bare number -- the failure this whole file exists to catch."""
    for node in ast.walk(ast.parse(body)):
        if not isinstance(node, ast.Assign):
            continue
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if CONSTANT in names and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, (int, float)) and not isinstance(node.value.value, bool):
                return node.value.value
    return None


def _imports_from(body):
    """Every module this file imports the constant from, as file names.

    Plural on purpose: the re-export in matrixark_mcp_core.py is a try/except pair of imports
    maintained separately, and two branches naming two different modules is the same divergence
    this guard is about, one level up.
    """
    out = []
    for node in ast.walk(ast.parse(body)):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                if alias.name == CONSTANT and alias.asname is None:
                    out.append(node.module.rsplit(".", 1)[-1] + ".py")
    return out


def _resolve(name, seen=()):
    """The value a surface uses, and the file that declares it."""
    if name in seen:
        return None, None
    body = _body(name)
    if _hardcodes(body) is not None:
        return None, None
    match = _DECLARES.search(body)
    if match:
        return CAST(match.group(1)), name
    sources = set(_imports_from(body))
    if len(sources) != 1:
        return None, None
    source = sources.pop()
    if not os.path.exists(os.path.join(HERE, source)):
        return None, None
    return _resolve(source, seen + (name,))


def python_defaults():
    return {name: _resolve(name)[0] for name in SURFACES}


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



def every_surface_honours_the_environment_variable():
    """A surface need not contain the variable name; what it must not do is arrive at a value the
    variable cannot reach. So the question is asked of whichever file DECLARES the value for that
    surface -- itself when it reads the environment, and the module it imports from when it does
    not.
    """
    for name in SURFACES:
        _value, declaring = _resolve(name)
        check(
            declaring is not None,
            "%s neither reads %s nor imports the constant from a module that does"
            % (name, VARIABLE),
        )
        if declaring is None:
            continue
        check(
            VARIABLE in _body(declaring),
            "%s takes its value from %s, which hardcodes it instead of reading %s"
            % (name, declaring, VARIABLE),
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
    every_surface_honours_the_environment_variable,
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

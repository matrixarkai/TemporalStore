"""The score threshold comes from one place.

It came from five, and they disagreed: config/temporalstore.toml declared 0.05, two Python modules
defaulted to 0.20, the engine-request builder sent a bare 0.0 that overrode both on every request,
and the engine fell back to 0.0 of its own. The effective threshold was 0.0 while three surfaces
said otherwise -- so raising it in the file changed nothing, which is the worst kind of knob.

The existing numeric-defaults guard cannot see this: it scans `os.environ.get(VAR, literal)` reads,
not config files and not a literal inline in a dict.
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
        match = re.match(r"\s*retrieval_min_score\s*=\s*([\d.]+)", line)
        if match:
            return float(match.group(1))
    return None


SURFACES = ("matrixark_mcp_core.py", "matrixark_mcp_runtime_config.py")

CONSTANT = "DEFAULT_RETRIEVAL_MIN_SCORE"
VARIABLE = "MATRIXARK_RETRIEVAL_MIN_SCORE"
CAST = float

_DECLARES = re.compile(
    r'DEFAULT_RETRIEVAL_MIN_SCORE = float\(os\.environ\.get\("MATRIXARK_RETRIEVAL_MIN_SCORE",.*?"([\d.]+)"'
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


def the_file_and_the_code_agree():
    config = config_value()
    check(config is not None, "config/temporalstore.toml no longer declares retrieval_min_score")
    values = {"config": config}
    values.update(python_defaults())
    for name, value in values.items():
        check(value is not None, "%s no longer declares the threshold" % name)
    distinct = {v for v in values.values() if v is not None}
    check(len(distinct) == 1, "the score threshold disagrees across surfaces: %s" % (values,))



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
    """A bare literal here overrides the file AND the code, on every request."""
    body = open(os.path.join(HERE, "matrixark_temporal_direct_read.py"), encoding="utf-8").read()
    start = body.find('"min_score": float(')
    check(start != -1, "the engine-request builder no longer sets min_score")
    if start == -1:
        return
    open_at = body.index("(", start + len('"min_score": float'))
    depth = 0
    expression = None
    for index in range(open_at, len(body)):
        if body[index] == "(":
            depth += 1
        elif body[index] == ")":
            depth -= 1
            if depth == 0:
                expression = body[open_at + 1:index]
                break
    check(expression is not None, "could not read the min_score expression")
    if expression is not None:
        check(
            "DEFAULT_RETRIEVAL_MIN_SCORE" in expression,
            "the builder re-defaults min_score with a literal: %r" % expression.strip()[:160],
        )
        bare = re.findall(r"\bor\s+([\d.]+)\b", expression)
        check(not bare, "the builder still falls back to a bare literal: %s" % bare)


def the_engine_treats_absent_as_return_everything_scoring():
    """The engine's own fallback is 0.0 and must stay that way.

    It is not a fourth default -- it is what "the caller named no threshold" means, and it has to
    return everything the query can score rather than inventing a cut of its own. The caller now
    always sends one, so this is the floor beneath a request that does not.
    """
    engine = open(
        os.path.join(ROOT, "crates", "temporalstore-rust", "src", "matrixark_rust_proxy_impl.rs"),
        encoding="utf-8",
    ).read()
    check(
        'ranking_field_from(request.min_score, &request_record, "min_score").unwrap_or(0.0)'
        in engine.replace("\n", " ").replace("        ", " ").replace("  ", " "),
        "the engine no longer falls back to 0.0 for an absent threshold",
    )


for test in (
    the_file_and_the_code_agree,
    every_surface_honours_the_environment_variable,
    the_request_builder_does_not_re_default_it,
    the_engine_treats_absent_as_return_everything_scoring,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all threshold-source checks pass")

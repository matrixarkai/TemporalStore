"""The score threshold comes from one place.

It came from five, and they disagreed: config/temporalstore.toml declared 0.05, two Python modules
defaulted to 0.20, the engine-request builder sent a bare 0.0 that overrode both on every request,
and the engine fell back to 0.0 of its own. The effective threshold was 0.0 while three surfaces
said otherwise -- so raising it in the file changed nothing, which is the worst kind of knob.

The existing numeric-defaults guard cannot see this: it scans `os.environ.get(VAR, literal)` reads,
not config files and not a literal inline in a dict.
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
        match = re.match(r"\s*retrieval_min_score\s*=\s*([\d.]+)", line)
        if match:
            return float(match.group(1))
    return None


def python_defaults():
    found = {}
    for name in ("matrixark_mcp_core.py", "matrixark_mcp_runtime_config.py"):
        body = open(os.path.join(HERE, name), encoding="utf-8").read()
        match = re.search(
            r'DEFAULT_RETRIEVAL_MIN_SCORE = float\(os\.environ\.get\("MATRIXARK_RETRIEVAL_MIN_SCORE",.*?"([\d.]+)"',
            body,
        )
        found[name] = float(match.group(1)) if match else None
    return found


def the_file_and_the_code_agree():
    config = config_value()
    check(config is not None, "config/temporalstore.toml no longer declares retrieval_min_score")
    values = {"config": config}
    values.update(python_defaults())
    for name, value in values.items():
        check(value is not None, "%s no longer declares the threshold" % name)
    distinct = {v for v in values.values() if v is not None}
    check(len(distinct) == 1, "the score threshold disagrees across surfaces: %s" % (values,))


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

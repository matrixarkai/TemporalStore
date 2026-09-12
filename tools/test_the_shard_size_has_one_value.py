"""Shard size is part of the on-disk contract, so every surface must agree on it.

A record lands in shard `arrival_index // shard_size`. The asymmetry is what makes this dangerous:
a reader assuming a LARGER shard size than the writer used computes `max_shard = (count - 1) / size`,
enumerates too few shards, and returns part of the store while reporting success. The other
direction is harmless -- surplus shard keys simply do not exist and read back empty.

It was 256 in the two serving modules, 4096 in the backfill (the only module that honoured the
environment variable), and 1024 in the engine's fallback. The engine guessing 1024 against a store
written at 256 would have read a quarter of it.
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


SURFACES = (
    "matrixark_mcp_core.py",
    "matrixark_mcp_runtime_config.py",
    "matrixark_context_backfill.py",
)

CONSTANT = "DIRECT_RECORD_LOG_SHARD_SIZE"
VARIABLE = "MATRIXARK_DIRECT_RECORD_LOG_SHARD_SIZE"

_DECLARES = re.compile(
    r'DIRECT_RECORD_LOG_SHARD_SIZE = int\(os\.environ\.get\(\s*"MATRIXARK_DIRECT_RECORD_LOG_SHARD_SIZE",.*?"(\d+)"'
)


def _body(name):
    return open(os.path.join(HERE, name), encoding="utf-8").read()


def _hardcodes(body):
    """The name bound to a bare number -- the failure this whole file exists to catch."""
    for node in ast.walk(ast.parse(body)):
        if not isinstance(node, ast.Assign):
            continue
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if CONSTANT in names and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, int) and not isinstance(node.value.value, bool):
                return node.value.value
    return None


def _imports_from(body):
    """Every module this file imports the constant from, as file names.

    Plural on purpose: the re-export in matrixark_mcp_core.py is a try/except pair of imports
    maintained separately, and two branches naming two different modules is exactly the split this
    guard is about.
    """
    out = []
    for node in ast.walk(ast.parse(body)):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                if alias.name == CONSTANT and alias.asname is None:
                    out.append(node.module.rsplit(".", 1)[-1] + ".py")
    return out


def _resolve(name, seen=()):
    """The shard size a surface uses, and the file that declares it.

    A surface satisfies this guard two ways: it reads the environment variable itself, or it
    imports the constant from a module that does. The second is the STRONGER of the two -- one
    definition reached by import cannot drift from itself, where two kept in step can -- so
    following the import is not a relaxation. What is still refused is a surface that binds the
    name to a literal, and a surface whose imports disagree about where the value comes from.
    """
    if name in seen:
        return None, None
    body = _body(name)
    if _hardcodes(body) is not None:
        return None, None
    match = _DECLARES.search(body)
    if match:
        return int(match.group(1)), name
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
    match = re.search(r"const DEFAULT_RECORD_LOG_SHARD_SIZE: u64 = (\d+);", body)
    return int(match.group(1)) if match else None


def every_surface_agrees_on_the_shard_size():
    values = python_defaults()
    values["rust"] = rust_default()
    for name, value in values.items():
        check(value is not None, "%s no longer declares the shard size the expected way" % name)
    distinct = {v for v in values.values() if v is not None}
    check(len(distinct) == 1, "shard size disagrees across surfaces: %s" % (values,))


def every_surface_honours_the_environment_variable():
    """It was hardcoded in the two serving modules, so the knob existed and only the backfill read
    it -- which is how the writer and the reader came to disagree in the first place.

    The surface itself need not contain the variable name: what it must not do is arrive at a
    value the variable cannot reach. So the question is asked of whichever file DECLARES the size
    for that surface, which is the surface itself when it reads the environment and the module it
    imports from when it does not.
    """
    for name in ("matrixark_mcp_core.py", "matrixark_mcp_runtime_config.py"):
        _value, declaring = _resolve(name)
        check(
            declaring is not None,
            "%s neither reads %s nor imports the shard size from a module that does" % (
                name, VARIABLE),
        )
        if declaring is None:
            continue
        check(
            VARIABLE in _body(declaring),
            "%s takes its shard size from %s, which hardcodes it instead of reading the variable"
            % (name, declaring),
        )


def the_engine_does_not_guess_a_larger_size():
    """The engine's fallback must never exceed the writer's default.

    Larger is the losing direction: it under-enumerates shards and silently drops records. This
    asserts the relationship, not just the number, so raising the writer's default later cannot
    quietly leave the engine reading a fraction of the store.
    """
    writer = python_defaults().get("matrixark_mcp_core.py")
    engine = rust_default()
    check(
        writer is not None and engine is not None,
        "the shard size could not be read from both the writer and the engine, so the comparison "
        "below did not run -- a guard that cannot find its inputs reports nothing, not safety",
    )
    if writer is None or engine is None:
        return
    check(
        engine <= writer,
        "the engine assumes %d records per shard against a writer at %d, so it would enumerate "
        "too few shards and read part of the store" % (engine, writer),
    )


for test in (
    every_surface_agrees_on_the_shard_size,
    every_surface_honours_the_environment_variable,
    the_engine_does_not_guess_a_larger_size,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all shard-size checks pass")

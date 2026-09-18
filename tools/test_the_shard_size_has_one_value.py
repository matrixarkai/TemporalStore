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
    # Added after it was found holding its own `DEFAULT_SHARD_SIZE = 256`: right by value and
    # unreachable by the environment variable, so a deployment that set the variable would have
    # had this tool read a different shard for every sequence and report the wrong record.
    "inspect_matrixark_codex_hook_records.py",
)

#: A name may hold a shard size that is deliberately NOT the writer's, if it says so. The only
#: one today is the retired codex-hook layout kept as a read fallback. Checked by name rather
#: than by a list of files, because a maintainer has to choose the name and cannot add it by
#: accident, where a line in an exemption list is one edit away.
DELIBERATELY_NOT_THE_WRITERS = "LEGACY"

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


def _module_level_shard_sizes():
    """Every top-level `*SHARD_SIZE* = <int>` under tools/, and how many files were read.

    The count is returned with the findings on purpose: a sweep that walked nothing reports
    exactly what a clean directory reports.

    No exclusion for this file is needed and none is written: the names bound here are
    `CONSTANT` and `VARIABLE`, and the scan reads assignment TARGETS, so the constant's name
    appearing in a string cannot match.
    """
    found = []
    scanned = 0
    for filename in sorted(os.listdir(HERE)):
        if not filename.endswith(".py"):
            continue
        try:
            tree = ast.parse(_body(filename))
        except SyntaxError:
            continue
        scanned += 1
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            if not isinstance(node.value, ast.Constant):
                continue
            if not isinstance(node.value.value, int) or isinstance(node.value.value, bool):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name) and "SHARD_SIZE" in target.id.upper():
                    found.append((filename, target.id, node.value.value))
    return scanned, found


def _inline_shard_arithmetic():
    """Every `:records:` key built with a bare number as the divisor or the modulus.

    Returned with the file count for the same reason as above: a scan that stopped recognising
    the shape reports what a clean directory reports.
    """
    found = []
    keys_seen = 0
    scanned = 0
    for filename in sorted(os.listdir(HERE)):
        if not filename.endswith(".py"):
            continue
        try:
            tree = ast.parse(_body(filename))
        except SyntaxError:
            continue
        scanned += 1
        for node in ast.walk(tree):
            if not isinstance(node, ast.JoinedStr):
                continue
            literal = "".join(
                part.value for part in node.values
                if isinstance(part, ast.Constant) and isinstance(part.value, str)
            )
            if ":records:" not in literal:
                continue
            keys_seen += 1
            for inner in ast.walk(node):
                if not isinstance(inner, ast.BinOp):
                    continue
                if not isinstance(inner.op, (ast.FloorDiv, ast.Mod)):
                    continue
                right = inner.right
                if isinstance(right, ast.Constant) and isinstance(right.value, int) \
                        and not isinstance(right.value, bool):
                    found.append((filename, ast.unparse(inner)))
    return scanned, keys_seen, found


def no_records_key_is_built_from_a_bare_number():
    """A copy of the shard size does not have to be a named constant to be a copy.

    Three of them lived inline in the ingestion report as `sequence // <number>` and
    `sequence % <number>`, which the named sweep above cannot see: there is no name to find.
    Correct by value and unreachable by the environment variable, which is the whole failure --
    the value being right today is what makes a copy look harmless.

    A retired layout may still be addressed, but through a name that says so, so that this
    check and a reader can both tell it from the writer's number.
    """
    scanned, keys_seen, found = _inline_shard_arithmetic()
    check(
        scanned > 100,
        "the sweep parsed only %d files under tools/ and its silence means nothing" % scanned,
    )
    check(
        keys_seen >= 5,
        "only %d sharded record keys were recognised; the scan has stopped seeing the shape it "
        "looks for, so a new inline copy would read as clean" % keys_seen,
    )
    offenders = ["%s: %s" % (filename, source) for filename, source in found]
    check(
        not offenders,
        "these compute a record's shard or field from a literal, so the environment variable "
        "cannot reach them: %s. Use %s, or a named constant that says the value is deliberately "
        "not the writer's" % (", ".join(offenders), CONSTANT),
    )


def no_module_holds_its_own_copy_of_the_shard_size():
    """The list above cannot be the whole guard, because the list is what goes stale.

    Two copies lived outside it: `DEFAULT_SHARD_SIZE = 256` in the record inspector and
    `_CODEX_HOOK_SHARD_SIZE = 10000` in the HTTP facade, the second of which made the codex-hook
    query able to see the first shard of a store and nothing after it. Neither was a surface this
    file named, so naming surfaces could not have caught either. A sweep can.
    """
    scanned, found = _module_level_shard_sizes()
    check(
        scanned > 100,
        "the sweep parsed only %d files under tools/, so it is not looking at the directory any "
        "more and its silence means nothing" % scanned,
    )
    check(
        len(found) >= 1,
        "no module-level shard size was found at all; the sweep has stopped recognising the "
        "shape it looks for and would report a new copy as clean",
    )
    offenders = [
        "%s:%s = %d" % (filename, name, value)
        for filename, name, value in found
        if DELIBERATELY_NOT_THE_WRITERS not in name.upper()
    ]
    check(
        not offenders,
        "these bind a shard size to a literal, so the environment variable cannot reach them and "
        "they drift the moment the canonical definition moves: %s. Import "
        "%s instead, or say in the name that the value is deliberately not the writer's"
        % (", ".join(offenders), CONSTANT),
    )


#: The engine spells the same contract twice, under two names and two types. Both are read: a
#: guard that follows one of two constants is how the OTHER one goes unwatched.
RUST_CONSTANTS = (
    ("DEFAULT_RECORD_LOG_SHARD_SIZE", "u64"),
    ("DIRECT_RECORD_LOG_SHARD_SIZE", "usize"),
)


def rust_defaults():
    path = os.path.join(ROOT, "crates", "temporalstore-rust", "src", "matrixark_rust_proxy_impl.rs")
    body = open(path, encoding="utf-8").read()
    values = {}
    for name, rust_type in RUST_CONSTANTS:
        match = re.search(r"const %s: %s = (\d+);" % (name, rust_type), body)
        values[name] = int(match.group(1)) if match else None
    return values


def rust_default():
    return rust_defaults()["DEFAULT_RECORD_LOG_SHARD_SIZE"]


def the_engines_two_names_for_the_shard_size_agree():
    """One number, two constants, one file. They have to hold the same value or one of them is
    addressing a store the other did not write."""
    values = rust_defaults()
    for name, value in sorted(values.items()):
        check(value is not None, "the engine no longer declares %s the expected way" % name)
    distinct = {value for value in values.values() if value is not None}
    check(
        len(distinct) == 1,
        "the engine's two names for the record shard size disagree: %s" % (values,),
    )


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
    the_engines_two_names_for_the_shard_size_agree,
    no_module_holds_its_own_copy_of_the_shard_size,
    no_records_key_is_built_from_a_bare_number,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all shard-size checks pass")

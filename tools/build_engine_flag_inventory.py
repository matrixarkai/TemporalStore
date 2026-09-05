#!/usr/bin/env python3
"""Generate the engine flag inventory: every environment variable the engine reads, what it is
for, and what keeping it costs.

Written because "there are too many flags" is true and not actionable on its own. No accessor here
is unreachable -- every one has a non-test caller -- so the list cannot be shortened by deleting
code nothing calls. What shortens it is a different question, asked per flag: does anything
anywhere select the off position? A flag whose off-path no test, config, launch profile or portal
setting ever chooses is not a switch, it is a branch, and the live side can be made unconditional.
The two columns that answer it are `default` and `set by`.

Generated rather than written, and checked by a test, because a hand-maintained list of this many
knobs is wrong within a week and its staleness is silent. Every count is computed, including the
accessor count: the one number that was hardcoded went stale the first time a flag was retired.

Being generated is not the same as being complete, which is the harder lesson here. Regenerating
byte-identically proves the document matches the SCAN, not the engine -- and a scan that knew one
way of reading a flag (`env::var("TS_X")`) missed every helper-mediated read and every non-`TS_`
prefix, listing 94 of 313. `test_matrixark_engine_flag_inventory.py` now checks the document
against the source with a rule simpler than this one, and states how much source it read.
"""
import io
import pathlib
import re
import sys

# Default to the repository this script lives in. It used to default to an absolute path to
# one particular checkout on one particular machine, so running it anywhere else regenerated
# the document from a tree the caller had never heard of -- or from nothing at all. The test
# always passes a directory, so it never touched the default and never saw this.
ROOT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                    else pathlib.Path(__file__).resolve().parent.parent)
SRC = ROOT / "crates" / "temporalstore-rust" / "src"
OUT = ROOT / "docs" / "ops" / "temporalstore-engine-flags.md"

CLASS_RULES = [
    ("topology", re.compile(r"_(ADDR|NODES|BIND|URI|ENDPOINT|BUCKET|CLUSTER_ID|NODE_ID|SHARD_ID|"
                            r"DISTRIBUTED|STANDALONE|STORAGE_BACKEND|LOCATION)$|_ADDR_|_DIR$")),
    ("credential", re.compile(r"TOKEN|SECRET|_KEY$")),
    ("durability", re.compile(r"WAL|FSYNC|BARRIER|FENCE|RECOVERY|RECLAIM|SNAPSHOT")),
    ("format", re.compile(r"BINARY|CODEC|LEGACY|ARRAY_BYTES|FRAME|RECORDS|FOLD|VECTOR")),
    ("capacity", re.compile(r"BYTES|SIZE|MAX_|MIN_|PERCENT|SERIES|JOBS|TIMEOUT|DELAY|INTERVAL")),
    ("diagnostic", re.compile(r"TRACE|DEBUG|PROFILE|SCALE|METRICS|REPORT")),
    # Below capacity on purpose: a cap is a cap whatever subsystem it belongs to, so
    # `MAX_SELECTED_REFS` stays under capacity rather than moving to context. These four pick up
    # what "behaviour" was absorbing -- it held 148 of 313 flags, which is a bucket, not a group.
    ("identity", re.compile(r"_(ACCOUNT_ID|TENANT_ID|USER_ID|SESSION_ID|AGENT_NAME)$")),
    ("benchmark", re.compile(r"BENCHMARK")),
    ("context", re.compile(r"CONTEXT|EMBED|RETRIEV|BACKFILL|LEXICAL|SUMMAR|CHUNK|SKILL|PACK")),
    ("cluster policy", re.compile(r"META_|REBALANCE|CONVICT|FAILURE_DETECTOR|FAILOVER|FREEZE|"
                                  r"RETENTION|DIVERGENCE")),
]

# A legacy path is one the flag RESTORES. That verb is the evidence; the mood is not.
#
# The previous pattern accepted "before", "previous" and "escape hatch" anywhere in the doc. Those
# appear in ordinary prose about any bound or any toggle -- "how many entries the queue may hold
# BEFORE a write stops deferring" is a bound, not a legacy path -- and on that basis it classified
# an admin token and an allocator knob as older code paths kept alive.
LEGACY = re.compile(
    r"(restor\w+|roll(s|ed)? back|rollback|revert\w*|falls? back to)\s+"
    r"(the\s+|a\s+)?(?:\w+[\s-]+){0,3}?"
    r"(legacy|previous|older|old|growing|full-log|per-shard|array)"
    r"|as they were written before"
    r"|superseded",
    re.I)


NEWLINE = chr(10)

# Every place a flag name is written down, not one way of writing it down.
#
# This used to match `env::var("TS_...")` only. The engine also reads flags through helpers that
# take the name as an argument -- `env_flag_default_on("TS_X")`, `env_bool("TS_X", default)`,
# `raft_env_flag_default_on("TS_X")` -- and a regex for one idiom cannot see the others. It listed
# 94 of the 240 `TS_*` names the engine writes down, and missed every metaserver, proxy and
# raft-tuning knob there is: whole subsystems absent from the one document that claims to list
# them all, with nothing to notice, because a generator that under-counts still regenerates
# byte-identically.
#
# The name in quotes is what every idiom has in common, so that is what this looks for.
#
# All three prefixes, because the engine reads all three. `TS_` alone left 73 `MATRIXARK_*` and
# `TEMPORALSTORE_*` knobs -- the whole context and gateway surface, the embed drainer, the
# retrieval caps -- in no inventory at all: this document excluded them by prefix, and the
# Python-side inventory only reads `tools/`. A knob nobody lists is a knob nobody can decide
# about, which is the failure this document exists to prevent.
FLAG_READ = re.compile(r'"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"')


TEST_MODULE = re.compile(r"\s*(?:pub(?:\([a-z():]+\))?\s+)?mod\s+[A-Za-z0-9_]+")


def strip_test_modules(text):
    """Blank out `#[cfg(test)] mod ... { }` bodies, preserving line numbers.

    Excluding test PATHS is not the same as excluding test CODE: most of this engine's unit tests
    live in an inline `mod tests` inside the file they test. Counting those, a flag named only by
    a test would be reported as one the engine reads.

    Only a `mod`. The first version of this took any `#[cfg(test)]`, scanned FORWARD for the next
    `{` and blanked to its match -- so `#[cfg(test)]` on a single statement (which this engine
    uses, e.g. a test-only watermark store in `lifecycle.rs`) swallowed whatever block came
    after it. That silently removed live code from the scan: five accessors with obvious callers
    were reported as having none, because the calls were inside the region this had erased.
    """
    lines = text.splitlines()
    out = list(lines)
    index = 0
    while index < len(lines):
        if lines[index].strip().startswith("#[cfg(test)]"):
            head = index + 1
            while head < len(lines) and (not lines[head].strip()
                                         or lines[head].strip().startswith("#[")):
                head += 1
            if head < len(lines) and TEST_MODULE.match(lines[head]):
                # `#[cfg(test)] mod tests;` declares a module in another FILE and has no body
                # here. Hunting for its opening brace finds the next item's instead and blanks
                # that -- which is how `propose_distributed_one` came to look uncalled: the
                # whole of raft.rs after the declaration had been erased from the scan.
                if lines[head].rstrip().endswith(";"):
                    out[head] = ""
                    index = head
                    index += 1
                    continue
                depth = 0
                started = False
                for i in range(head, len(lines)):
                    depth += lines[i].count("{") - lines[i].count("}")
                    if "{" in lines[i]:
                        started = True
                    out[i] = ""
                    if started and depth <= 0:
                        index = i
                        break
                else:
                    index = len(lines)
        index += 1
    return NEWLINE.join(out)


def function_end(lines, fn_line):
    """Last line of the function opening at `fn_line`, by brace matching.

    Needed because "which flags does this function read" is what decides whether its doc comment
    can be attributed to any single one of them.
    """
    depth = 0
    started = False
    for i in range(fn_line, len(lines)):
        depth += lines[i].count("{") - lines[i].count("}")
        if "{" in lines[i]:
            started = True
        if started and depth <= 0:
            return i + 1
    return len(lines)


def doc_for_flag(name, doc, lines, fn_line):
    """The doc comment to credit to `name`, and the flags it would otherwise be shared with.

    A function reading several flags has one doc comment above it, written about one of them.
    Crediting it to all of them files words under flags they were never written about, and the
    inventory then reports those words as the reason a flag exists.

    This was written when it changed nothing: nine functions read more than one flag and none of
    them carried a doc comment, so it returned an empty shared list every time and the count it
    feeds was zero. It was kept anyway because the failure it prevents is silent. Widening the
    scan to every read idiom brought in the functions that read a dozen knobs at startup, several
    of which do carry a doc comment, and the count is now in the dozens -- the words those
    comments contain were about one flag each, and this is what stops them being reported as the
    reason all dozen exist. A zero that is measured is worth more than a zero that is assumed,
    and this one did not stay zero.
    """
    if not doc or name in doc or fn_line is None:
        return doc, []
    shared = set(FLAG_READ.findall(NEWLINE.join(lines[fn_line:function_end(lines, fn_line)])))
    if len(shared) <= 1:
        return doc, []
    return "", sorted(shared - {name})

# What a boolean flag does when nobody sets it, read off the code that reads it.
#
# Paired with the "set by" column this is the whole retirement question in two cells: a flag
# defaulting ON that nothing sets is one whose off-path no deployment has ever taken.
#
# Deliberately narrow. Only the idioms below are recognised, and anything else reports nothing at
# all -- a wrong default here would be read as fact and acted on, while a blank is read as "go
# look", which is what it is. Non-boolean knobs have no ON/OFF to state and are blank by
# construction.
# The shapes a reader states its default in. Two were added after the portal refused to offer a
# knob whose default nothing could derive: `env_bool(NAME, false)` puts it in an argument, and
# `env_flag_on` puts it in the helper's NAME -- that one is the default-OFF reader, where
# `env_flag_default_on` is its opposite. The two helper names are close enough to read as the same
# thing, which is why the alternation spells both rather than matching a prefix.
DEFAULT_ON = re.compile(
    r"unwrap_or\(\s*true\s*\)|env_flag_default_on\(|\.is_err\(\)"
    r"|env_bool\([^)]*,\s*true\s*\)")
DEFAULT_OFF = re.compile(
    r"unwrap_or\(\s*false\s*\)|\.is_ok\(\)"
    r"|env_flag_on\(|env_bool\([^)]*,\s*false\s*\)")


def _end_of_flag_chain(body: str) -> int:
    """Where the expression that reads the flag stops, so a LATER default is not read as its own.

    `matrixark_rust_sdk_mode_is_direct` reads `MATRIXARK_RUST_SDK_MODE`, compares it against four
    mode strings, and then ORs in a second test on `env::args()` ending `.unwrap_or(false)`.
    Searching the whole body found that `false` and reported a four-valued mode string as a
    boolean defaulting off -- a default that is not merely unknown but wrong, because setting the
    variable to `1` does nothing at all.

    Only the END is cut. The `!` that inverts `!matches!(...)` sits several lines ABOVE the name,
    so trimming the front would reintroduce exactly the error the caller's docstring describes.
    """
    first = FLAG_READ.search(body)
    if not first:
        return len(body)
    later = body.find("env::", first.end())
    return len(body) if later < 0 else later


def statement_around(lines, read_line):
    """The whole statement a flag read sits in, so a default is never taken from a neighbour."""
    start = read_line
    while start > 0:
        previous = lines[start - 1].strip()
        if previous.endswith((";", "{", "}")) or not previous:
            break
        start -= 1
    end = read_line
    while end < len(lines) - 1 and not lines[end].strip().endswith(";"):
        end += 1
    return NEWLINE.join(lines[start:end + 1])


def default_of_statement(lines, read_line):
    """"on", "off", or "" -- the default of a flag read INLINE, from its own statement.

    `default_of` needs a one-flag function returning bool, so a read in the middle of a larger
    function reports nothing. That is not a small gap: `MATRIXARK_RUST_PROXY_ASYNC_CACHE_WARM_ON_LOAD`
    is written inline, defaults ON, and is set by nothing anywhere -- exactly the shape the summary
    counts, and it counted zero because it could not see it.

    The unit is the STATEMENT, not a window of lines. That distinction is the whole safety
    argument: the `!` that inverts `!matches!(env::var(X)...)` sits above the name but inside the
    same statement, so a statement always carries its own negation, while a window of n lines
    carries it only when n happens to be large enough.
    """
    statement = statement_around(lines, read_line)
    if len(FLAG_READ.findall(statement)) != 1:
        return ""
    # Boolean-shaped only. A statement reading a millisecond count has no ON/OFF to state.
    if not any(mark in statement for mark in
               ("matches!", "unwrap_or(true)", "unwrap_or(false)", ".is_ok()", ".is_err()")):
        return ""
    if DEFAULT_ON.search(statement):
        return "on"
    if DEFAULT_OFF.search(statement):
        return "off"
    index = statement.find("matches!(")
    if index < 0:
        return ""
    negated = index > 0 and statement[index - 1] == "!"
    # `unwrap_or_else(|_| "1")` supplies the default in the string itself rather than as a bool.
    fallback_on = '"1"' in statement or '"true"' in statement
    if negated:
        return "on" if fallback_on or "unwrap_or_default()" in statement else ""
    return "off" if "unwrap_or_default()" in statement else ""


def default_of(body: str, flags_read: int) -> str:
    """"on", "off", or "" -- the value of the boolean the function `body` returns when unset.

    Read from the WHOLE accessor rather than a window around the name. A window is what a first
    attempt used, and it reported five default-ON flags as off: they are written
    `!matches!(env::var(X).unwrap_or_default().trim(), "0" | "false")`, where the `!` that
    inverts the whole test sits several lines ABOVE the name and outside any window small enough
    to be safe. A wrong default is worse than a blank one -- it is read as fact -- so this reads
    the construct entire or says nothing.

    Only for a function that reads ONE flag and returns a bool. A function reading several has
    no single default to state, and attributing one of them to all of them is the same error in
    another costume. The bool requirement is the other half: `TS_PROXY_CONTEXT_IO_TIMEOUT_MS` is
    a millisecond count read inside a function that happens to contain an `.is_ok()` elsewhere,
    and without it this reported that timeout as defaulting "off".
    """
    if flags_read != 1 or not re.match(r"[^\n]*\)\s*->\s*bool\b", body):
        return ""
    body = body[: _end_of_flag_chain(body)]
    if DEFAULT_ON.search(body):
        return "on"
    if DEFAULT_OFF.search(body):
        return "off"
    if "unwrap_or_default()" not in body:
        return ""
    # `!matches!(value, "0" | "false")` is on unless switched off; the same without the `!` is
    # off unless switched on. Which one it is, is decided by the character before `matches!`.
    index = body.find("matches!(")
    if index < 0:
        return ""
    return "on" if index > 0 and body[index - 1] == "!" else "off"


# A number stated in the read itself, and the named constants such a read can point at.
UNWRAP_NUMBER = re.compile(r"\.unwrap_or\(\s*(-?\d[\d_]*)\s*\)")
UNWRAP_NAMED = re.compile(r"\.unwrap_or\(\s*([A-Z][A-Z0-9_]{2,})\s*\)")
CONST_LITERAL = re.compile(
    r"const ([A-Z][A-Z0-9_]+)\s*:\s*[a-z][a-z0-9]*\s*=\s*([^;]+);")
ARITHMETIC = re.compile(r"^[\d_ ()*<+]+$")


def literal_consts(bodies):
    """Every `const NAME = <literal>` whose value is a plain number or bool, as display text.

    Only arithmetic on literals is evaluated -- digits, `*`, `<<`, `+`, parentheses. A const
    computed from another const, or from a function call, is left out rather than guessed at: a
    wrong number in this table would be published as fact for every flag pointing at it.
    """
    found = {}
    for rel in sorted(bodies):
        for name, expr in CONST_LITERAL.findall(bodies[rel]):
            expr = expr.strip()
            if expr in ("true", "false"):
                found.setdefault(name, "on" if expr == "true" else "off")
            elif ARITHMETIC.match(expr):
                try:
                    found.setdefault(
                        name, str(eval(expr.replace("_", ""), {"__builtins__": {}}, {})))
                except Exception:
                    pass
    return found


def numeric_default_of_statement(lines, read_line, consts):
    r"""A number, as text, or "" -- the default of a flag whose read states one.

    `default_of_statement` answers this for booleans and says so: a statement reading a
    millisecond count has no ON/OFF to state. It had no answer for the count itself, so a page
    an operator consults to decide what to change showed an em dash for 26 flags whose default
    is written down three lines from the name.

    Only what comes AFTER the read can be its default. Scanning the whole statement reported
    `MATRIXARK_ACCOUNT_ID` -- which defaults to the string "acct_codex" -- as defaulting to
    1024, borrowed from a neighbouring field of the same struct literal, because a struct
    literal separates its fields with commas and so is one statement. Reading forward from the
    name cannot make that mistake.

    `unwrap_or_else(` cannot match `unwrap_or\(`, so a string fallback never reads as a number.
    """
    statement = statement_around(lines, read_line)
    first = FLAG_READ.search(statement)
    if not first or len(FLAG_READ.findall(statement)) != 1:
        return ""
    after = statement[first.end():]
    number = UNWRAP_NUMBER.search(after)
    if number:
        return number.group(1).replace("_", "")
    named = UNWRAP_NAMED.search(after)
    if named and named.group(1) in consts:
        value = consts[named.group(1)]
        return value if value not in ("on", "off") else ""
    return ""


def classify(name: str) -> str:
    for label, pattern in CLASS_RULES:
        if pattern.search(name):
            return label
    return "behaviour"


sources = {}
for path in sorted(SRC.rglob("*.rs")):
    rel = str(path.relative_to(SRC)).replace("\\", "/")
    sources[rel] = path.read_text(encoding="utf-8", errors="ignore")

prod = {k: strip_test_modules(v) for k, v in sources.items()
        if "/tests" not in k and not k.startswith("tests")}

# Named constants a numeric read can point at, resolved once so every flag sees the same table.
CONSTS = literal_consts(prod)

flags = {}
# Distinct functions that read a flag, as (file, line) -> name. Several flags can share one, so
# this is not len(flags).
accessors = {}
for rel, text in prod.items():
    lines = text.splitlines()
    for match in FLAG_READ.finditer(text):
        name = match.group(1)
        entry = flags.setdefault(name, {"sites": set(), "doc": "", "default": ""})
        entry["sites"].add(rel)
        # Two things are wanted per flag and they are not found at the same site: the doc comment
        # comes from wherever one was written, the default only from the accessor that returns a
        # bool. Skipping the rest of a flag's sites once a doc was found also skipped its
        # accessor -- `TS_VECTOR_INT8` is read first by a function with a doc and a moment later
        # by `vector_int8_enabled`, and only the second one knows what unset means.
        if entry["doc"] and entry["default"]:
            continue
        line_no = text[:match.start()].count("\n")
        fn_line = None
        for i in range(line_no, max(line_no - 40, -1), -1):
            # `pub(crate)` was the only visibility this knew about, so a `pub(super) fn`
            # accessor -- which is what most of the per-module gates are -- did not read as a
            # function at all: the walk continued past it to whatever function came before, and
            # the flag was credited with that one's doc comment or none. Both `TS_BLOCK_IN_WAL`
            # and `TS_HOT_PAGE_SPILL` say "restore the previous behaviour" in their own doc and
            # were reported as keeping no older path alive.
            if re.match(r"\s*(pub(\([a-z():]+\))?\s+)?(async\s+)?fn ", lines[i]):
                fn_line = i
                break
        doc = []
        if fn_line is not None:
            i = fn_line - 1
            while i >= 0 and lines[i].strip().startswith("///"):
                doc.append(lines[i].strip()[3:].strip())
                i -= 1
            doc.reverse()
        if fn_line is not None:
            match_name = re.match(
                r"\s*(?:pub(?:\([a-z():]+\))?\s+)?(?:async\s+)?fn\s+([A-Za-z0-9_]+)",
                lines[fn_line])
            if match_name:
                accessors[(rel, fn_line)] = match_name.group(1)
        if fn_line is not None and not entry["default"]:
            body = NEWLINE.join(lines[fn_line:function_end(lines, fn_line)])
            entry["default"] = default_of(body, len(set(FLAG_READ.findall(body))))
        if not entry["default"]:
            entry["default"] = default_of_statement(lines, line_no)
        if not entry["default"]:
            entry["default"] = numeric_default_of_statement(lines, line_no, CONSTS)
        if entry["doc"]:
            continue
        text_doc, shared_with = doc_for_flag(name, " ".join(doc), lines, fn_line)
        if shared_with:
            entry["doc_shared_with"] = shared_with
        entry["doc"] = text_doc

# knobs named by a constant, which no env::var scan sees
for rel, text in prod.items():
    for ident, value in re.findall(
            r'pub const ([A-Z0-9_]+)\s*:\s*&(?:\'static\s+)?str\s*=\s*'
        r'"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"', text):
        entry = flags.setdefault(value, {"sites": set(), "doc": "", "default": ""})
        entry["sites"].add(rel)
        # storage_config.rs declares the variable NAME and its DEFAULT as neighbouring consts and
        # reads one with the other, so the default is derivable even though no scan of string
        # literals can see the read. The pairing is on the const IDENTIFIER, not the string:
        # `TS_BLOCK_SLAB_TARGET_BYTES` names the variable `TS_BLOCK_SEGMENT_TARGET_BYTES`, and
        # matching on the string would silently drop it.
        if not entry["default"] and ident.startswith("TS_"):
            entry["default"] = CONSTS.get("DEFAULT_" + ident[len("TS_"):], "")

# Where a flag is given a value, as opposed to read.
#
# This is the column the inventory was missing. "How many files does it reach" measures the code
# its off-path keeps alive; it says nothing about whether that path is ever taken. A flag that no
# test, config file, launch profile or portal setting ever sets is not a switch anyone throws --
# it is a branch with one live side, and the other side can go.
#
# Deliberately literal: it reports WHERE a value is set, never whether that value differs from the
# default, because the default lives in Rust and this reads text. An owner reading "config" still
# has to look; an owner reading "nothing" does not.
NAME = r"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"
SETTERS = [
    ("test", re.compile(r'(?:set_var|EnvFlagGuard::(?:set|off)|EnvGuard::(?:set|off))\(\s*"%s"'
                        % NAME)),
    ("launch", re.compile(r"export\s+%s=" % NAME)),
    ("harness", re.compile(r'\.envs?\(\s*(?:&\[)?\(?\s*"%s"' % NAME)),
    # Python launchers put a value in the environment of the process they start. Without this the
    # column would say "nothing" for anything only the Python side configures, which is most of
    # what an operator actually turns on.
    ("script", re.compile(r'(?:environ(?:\.setdefault)?|env)\s*[\[(]\s*"%s"\s*[\])]?\s*(?:,|=[^=])'
                          % NAME)),
]
CONFIG_NAME = re.compile(NAME)
SET_ROOTS = [
    (ROOT / "crates" / "temporalstore-rust" / "src", (".rs",)),
    (ROOT / "crates" / "temporalstore-rust" / "tests", (".rs",)),
    (ROOT / "config", (".toml",)),
    (ROOT / "tools", (".sh", ".py")),
    (ROOT / "deploy", (".sh", ".yml", ".yaml", ".env")),
]


def _setters_by_name():
    """name -> {kinds}, in ONE pass over the corpus.

    Asking "where is THIS name set" per flag re-read every file three hundred times and turned a
    one-second generator into a minutes-long one, which is how a generated document stops being
    regenerated. The names are matched by the same patterns, read out of the text rather than
    substituted into it -- so nesting (`TS_META_RAFT` inside `TS_META_RAFT_NODES`) resolves by
    what the pattern actually captured, not by a substring test that would credit the shorter
    name with the longer one's setting.
    """
    found = {}
    for root, suffixes in SET_ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            body = path.read_text(encoding="utf-8", errors="ignore")
            if path.suffix == ".toml":
                # The shipped config names each variable in the comment beside the key it maps
                # to, so appearing at all is appearing as a setting.
                for name in CONFIG_NAME.findall(body):
                    found.setdefault(name, set()).add("config")
                continue
            for label, pattern in SETTERS:
                for name in pattern.findall(body):
                    found.setdefault(name, set()).add(label)
    return found


SET_BY = _setters_by_name()


def set_by(name):
    """Sorted kinds of place that give `name` a value. Empty means nothing does."""
    return sorted(SET_BY.get(name, ()))


sys.path.insert(0, str(ROOT / "tools"))
try:
    import matrixark_gateway_config as cfg
    offered = {s.env for s in cfg.SETTINGS if s.env}
except Exception:
    offered = set()

rows = []
for name in sorted(flags):
    entry = flags[name]
    doc = entry["doc"]
    legacy = bool(LEGACY.search(doc))
    setters = set_by(name)
    if name in offered:
        setters.append("portal")
    rows.append({
        "name": name,
        "group": classify(name),
        "sites": len(entry["sites"]),
        "legacy": legacy,
        "default": entry.get("default", ""),
        "offered": name in offered,
        "set_by": setters,
        "doc": doc,
        "shared_with": entry.get("doc_shared_with", []),
    })

by_group = {}
for row in rows:
    by_group.setdefault(row["group"], []).append(row)

lines = [
    "# Engine flags",
    "",
    "Every environment variable the engine reads -- `TS_*`, `MATRIXARK_*` and",
    "`TEMPORALSTORE_*` -- grouped by what it decides. Generated from the source",
    "by `tools/build_engine_flag_inventory.py` and checked by",
    "`test_matrixark_engine_flag_inventory.py`, because a hand-kept list of this many knobs is wrong",
    "within a week and its staleness is silent.",
    "",
    "## Why this exists",
    "",
    "There are %d of them, read by %d functions." % (len(rows), len(accessors)),
    "",
    "Deleting unreachable code is not the lever. An earlier version of this document argued that",
    "by asserting every accessor had a caller -- true when it was hand-checked at 55, and carried",
    "forward unexamined ever since. Computing it instead named four functions as uncalled that",
    "plainly are: Rust reaches a function by more than one syntax (a generic definition,",
    "`unwrap_or_else(name)` passing it as a value, serde naming it in a string) and a name scan",
    "sees none of those. So the claim is neither asserted nor computed here.",
    ""
    "",
    "What shortens it is a narrower question, asked per flag: **does anything anywhere select the",
    "off position?** Not a test, not a config file, not a launch profile, not a portal setting. A",
    "flag no one can be shown to turn off is not a switch, it is a branch -- and the live side can",
    "be made unconditional, taking the dead side with it. That is a safe edit exactly when reads do",
    "not consult the flag: if the decoder already accepts both shapes, retiring the writer strands",
    "nothing already written.",
    "",
    "What the list gives an owner asking that is: how many files each flag reaches (the code its",
    "non-default path keeps alive), whether its own documentation calls that path legacy, and --",
    "the two columns that answer the question -- its **default**, where that can be read off the",
    "source, and **who sets it**: a test, the shipped config file, a launch profile, a Python",
    "launcher, a test harness, or the customer portal. `nothing` means no place in this",
    "repository gives it a value, so whatever its non-default path does, nothing here asks for it.",
    "",
    "That column is literal about WHERE, not about WHAT: it does not compare the value set against",
    "the default, because the default lives in Rust and this reads text. `config` is a prompt to go",
    "look; `nothing` is an answer.",
    "",
    "The **default** column reads two shapes: a function that reads one flag and returns a bool,",
    "and a single statement that does the same inline. The second was added because the first",
    "made the row below it -- flags defaulting on that nothing sets -- report zero while",
    "`MATRIXARK_RUST_PROXY_ASYNC_CACHE_WARM_ON_LOAD` sat inline in the middle of a function,",
    "defaulting on, set by nothing. A statement is the unit rather than a window of lines because",
    "the `!` that inverts `!matches!(env::var(X)...)` sits above the name but inside the same",
    "statement: a statement always carries its own negation, a window carries it only by luck.",
    "Anything else is blank, and a blank means go and look.",
    "",
    "| flags | count |",
    "|---|---|",
    "| total | %d |" % len(rows),
    "| booleans whose default this could read off the source | %d |"
    % sum(1 for r in rows if r["default"] in ("on", "off")),
    "| numbers whose default this could read off the source | %d |"
    % sum(1 for r in rows if r["default"] and r["default"] not in ("on", "off")),
    "| **defaulting on, and set by nothing** | %d |"
    % sum(1 for r in rows if r["default"] == "on" and not r["set_by"]),
    "| offered on the portal | %d |" % sum(1 for r in rows if r["offered"]),
    "| **that nothing in this repository sets** | %d |"
    % sum(1 for r in rows if not r["set_by"]),
    "| documented as keeping an older path alive | %d |" % sum(1 for r in rows if r["legacy"]),
    "| reaching more than two files | %d |" % sum(1 for r in rows if r["sites"] > 2),
    # Zero today, and measured rather than assumed: see doc_for_flag.
    "| whose doc comment is really about another flag | %d |"
    % sum(1 for r in rows if r["shared_with"]),
    "",
]

ORDER = ["topology", "cluster policy", "credential", "durability", "format", "capacity",
         "context", "identity", "diagnostic", "benchmark", "behaviour"]
BLURB = {
    "topology": "Where this node is and what it talks to. Set by whoever provisions the node; not "
                "tenant-facing and not tuning.",
    "credential": "Secrets. Never a form field, never in a launch artifact.",
    "durability": "What is written, when it is flushed, and what is reclaimed. The escape hatches "
                  "here trade throughput for a more conservative barrier.",
    "format": "The shape of what is written. Readers generally accept both shapes, which is what "
              "makes these safe to flip and hard to retire.",
    "capacity": "Sizes, ceilings and intervals. The tuning a deployment actually reaches for.",
    "diagnostic": "Extra evidence for someone investigating. Off by default.",
    "identity": "Who the caller is, not what the engine does. Supplied per request or per "
                "process; nothing here is a switch.",
    "benchmark": "Read only by the benchmark harnesses. Never consulted on a serving path.",
    "context": "The memory pipeline: what gets extracted, embedded, drained and packed. The "
               "surface a deployment tunes for recall rather than for throughput.",
    "cluster policy": "What the metaserver does about nodes it cannot reach -- conviction, "
                      "freezing, rebalancing, failover. Cluster-wide, and never per shard.",
    "behaviour": "Everything else that changes what the engine does.",
}

for group in ORDER:
    members = by_group.get(group, [])
    if not members:
        continue
    lines.append("## %s (%d)" % (group, len(members)))
    lines.append("")
    lines.append(BLURB[group])
    lines.append("")
    lines.append("| flag | default | set by | files | keeps an older path |")
    lines.append("|---|---|---|---|---|")
    for row in sorted(members, key=lambda r: (-r["sites"], r["name"])):
        lines.append("| `%s` | %s | %s | %d | %s |" % (
            row["name"], row["default"] or "—",
            ", ".join(row["set_by"]) if row["set_by"] else "nothing",
            row["sites"],
            "yes" if row["legacy"] else "—"))
    lines.append("")

# --- where the engine ends ----------------------------------------------------------------------
# `sdk/rust/temporalstore` is a SECOND Rust tree with its own copy of the proxy implementation --
# a different file, not a stale duplicate: 121 KB against 345 KB. The root manifest carries it as
# `exclude = ["sdk/rust/temporalstore"]`, so it is not a workspace member and `cargo check
# --all-targets` never builds it, in CI or here.
#
# That makes it out of scope for a document about the engine, and it stays out of scope. What it
# must not be is invisible: a reader who greps this file for a variable the SDK reads finds
# nothing and concludes nothing reads it. So the boundary is stated, and the names on the far
# side of it are computed rather than listed by hand, which is the only version that stays true.
SDK_SRC = ROOT / "sdk" / "rust" / "temporalstore" / "src"
sdk_names = set()
if SDK_SRC.is_dir():
    for path in sorted(SDK_SRC.rglob("*.rs")):
        sdk_names.update(FLAG_READ.findall(io.open(path, encoding="utf-8").read()))
sdk_only = sorted(sdk_names - {r["name"] for r in rows})
if sdk_only:
    lines.append("## outside this document (%d)" % len(sdk_only))
    lines.append("")
    lines.append("`sdk/rust/temporalstore` is a second Rust tree, carrying its own copy of the")
    lines.append("proxy implementation -- a different file, not a stale duplicate. The root")
    lines.append("manifest excludes it from the workspace, so `cargo check --all-targets` does not")
    lines.append("build it, here or in CI, and it is a client of the engine rather than part of")
    lines.append("it. These are the variables it reads that this document does not cover. The list")
    lines.append("is computed, so one more cannot appear quietly.")
    lines.append("")
    for name in sdk_only:
        lines.append("- `%s`" % name)
    lines.append("")

OUT.parent.mkdir(parents=True, exist_ok=True)
io.open(OUT, "w", encoding="utf-8", newline="\n").write("\n".join(lines) + "\n")
print("  wrote %s" % OUT)
print("  flags: %d   offered: %d   legacy-shaped: %d   set by nothing: %d"
      % (len(rows), sum(1 for r in rows if r["offered"]), sum(1 for r in rows if r["legacy"]),
         sum(1 for r in rows if not r["set_by"])))

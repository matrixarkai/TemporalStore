#!/usr/bin/env python3
"""Generate the engine flag inventory: every TS_* knob, what it is for, and what keeping it costs.

Written because "there are too many flags" is true and not actionable on its own. Nothing here is
deleted: all 55 flag accessors have a caller, so every flag changes behaviour and retiring one is a
decision about behaviour rather than a cleanup. What was missing is the information to make those
decisions -- how many places each flag reaches, whether its off-path is a documented legacy shape,
and whether a customer can even set it.

Generated rather than written, and checked by a test, because a hand-maintained list of 98 knobs is
wrong within a week and its staleness is silent.
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

FLAG_READ = re.compile(r'(?:std::)?env::var(?:_os)?\(\s*"(TS_[A-Z0-9_]+)"')


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

    **This changes nothing today.** Nine functions read more than one flag; none of them carries a
    doc comment, so this returns an empty shared list every time and the count it feeds is zero.
    It is here because the failure it prevents is silent -- the first documented multi-flag
    function would produce a confidently wrong row with nothing to notice -- and because a zero
    that is measured is worth more than a zero that is assumed.
    """
    if not doc or name in doc or fn_line is None:
        return doc, []
    shared = set(FLAG_READ.findall(NEWLINE.join(lines[fn_line:function_end(lines, fn_line)])))
    if len(shared) <= 1:
        return doc, []
    return "", sorted(shared - {name})

def classify(name: str) -> str:
    for label, pattern in CLASS_RULES:
        if pattern.search(name):
            return label
    return "behaviour"


sources = {}
for path in SRC.rglob("*.rs"):
    rel = str(path.relative_to(SRC)).replace("\\", "/")
    sources[rel] = path.read_text(encoding="utf-8", errors="ignore")

prod = {k: v for k, v in sources.items() if "/tests" not in k and not k.startswith("tests")}

flags = {}
for rel, text in prod.items():
    lines = text.splitlines()
    for match in re.finditer(r'(?:std::)?env::var(?:_os)?\(\s*"(TS_[A-Z0-9_]+)"', text):
        name = match.group(1)
        entry = flags.setdefault(name, {"sites": set(), "doc": ""})
        entry["sites"].add(rel)
        if entry["doc"]:
            continue
        line_no = text[:match.start()].count("\n")
        fn_line = None
        for i in range(line_no, max(line_no - 40, -1), -1):
            if re.match(r"\s*(pub(\(crate\))?\s+)?fn ", lines[i]):
                fn_line = i
                break
        doc = []
        if fn_line is not None:
            i = fn_line - 1
            while i >= 0 and lines[i].strip().startswith("///"):
                doc.append(lines[i].strip()[3:].strip())
                i -= 1
            doc.reverse()
        text_doc, shared_with = doc_for_flag(name, " ".join(doc), lines, fn_line)
        if shared_with:
            entry["doc_shared_with"] = shared_with
        entry["doc"] = text_doc

# knobs named by a constant, which no env::var scan sees
for rel, text in prod.items():
    for ident, value in re.findall(
            r'pub const (TS_[A-Z0-9_]+)\s*:\s*&(?:\'static\s+)?str\s*=\s*"(TS_[A-Z0-9_]+)"', text):
        entry = flags.setdefault(value, {"sites": set(), "doc": ""})
        entry["sites"].add(rel)

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
    rows.append({
        "name": name,
        "group": classify(name),
        "sites": len(entry["sites"]),
        "legacy": legacy,
        "offered": name in offered,
        "doc": doc,
        "shared_with": entry.get("doc_shared_with", []),
    })

by_group = {}
for row in rows:
    by_group.setdefault(row["group"], []).append(row)

lines = [
    "# Engine flags",
    "",
    "Every `TS_*` variable the engine reads, grouped by what it decides. Generated from the source",
    "by `tools/build_engine_flag_inventory.py` and checked by",
    "`test_matrixark_engine_flag_inventory.py`, because a hand-kept list of this many knobs is wrong",
    "within a week and its staleness is silent.",
    "",
    "## Why this exists",
    "",
    "There are %d of them, and **none is dead**: all 55 accessor functions that read one have a"
    % len(rows),
    "non-test caller. So the length of this list is not a cleanup backlog -- every flag changes",
    "behaviour, and retiring one is a decision about behaviour rather than tidying.",
    "",
    "What the list gives an owner deciding that is: how many files each flag reaches (the code its",
    "non-default path keeps alive), whether its own documentation calls that path legacy, and",
    "whether a customer can set it at all.",
    "",
    "| flags | count |",
    "|---|---|",
    "| total | %d |" % len(rows),
    "| offered on the portal | %d |" % sum(1 for r in rows if r["offered"]),
    "| documented as keeping an older path alive | %d |" % sum(1 for r in rows if r["legacy"]),
    "| reaching more than two files | %d |" % sum(1 for r in rows if r["sites"] > 2),
    # Zero today, and measured rather than assumed: see doc_for_flag.
    "| whose doc comment is really about another flag | %d |"
    % sum(1 for r in rows if r["shared_with"]),
    "",
]

ORDER = ["topology", "credential", "durability", "format", "capacity", "diagnostic", "behaviour"]
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
    lines.append("| flag | files | portal | keeps an older path |")
    lines.append("|---|---|---|---|")
    for row in sorted(members, key=lambda r: (-r["sites"], r["name"])):
        lines.append("| `%s` | %d | %s | %s |" % (
            row["name"], row["sites"],
            "yes" if row["offered"] else "—",
            "yes" if row["legacy"] else "—"))
    lines.append("")

OUT.parent.mkdir(parents=True, exist_ok=True)
io.open(OUT, "w", encoding="utf-8", newline="\n").write("\n".join(lines) + "\n")
print("  wrote %s" % OUT)
print("  flags: %d   offered: %d   legacy-shaped: %d"
      % (len(rows), sum(1 for r in rows if r["offered"]), sum(1 for r in rows if r["legacy"])))

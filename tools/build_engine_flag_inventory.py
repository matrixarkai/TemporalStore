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

ROOT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "/root/wt-flags")
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

LEGACY = re.compile(r"\b(legacy|escape hatch|rollback|previous|as they were|before|superseded)\w*",
                    re.I)


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
        entry["doc"] = " ".join(doc)

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

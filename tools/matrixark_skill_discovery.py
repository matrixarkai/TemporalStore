#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Skill discovery / extraction — mine reusable procedures from agent interactions.

The skill *storage + retrieval + budget* lane already exists in TemporalStore:
`skill_manifest` / `skill_section` records, the resource-and-skill scan the retrieve path runs
over the folders it selects, and the shared-skill budget slice. What was missing is the *discovery* layer that turns
the agent's own repeated tool-use procedures into skills. This module implements the
discover -> capture -> learn loop, in pure Python (MatrixArk front), emitting records
that drop straight into the existing retrieval lane.

  discover  split interaction history into task *episodes* (a user request and the
            tool actions taken to satisfy it); take each episode's ordered
            tool-action *signature*; cluster episodes by signature.
  capture   a signature seen >= min_support times, with >= min_steps tool steps,
            becomes a SkillSpec (name, triggers, ordered steps, allowed_tools,
            examples, provenance) and is rendered into skill_manifest / skill_registry
            / skill_section records.
  learn     re-running raises a skill's support count and version as it recurs; a
            content hash keeps the skill stable when the procedure is unchanged.

The discovery is deterministic (no LLM required), so it is testable and cheap; an LLM
naming/summarization pass can be layered on top of `SkillSpec` later without changing
the record shape.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

try:  # integrate with the store's hashing + record builders when available
    from tools.matrixark_mcp_core import stable_hash  # type: ignore
    from tools.matrixark_mcp_ingest_skill_records import (  # type: ignore
        skill_manifest_record,
        skill_registry_record,
        skill_section_record,
    )
    _HAVE_BUILDERS = True
except ModuleNotFoundError:  # pragma: no cover - exercised via direct tools/ import
    try:
        from matrixark_mcp_core import stable_hash  # type: ignore
        from matrixark_mcp_ingest_skill_records import (  # type: ignore
            skill_manifest_record,
            skill_registry_record,
            skill_section_record,
        )
        _HAVE_BUILDERS = True
    except ModuleNotFoundError:  # discovery can still run standalone (naming/specs only)
        import hashlib

        def stable_hash(value: Any) -> int:  # type: ignore
            return int(hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:15], 16)

        skill_manifest_record = skill_registry_record = skill_section_record = None  # type: ignore
        _HAVE_BUILDERS = False

Json = dict[str, Any]

# --------------------------------------------------------------------------------------
# Tuning (env-overridable by the caller; kept as plain module constants here).
# --------------------------------------------------------------------------------------
MIN_SUPPORT = 3          # a procedure must recur at least this many times to be a skill
MIN_STEPS = 2            # ... and involve at least this many tool actions
MAX_SKILLS = 64          # cap on discovered skills per run (highest-support first)
MAX_EXAMPLES = 4         # representative trigger snippets kept per skill
MAX_TRIGGERS = 8         # trigger keywords kept per skill
COLLAPSE_REPEATS = True  # collapse immediately-repeated tools in the signature (Read,Read->Read)

# Primary argument keys to summarize a tool call (mirrors the ingest tool-leaning order).
_ARG_KEYS = ("command", "file_path", "path", "pattern", "query", "url", "notebook_path", "name", "description")

_STOP = set(
    "the a an and or of to in on for with is are was were be been being this that these those "
    "i you we it he she they them his her our your my me can could should would will just now "
    "please help need want make get let do does did done how what why when where which who".split()
)
# Verbs that make good skill-name heads, in rough priority order.
_INTENT_VERBS = (
    "fix", "debug", "test", "run", "build", "deploy", "install", "configure", "refactor",
    "search", "find", "add", "remove", "update", "rename", "migrate", "review", "benchmark",
    "commit", "push", "generate", "validate", "analyze", "profile", "trace", "parse", "ingest",
)


# Path/format fragments that leak in from file paths and add no intent signal to a name.
_PATH_NOISE = set(
    "appdata local users deeproute mnt root tmp temp var src github com www http https "
    "documents downloads desktop home usr bin lib etc jsonl json yaml yml txt md py rs toml "
    "html css js ts sh log out err path file dir folder null none true false".split()
)


def _tokens(text: str) -> list[str]:
    return [w for w in re.findall(r"[a-z][a-z0-9_+.#-]{1,}", (text or "").lower()) if w not in _STOP]


def _name_tokens(text: str) -> list[str]:
    """Stricter keyword set for naming/triggers: plain words, no path/format fragments."""
    return [w for w in re.findall(r"[a-z]{3,20}", (text or "").lower())
            if w not in _STOP and w not in _PATH_NOISE]


def _slug(text: str, max_len: int = 48) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return s[:max_len].strip("-") or "skill"


def _token_estimate(text: str) -> int:
    return max(1, len(text) // 4)


def normalize_tool(name: str) -> str:
    """Normalize a tool name to a stable, comparable token."""
    n = re.sub(r"^(mcp__[^_]+__|functions\.)", "", str(name or "").strip())
    return n or "tool"


# --------------------------------------------------------------------------------------
# Interaction model
# --------------------------------------------------------------------------------------
@dataclass
class InteractionEvent:
    session_id: str
    ts_ms: int
    role: str                 # "user" | "assistant" | "tool"
    text: str = ""
    tool_name: str = ""       # set for tool_use events
    tool_input: Json = field(default_factory=dict)
    origin: str = ""          # "claude" | "codex" | ...


@dataclass
class Episode:
    session_id: str
    trigger_text: str
    actions: list[tuple[str, str]]   # ordered (normalized_tool, arg_summary)
    start_ms: int
    end_ms: int


def _arg_summary(tool_input: Json) -> str:
    if isinstance(tool_input, dict):
        for k in _ARG_KEYS:
            v = tool_input.get(k)
            if v:
                return " ".join(str(v).split())[:80]
        if tool_input:
            return " ".join(json.dumps(tool_input, ensure_ascii=False).split())[:80]
    return ""


def mine_episodes(events: Iterable[InteractionEvent]) -> list[Episode]:
    """Split each session's stream into task episodes bounded by user turns.

    An episode = one user request + the tool actions taken before the next user turn.
    Episodes with no tool actions are dropped (nothing procedural to learn).
    """
    by_session: dict[str, list[InteractionEvent]] = {}
    for e in events:
        by_session.setdefault(e.session_id, []).append(e)

    episodes: list[Episode] = []
    for sid, evs in by_session.items():
        evs = sorted(evs, key=lambda e: e.ts_ms)
        cur: Optional[Episode] = None
        for e in evs:
            if e.role == "user" and (e.text or "").strip():
                if cur and cur.actions:
                    episodes.append(cur)
                cur = Episode(session_id=sid, trigger_text=e.text.strip(), actions=[], start_ms=e.ts_ms, end_ms=e.ts_ms)
            elif e.tool_name and cur is not None:
                cur.actions.append((normalize_tool(e.tool_name), _arg_summary(e.tool_input)))
                cur.end_ms = e.ts_ms
        if cur and cur.actions:
            episodes.append(cur)
    return episodes


def episode_signature(ep: Episode, *, collapse_repeats: bool = COLLAPSE_REPEATS) -> tuple[str, ...]:
    """Ordered tool sequence of an episode — the procedure's fingerprint."""
    seq = [t for t, _ in ep.actions]
    if collapse_repeats:
        out: list[str] = []
        for t in seq:
            if not out or out[-1] != t:
                out.append(t)
        seq = out
    return tuple(seq)


# --------------------------------------------------------------------------------------
# Skill specification
# --------------------------------------------------------------------------------------
@dataclass
class SkillSpec:
    name: str
    slug: str
    description: str
    triggers: list[str]
    allowed_tools: list[str]
    steps: list[str]                 # human-readable ordered procedure
    examples: list[str]              # representative trigger snippets
    signature: tuple[str, ...]
    support: int                     # episodes matching this procedure
    sessions: list[str]              # distinct sessions it was observed in
    content_hash: str = ""

    def to_manifest_metadata(self) -> Json:
        return {
            "owner_scope": "user",
            "version": "1",
            "status": "active",
            "precedence": "normal",
            "triggers": list(self.triggers),
            "allowed_tools": list(self.allowed_tools),
            "examples": list(self.examples),
            "permissions": [],
            "inputs": [],
            "outputs": [],
            "source": "skill_discovery",
            "support": self.support,
            "distinct_sessions": len(self.sessions),
            "signature": list(self.signature),
            "content_hash": self.content_hash,
        }

    def render_markdown(self) -> str:
        lines = [f"# {self.name}", "", self.description, ""]
        if self.triggers:
            lines += [f"**Triggers:** {', '.join(self.triggers)}", ""]
        if self.allowed_tools:
            lines += [f"**Tools:** {', '.join(self.allowed_tools)}", ""]
        lines += ["## Procedure", ""]
        lines += [f"{i}. {s}" for i, s in enumerate(self.steps, 1)]
        if self.examples:
            lines += ["", "## Seen in", ""]
            lines += [f"- {ex}" for ex in self.examples]
        lines += ["", f"_Discovered from {self.support} occurrences across {len(self.sessions)} session(s)._"]
        return "\n".join(lines)


def _skill_name(triggers: list[str], signature: tuple[str, ...]) -> str:
    """A readable name: an intent verb + primary object, falling back to the tool chain."""
    verb = next((t for t in triggers if t in _INTENT_VERBS), "")
    obj = next((t for t in triggers if t != verb and len(t) > 2), "")
    if verb and obj:
        return f"{verb.capitalize()} {obj}"
    if verb:
        return f"{verb.capitalize()} workflow"
    if triggers:
        return " ".join(w.capitalize() for w in triggers[:2])
    return " → ".join(signature) if signature else "Procedure"


def _skill_steps(signature: tuple[str, ...], reps: dict[str, str]) -> list[str]:
    steps = []
    for tool in signature:
        arg = reps.get(tool, "")
        steps.append(f"`{tool}`" + (f" — {arg}" if arg else ""))
    return steps


def discover_skills(
    events: Iterable[InteractionEvent],
    *,
    min_support: int = MIN_SUPPORT,
    min_steps: int = MIN_STEPS,
    max_skills: int = MAX_SKILLS,
    collapse_repeats: bool = COLLAPSE_REPEATS,
) -> list[SkillSpec]:
    """Discover reusable skills from interaction history."""
    episodes = mine_episodes(events)
    clusters: dict[tuple[str, ...], list[Episode]] = {}
    for ep in episodes:
        sig = episode_signature(ep, collapse_repeats=collapse_repeats)
        if len(sig) < min_steps:
            continue
        clusters.setdefault(sig, []).append(ep)

    specs: list[SkillSpec] = []
    for sig, eps in clusters.items():
        if len(eps) < min_support:
            continue
        # triggers: most common significant keywords across the episodes' requests
        term_freq: dict[str, int] = {}
        for ep in eps:
            for w in set(_name_tokens(ep.trigger_text)):
                term_freq[w] = term_freq.get(w, 0) + 1
        triggers = [w for w, _ in sorted(term_freq.items(), key=lambda kv: (-kv[1], kv[0]))[:MAX_TRIGGERS]]
        # representative arg per tool (first non-empty seen, deterministic by episode order)
        reps: dict[str, str] = {}
        for ep in sorted(eps, key=lambda e: (e.start_ms, e.session_id)):
            for tool, arg in ep.actions:
                if arg and tool not in reps:
                    reps[tool] = arg
        allowed_tools = sorted(set(sig))
        examples = [ep.trigger_text[:120] for ep in sorted(eps, key=lambda e: e.start_ms)[:MAX_EXAMPLES]]
        sessions = sorted({ep.session_id for ep in eps})
        name = _skill_name(triggers, sig)
        slug = _slug(name if name != " → ".join(sig) else "-".join(sig))
        steps = _skill_steps(sig, reps)
        description = (
            f"Reusable procedure the agent repeated {len(eps)} times: "
            + " → ".join(sig)
            + (f". Typically for: {', '.join(triggers[:4])}." if triggers else ".")
        )
        content_hash = f"{stable_hash('|'.join(list(sig) + triggers + allowed_tools)):x}"
        specs.append(
            SkillSpec(
                name=name, slug=slug, description=description, triggers=triggers,
                allowed_tools=allowed_tools, steps=steps, examples=examples, signature=sig,
                support=len(eps), sessions=sessions, content_hash=content_hash,
            )
        )

    # highest-support (then most sessions, then name) first; cap
    specs.sort(key=lambda s: (-s.support, -len(s.sessions), s.slug))
    return specs[:max_skills]


# --------------------------------------------------------------------------------------
# Capture: render a SkillSpec into store records that feed the existing retrieval lane.
# --------------------------------------------------------------------------------------
def skill_records_for_spec(
    spec: SkillSpec,
    *,
    scope: Json,
    deployment_scope: str = "user",
    sharing_scope: str = "user",
    node_path: Optional[list[str]] = None,
    updated_at_ms: int = 0,
) -> list[Json]:
    """Build skill_manifest + skill_registry + skill_section records for one skill.

    These are exactly the record types the retrieve path's resource-and-skill scan reads and
    the shared-skill budget slice funds, so a discovered skill becomes retrievable with
    no changes to the retrieval path. Requires the store's record builders; raises if
    they are unavailable (standalone/naming-only mode).
    """
    if not _HAVE_BUILDERS:
        raise RuntimeError("skill record builders unavailable; import matrixark_mcp_core / ingest_skill_records")
    node_path = node_path or ["skills", "discovered", spec.slug]
    raw_uri = f"skill://discovered/{spec.slug}"
    skill_hash = stable_hash(f"discovered_skill:{spec.slug}:{spec.content_hash}")
    import_task_hash = stable_hash(f"skill_discovery_task:{skill_hash}:{updated_at_ms}")
    node_hash = stable_hash(":".join(node_path))
    raw_uri_hash = stable_hash(raw_uri)
    access_scope = {**scope, "sharing_scope": sharing_scope}
    body = spec.render_markdown()
    metadata = spec.to_manifest_metadata()
    storage_resolution = {"upload_status": "not_required", "cloud_bucket": "", "cloud_key": ""}

    records: list[Json] = [
        skill_manifest_record(
            skill_hash=skill_hash, import_task_hash=import_task_hash, node_hash=node_hash,
            node_path=node_path, raw_uri=raw_uri, requested_raw_uri=raw_uri,
            raw_storage_mode="inline", raw_storage_policy="raw_uri_only",
            storage_resolution=storage_resolution, name=spec.name, description=spec.description,
            metadata=metadata, access_scope=access_scope, deployment_scope=deployment_scope,
            text=body, token_estimate=_token_estimate(body), serving_metadata=metadata,
            scope=scope, storage_options={}, updated_at_ms=updated_at_ms,
        ),
        skill_registry_record(
            skill_hash=skill_hash, import_task_hash=import_task_hash, raw_uri=raw_uri,
            requested_raw_uri=raw_uri, raw_storage_mode="inline", raw_storage_policy="raw_uri_only",
            storage_resolution=storage_resolution, name=spec.name, description=spec.description,
            metadata=metadata, access_scope=access_scope, deployment_scope=deployment_scope,
            node_hash=node_hash, node_path=node_path, scope=scope, updated_at_ms=updated_at_ms,
        ),
    ]
    # one section per procedure part: a "Procedure" section + a "Triggers" section
    sections = [("Procedure", "\n".join(spec.steps)), ("Triggers", ", ".join(spec.triggers))]
    for heading, text in sections:
        if not text.strip():
            continue
        section_hash = stable_hash(f"{skill_hash}:{heading}")
        records.append(
            skill_section_record(
                import_task_hash=import_task_hash, skill_hash=skill_hash, section_hash=section_hash,
                node_hash=node_hash, node_path=node_path, raw_uri_hash=raw_uri_hash,
                source_locator=f"{raw_uri}#{_slug(heading)}", heading=heading, text=text,
                token_estimate=_token_estimate(text),
                metadata={"triggers": spec.triggers, "allowed_tools": spec.allowed_tools, "source": "skill_discovery"},
                access_scope=access_scope, deployment_scope=deployment_scope, scope=scope,
                updated_at_ms=updated_at_ms,
            )
        )
    return records


def skill_ingest_envelope(spec: SkillSpec, *, scope: Json, ingestion_time_ms: int = 0) -> Json:
    """A `kind="skill"` ingest envelope for the live import path (ingest_resource_or_skill_if_needed)."""
    raw_uri = f"skill://discovered/{spec.slug}"
    return {
        "kind": "skill",
        "resource_type": "skill",
        "raw_uri": raw_uri,
        "text": spec.render_markdown(),
        "scope": scope,
        "ingestion_time_ms": ingestion_time_ms,
        "metadata": {
            "raw_uri": raw_uri, "resource_type": "skill", "name": spec.name,
            "description": spec.description, **spec.to_manifest_metadata(),
        },
    }


# --------------------------------------------------------------------------------------
# Optional LLM naming pass — readable names/descriptions on top of the deterministic ones.
# --------------------------------------------------------------------------------------
def _naming_prompt(spec: "SkillSpec") -> str:
    return (
        "Name a reusable agent skill from its observed tool procedure. "
        "Reply ONLY compact JSON: {\"name\": <=6 words, \"description\": one sentence}.\n"
        f"Tool sequence: {' -> '.join(spec.signature)}\n"
        f"Common request keywords: {', '.join(spec.triggers[:6])}\n"
        f"Example requests: {' | '.join(spec.examples[:3])}\n"
    )


def _parse_naming(raw: str) -> Json:
    m = re.search(r"\{.*\}", raw or "", re.DOTALL)
    if not m:
        return {}
    try:
        data = json.loads(m.group(0))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def name_skills_with_llm(specs: list["SkillSpec"], complete: Callable[[str], str], *, max_skills: Optional[int] = None) -> list["SkillSpec"]:
    """Give skills readable names/descriptions via an injected `complete(prompt)->text` fn.

    Model-agnostic: `complete` can wrap Ollama, Anthropic, or any endpoint; it should
    return raw text expected to contain a JSON object {name, description}. Any failure or
    unparseable reply leaves the deterministic name in place. Mutates specs in place.
    """
    for spec in specs[: max_skills if max_skills is not None else len(specs)]:
        try:
            data = _parse_naming(complete(_naming_prompt(spec)) or "")
        except Exception:  # a naming failure must never break discovery
            continue
        name = str(data.get("name") or "").strip()
        desc = str(data.get("description") or "").strip()
        if name:
            spec.name = name[:60]
            spec.slug = _slug(name)
            spec.content_hash = f"{stable_hash('|'.join(list(spec.signature) + spec.triggers + spec.allowed_tools)):x}"
        if desc:
            spec.description = desc[:240]
    return specs


# --------------------------------------------------------------------------------------
# Identity dedup — never re-capture a skill the agent already has locally.
# --------------------------------------------------------------------------------------
def _skill_tool_set(tools: Iterable[str]) -> frozenset:
    return frozenset(normalize_tool(t) for t in tools)


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def local_skill_identities_from_records(records: Iterable[Json]) -> tuple[list[str], list[list[str]]]:
    """Pull (names, tool-sets) of already-defined skills from ingested skill records."""
    names: list[str] = []
    tool_sets: list[list[str]] = []
    for r in records:
        if r.get("record_type") in {"skill_manifest", "skill_registry"}:
            names.append(str(r.get("name") or ""))
            tool_sets.append(list(r.get("allowed_tools") or []))
    return names, tool_sets


def dedup_discovered_vs_local(
    specs: list["SkillSpec"],
    *,
    local_names: Iterable[str] = (),
    local_tool_sets: Iterable[Iterable[str]] = (),
    tool_jaccard: float = 0.8,
) -> list["SkillSpec"]:
    """Drop discovered skills the agent already has locally (identity, not text, match).

    A discovered skill is dropped if its slug matches a local skill's slug, or its tool
    set is >= `tool_jaccard` similar to a local skill's tool set. This is the identity
    dedup that keeps augment-mode retrieval from re-returning locally-defined skills while
    still surfacing genuinely net-new discovered procedures.
    """
    local_slugs = {_slug(n) for n in local_names if n}
    local_sets = [_skill_tool_set(ts) for ts in local_tool_sets if ts]
    kept: list[SkillSpec] = []
    for spec in specs:
        if spec.slug in local_slugs:
            continue
        ts = _skill_tool_set(spec.allowed_tools)
        if any(_jaccard(ts, ls) >= tool_jaccard for ls in local_sets):
            continue
        kept.append(spec)
    return kept


def discover_capture_for_session(
    events: Iterable[InteractionEvent],
    *,
    scope: Json,
    ingest_records: Callable[[list[Json]], Any],
    local_records: Iterable[Json] = (),
    complete: Optional[Callable[[str], str]] = None,
    updated_at_ms: int = 0,
    min_support: int = MIN_SUPPORT,
    min_steps: int = MIN_STEPS,
    max_skills: int = MAX_SKILLS,
    deployment_scope: str = "user",
    sharing_scope: str = "user",
) -> Json:
    """End-to-end for the commit path: discover -> dedup-vs-local -> (LLM name) -> capture.

    `ingest_records(records)` persists the built skill records (manifest/registry/section)
    into the store. `complete` (optional) enables the LLM naming pass. Returns a summary.
    Designed to be safe to call fire-and-forget from idle/session commit: bounded work,
    never raises through to the caller.
    """
    try:
        specs = discover_skills(events, min_support=min_support, min_steps=min_steps, max_skills=max_skills)
        if not specs:
            return {"discovered": 0, "captured": 0, "reason": "no_recurring_procedures"}
        names, tool_sets = local_skill_identities_from_records(local_records)
        specs = dedup_discovered_vs_local(specs, local_names=names, local_tool_sets=tool_sets)
        if not specs:
            return {"discovered": 0, "captured": 0, "reason": "all_dup_of_local_skills"}
        if complete is not None:
            name_skills_with_llm(specs, complete)
        captured = 0
        for spec in specs:
            records = skill_records_for_spec(
                spec, scope=scope, deployment_scope=deployment_scope,
                sharing_scope=sharing_scope, updated_at_ms=updated_at_ms,
            )
            ingest_records(records)
            captured += 1
        return {"discovered": len(specs), "captured": captured,
                "skills": [{"name": s.name, "slug": s.slug, "support": s.support} for s in specs]}
    except Exception as exc:  # never break the commit path
        return {"discovered": 0, "captured": 0, "error": f"{type(exc).__name__}: {exc}"}


# --------------------------------------------------------------------------------------
# Local-history loader + CLI (reuses the backfill transcript parsing when available).
# --------------------------------------------------------------------------------------
_TOOL_MARKER = re.compile(r"\[tool:([^\]]+)\]\s*([^\[\n]*)")


def events_from_leaned_messages(messages: Iterable[Json], session_id: str) -> list[InteractionEvent]:
    """Build InteractionEvents from committed messages whose tool calls are leaned to
    `[tool:NAME] arg` text (the live ingestion format used by the hooks/backfill).

    User messages become episode triggers; each `[tool:...]` marker in an assistant
    message becomes an ordered tool action. This lets the session-commit path run skill
    discovery over exactly what was ingested, without needing the raw structured blocks.
    """
    out: list[InteractionEvent] = []
    ts = 0
    for m in messages:
        role = str(m.get("role") or "")
        content = m.get("content")
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            text = " ".join(b.get("text", "") for b in content if isinstance(b, dict) and isinstance(b.get("text"), str))
        else:
            text = ""
        ts += 1
        if role in ("user", "human") and text.strip():
            out.append(InteractionEvent(session_id=session_id, ts_ms=ts, role="user", text=text))
        for name, arg in _TOOL_MARKER.findall(text or ""):
            ts += 1
            out.append(InteractionEvent(
                session_id=session_id, ts_ms=ts, role="assistant", tool_name=name.strip(),
                tool_input={"command": arg.strip()} if arg.strip() else {},
            ))
    return out


def events_from_backfill_records(records: Iterable[Json]) -> list[InteractionEvent]:
    """Adapt normalized backfill records (dicts) into InteractionEvents."""
    out: list[InteractionEvent] = []
    for r in records:
        out.append(
            InteractionEvent(
                session_id=str(r.get("session_id") or r.get("session") or ""),
                ts_ms=int(r.get("ts_ms") or r.get("timestamp_ms") or 0),
                role=str(r.get("role") or ""),
                text=str(r.get("text") or ""),
                tool_name=str(r.get("tool_name") or ""),
                tool_input=r.get("tool_input") if isinstance(r.get("tool_input"), dict) else {},
                origin=str(r.get("origin") or ""),
            )
        )
    return out


def main() -> int:  # pragma: no cover - thin CLI wrapper
    import argparse

    ap = argparse.ArgumentParser(description="Discover reusable skills from agent interaction history (JSONL of events).")
    ap.add_argument("--events", required=True, help="JSONL file of interaction events (session_id, ts_ms, role, text, tool_name, tool_input)")
    ap.add_argument("--min-support", type=int, default=MIN_SUPPORT)
    ap.add_argument("--min-steps", type=int, default=MIN_STEPS)
    ap.add_argument("--max-skills", type=int, default=MAX_SKILLS)
    ap.add_argument("--emit-markdown", action="store_true", help="print each discovered skill as markdown")
    ap.add_argument("--emit-records", metavar="JSONL", default=None, help="write skill_manifest/registry/section records here")
    args = ap.parse_args()

    raw = [json.loads(l) for l in open(args.events, encoding="utf-8") if l.strip()]
    events = events_from_backfill_records(raw)
    specs = discover_skills(events, min_support=args.min_support, min_steps=args.min_steps, max_skills=args.max_skills)
    print(json.dumps({"episodes_scanned": len(mine_episodes(events)), "skills_discovered": len(specs),
                      "skills": [{"name": s.name, "slug": s.slug, "support": s.support,
                                  "sessions": len(s.sessions), "signature": list(s.signature)} for s in specs]}, indent=2))
    if args.emit_markdown:
        for s in specs:
            print("\n" + "=" * 70 + "\n" + s.render_markdown())
    if args.emit_records:
        with open(args.emit_records, "w", encoding="utf-8") as fh:
            for s in specs:
                for rec in skill_records_for_spec(s, scope={"user": "local", "project": "default"}, updated_at_ms=0):
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print("[out]", args.emit_records)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Operator-settable gateway configuration: the write side of ``/v1/admin/config``.

``matrixark_v1_gateway._model_config_snapshot`` already tells an operator WHICH extraction and
embedding endpoints a deployment talks to. It could only ever read ``os.environ``, so configuring a
deployment still meant shell access to the process environment -- which a customer running the
gateway as a managed service does not have. This module is the other half: a small, audited set of
settings a customer can write from the portal, persisted outside the process so a restart keeps
them.

Three things make this safe to expose:

* **A closed registry.** Only the settings in ``SETTINGS`` can be written. An unknown key is
  rejected, so this is not a general "set any environment variable" endpoint.
* **Secrets are write-only.** A secret's VALUE is stored (0600, owner-only) and pushed into the
  process environment, but it is never returned by any read: the snapshot reports whether the
  secret resolves, never what it is. That mirrors the existing read-side contract.
* **Honest apply semantics.** Some of these environment variables are read once at import time
  into module constants (``matrixark_mcp_core.EXTRACTION_LLM_BASE_URL`` and friends); writing them
  at runtime cannot change the value the extractor already captured. Every setting therefore
  carries ``applies``: ``live`` (read per call -- takes effect on the next request) or ``restart``
  (frozen at import -- persisted now, effective after a restart). Reporting that is the difference
  between a customer who knows to restart and one who thinks the change landed and is silently
  running the old endpoint.

Precedence at boot matches ``matrixark_load_config``:

    built-in default  <  runtime config file  <  explicit environment variable

so an operator who exports a variable keeps winning over what the portal stored. An explicit portal
WRITE is different: it is a deliberate action taken now, so it applies to the live process
environment unconditionally and the snapshot then reports the value's source as ``portal``.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

Json = Dict[str, Any]

# The process environment as it was when this module was first imported. Used to keep the boot
# precedence honest: a variable already present here was set by the operator's launcher and must not
# be overwritten by the stored file.
_BOOT_ENV: Dict[str, str] = dict(os.environ)

# What each `applies == "restart"` setting was actually running as, recorded by apply_boot() once
# the stored file has been folded into the environment. Those settings are read into module
# constants at import and never re-read, so this is the only moment their effective value is
# knowable -- and without it "has this been changed since startup?" cannot be answered at all.
#
# Empty means nobody called apply_boot (a test, a tool importing this module). Then nothing is
# reported as pending: missing evidence is not evidence of a pending change.
_BOOT_EFFECTIVE: Dict[str, str] = {}

_TRUTHY = {"1", "true", "yes", "on"}

# Floors the ENGINE applies, by variable. These are not a policy this file invents -- the accessors
# in storage_config.rs read `parse_*(...).max(1024)`, so a smaller value is silently raised before
# anything uses it. Documented in each setting's help as well; recorded here so a write can say so
# at the moment it happens, which is the only time the customer is looking.
#
# test_matrixark_clamped_settings_say_so derives the same floors from the Rust accessor and fails if
# this map and the engine disagree, so it cannot quietly go stale.
_ENGINE_MINIMUMS: Dict[str, int] = {
    "TS_CONTEXT_PAGE_TARGET_BYTES": 1024,
    "TS_BLOCK_SLAB_TARGET_BYTES": 1024,
    "TS_STORAGE_ZONE_SIZE": 1024,
    "TS_STREAM_MAX_BLOB_SIZE": 1024,
}


def engine_minimum(env_name: str) -> Optional[int]:
    """The floor the engine raises this variable to, if it has one."""
    return _ENGINE_MINIMUMS.get(env_name)

DEFAULT_CONFIG_FILENAME = "runtime_config.json"

# How many past writes the stored document keeps. Bounded because this file is read on every boot
# and on every admin read; an unbounded log would grow without anyone noticing until it did.
HISTORY_LIMIT = 50

# Longest previous/new value recorded in the log. A base URL or a model name fits; nothing here
# needs to hold a document, and an unbounded value would make one write able to bloat the file.
_HISTORY_VALUE_MAX = 200


class Setting:
    """One writable knob: its portal identity, the env var it drives, and when a write takes effect."""

    __slots__ = ("key", "group", "env", "label", "kind", "default", "applies", "help", "choices")

    def __init__(self, key: str, group: str, env: str, label: str, kind: str, default: str,
                 applies: str, help: str, choices: Optional[List[str]] = None) -> None:
        self.key = key
        self.group = group
        self.env = env
        self.label = label
        self.kind = kind          # str | int | float | bool | secret
        self.default = default
        self.applies = applies    # live | restart
        self.help = help
        self.choices = choices or []

    @property
    def secret(self) -> bool:
        return self.kind == "secret"

    def describe(self) -> Json:
        return {
            "key": self.key, "group": self.group, "env": self.env, "label": self.label,
            "kind": self.kind, "default": self.default, "applies": self.applies,
            "help": self.help, "choices": list(self.choices), "secret": self.secret,
        }


# The settings a deployment does not work properly without. Everything else is a tuning knob with
# a defensible default; these have no default that is right in production, because the right value
# is a decision about YOUR deployment -- which model, whose key, where the documents are. A portal
# that presents all 79 as equals answers "what can I change" and leaves "what must I set"
# unanswered, and the second is the only question a customer setting up actually has.
ESSENTIAL_KEYS = {
    "extraction.provider",
    "extraction.base_url",
    "extraction.model",
    "extraction.api_key_env",
    "extraction.api_key",
    "embedding.provider",
    "embedding.api_base",
    "embedding.model",
    "embedding.api_key_env",
    "embedding.api_key",
    "embedding.require_model_embeddings",
    "ingestion.root",
}


# Groups, in the order the portal renders them. `advanced` groups start collapsed: a customer
# setting up a deployment needs the models and nothing else, and a 70-field wall of tuning knobs
# above the fold is how the two fields that matter get missed.
GROUPS: Dict[str, Json] = {
    "extraction": {
        "title": "Extraction model", "order": 10, "advanced": False,
        "note": "The LLM that turns ingested text into memories, and the one that writes the node "
                 "summaries retrieval walks -- both on the endpoint and key below. DeepSeek, "
                 "vLLM, Ollama and OpenAI "
                "all speak the same OpenAI-compatible shape.",
    },
    "embedding": {
        "title": "Embedding model", "order": 20, "advanced": False,
        "note": "The encoder that makes retrieval semantic. DeepSeek has no embeddings API, so a "
                "DeepSeek deployment pairs it with a local encoder.",
    },
    "retrieval": {
        "title": "Retrieval and context budget", "order": 30, "advanced": False,
        "note": "How much context a retrieve may return and how widely it looks for it. The token "
                "budget is usually the limit that actually binds; the candidate ceilings are "
                "backstops.",
    },
    "skills": {
        "title": "Skills and resources", "order": 40, "advanced": False,
        "note": "How much of a context pack skills may occupy, and how much of one skill comes "
                "back at a time.",
    },
    "ingestion": {
        "title": "Ingestion pipeline", "order": 50, "advanced": True,
        "note": "What happens to a document after it is accepted: summaries, time compression, and "
                "the background encoder drainer.",
    },
    "storage_engine": {
        "title": "Storage engine", "order": 55, "advanced": True,
        "note": "How the engine stores what it has already accepted: vector width, the memory it "
                "returns to the operating system, and the caps that stop diagnostics growing "
                "without bound. The engine runs in this process, so these take effect on the next "
                "write or read rather than at the next restart. Every default here is the engine's "
                "own, checked against its source by a test.",
    },
    "limits": {
        "title": "Rate limits and request quotas", "order": 60, "advanced": True,
        "note": "The edge's own ceilings. All of these are read when the gateway process starts, "
                "so a change here needs a restart.",
    },
    "audit": {
        "title": "Audit trail", "order": 65, "advanced": False,
        "note": "Off by default, and off means discarded: the access layer writes a record every "
                "time a key is used to manage another key or is refused, and drops it here. Audit "
                "records live in the main record log, so a busy deployment with this on grows the "
                "store without bound -- which is why the default is what it is, and why the "
                "choice belongs somewhere a customer can see it.",
    },
    "behaviour": {
        "title": "Retrieval behaviour", "order": 70, "advanced": True,
        "note": "Deployment-wide defaults. A tenant policy -- or, for the ones marked, one "
                "user's own setting -- overrides any of these; what you set here is the fallback. "
                "Set those on the retrieval settings above.",
    },
}


# `applies` below is not a guess: it is what a reader-site audit of tools/ shows. The extraction
# endpoint/model/timeout/max-tokens are captured into module constants at import
# (matrixark_mcp_core lines 269-273, matrixark_mcp_extraction_provider lines 27-32), so a write is
# persisted but only effective after a restart. The extraction API KEY is different -- the request
# path does `os.environ.get(EXTRACTION_LLM_API_KEY_ENV)` per call, so a key written here is live on
# the very next extraction. Every embedding setting is read per call in matrixark_mcp_embeddings.
# What changing an encoder costs a store that already holds documents, said once and shared by the
# three controls that change one. Embeddings are keyed by a model-specific ref and
# `embedding_conflicts` declines a stored vector whose model differs from the active one, so the
# vectors already in the store stop being usable the moment the encoder changes. The engine's own
# MIGRATION note calls the result "a quiet degradation rather than an error" -- which is exactly why
# it belongs beside the control rather than in a comment somebody has to go and find.
# The gateway re-reads both encoder fields on the next call, so `live` is true of it. A co-located
# encoder server does not: `context_minilm_embed_server` binds MATRIXARK_EMBEDDING_MODEL and
# MATRIXARK_EMBEDDING_MODEL_PATH at ITS import, and keeps whatever it started with. Said once and
# shared by both controls, because saying it twice is how one of them came to say it and the other
# not to.
ENCODER_SERVER_NOTE = (
    " The gateway picks this up on the next call, but a co-located encoder server reads it at ITS"
    " startup — restart that too if you are running one."
)


ENCODER_CHANGE_NOTE = (
    " Changing this on a store that already holds documents leaves every vector in it made by the "
    "previous encoder: they are declined as a different model, so retrieval falls back to lexical "
    "and recency until context_embed_backfill re-encodes them."
)


SETTINGS: List[Setting] = [
    # ---- extraction (the LLM that turns raw text into memories) --------------------------------
    Setting("extraction.provider", "extraction", "MATRIXARK_UNDERSTANDING_PROVIDER",
            "Extraction provider", "str", "deterministic", "restart",
            "deterministic runs local rules and calls no model at all. openai_compatible calls "
            "the endpoint below — this is the setting for DeepSeek, vLLM, Ollama, or OpenAI. "
            "anthropic calls the Messages API and reads Anthropic model and Anthropic base URL "
            "instead of the two fields below.",
            ["deterministic", "openai_compatible", "anthropic"]),
    Setting("extraction.base_url", "extraction", "MATRIXARK_EXTRACTION_BASE_URL",
            "Extraction base URL", "str", "", "restart",
            "OpenAI-compatible base, including /v1. DeepSeek: https://api.deepseek.com/v1. "
            "anthropic ignores this and uses Anthropic base URL below."),
    # No fixed variable: `_env_name` sends this to whichever one the selected provider reads,
    # the same way the API key is routed. An empty `env` is how the registry says "dynamic", and it
    # is what keeps the declared-default sweep from comparing one blank against two fallbacks.
    Setting("extraction.model", "extraction", "",
            "Extraction model", "str", "", "restart",
            "The model extraction calls, sent to whichever provider is selected above -- Anthropic "
            "reads its own variable and this field follows, so there is nothing to keep in step by "
            "hand. Blank means the provider's own default: claude-sonnet-5 on Anthropic, "
            "qwen2.5:1.5b on an OpenAI-compatible endpoint."),
    # anthropic does not read the BASE URL above: its endpoint has its own variable and no
    # fallback, so without this a customer who chose anthropic could fill in a base URL and watch a
    # different endpoint be called. The MODEL is no longer repeated here -- Extraction model above
    # writes to whichever variable the selected provider reads -- because two fields for one value
    # meant one of them was always inert, and which one depended on a dropdown three rows up.
    # Max tokens and timeout are not repeated here because
    # those DO fall back to the extraction controls.
    Setting("extraction.anthropic_base_url", "extraction", "MATRIXARK_ANTHROPIC_API_BASE",
            "Anthropic base URL", "str", "https://api.anthropic.com", "restart",
            "Used when Extraction provider is anthropic, which does not read Extraction base URL. "
            "The Messages API path is added to this, so give the host without /v1."),
    Setting("extraction.api_key_env", "extraction", "MATRIXARK_EXTRACTION_API_KEY_ENV",
            "Extraction key variable", "str", "", "restart",
            "Name of the environment variable holding the extraction key -- the key you enter below "
            "is written into it. Leave it empty and it follows the provider: ANTHROPIC_API_KEY for "
            "Anthropic, OPENAI_API_KEY otherwise. Set it only to point at a variable your own "
            "launcher already fills, such as DEEPSEEK_API_KEY."),
    Setting("extraction.api_key", "extraction", "", "Extraction API key", "secret", "", "live",
            "Stored owner-only and never returned by any read. Written into the variable named "
            "above, which the extraction request reads on every call — so a new key is live "
            "immediately, with no restart."),
    Setting("extraction.timeout_sec", "extraction", "MATRIXARK_EXTRACTION_TIMEOUT_SEC",
            "Extraction timeout (s)", "float", "30", "restart",
            "A slow endpoint that exceeds this falls back to the deterministic path "
            "silently. The Anthropic path prefers MATRIXARK_ANTHROPIC_TIMEOUT_SEC where "
            "that is set and falls back to this."),
    Setting("extraction.max_tokens", "extraction", "MATRIXARK_EXTRACTION_MAX_TOKENS",
            "Extraction max tokens", "int", "1200", "restart",
            "Completion cap per extraction call. The Anthropic path prefers "
            "MATRIXARK_ANTHROPIC_MAX_TOKENS where that is set and falls back to this, so "
            "a value there overrides this field for that provider only."),

    # ---- the summary model ----------------------------------------------------------------------
    # The third model a deployment runs, and the one called most: every context node gets a summary,
    # where extraction runs once per ingest. It needs no endpoint or key of its own --
    # openai_compatible_json_call sends a summary to EXTRACTION_LLM_BASE_URL with the key named by
    # MATRIXARK_EXTRACTION_API_KEY_ENV, and takes only the model and the budget as arguments -- so
    # these three sit in the extraction group beside the endpoint and key they use.
    #
    # `restart` on all three, and for the same reason audit.mode carries it: SUMMARY_LLM_MODEL and
    # SUMMARY_LLM_MAX_TOKENS are module constants in two modules, bound at import. The provider is
    # the mixed one -- summary_provider() re-reads the environment on every call, so the CHOICE
    # moves at once, while the model those constants carry does not. `restart` is the only label
    # true of all three, and the help says which half moves first.
    Setting("summary.provider", "extraction", "MATRIXARK_SUMMARY_PROVIDER",
            "Summary provider", "str", "", "restart",
            "Which provider writes the node summaries retrieval walks. Blank follows the extraction "
            "provider above, which is the default. openai_compatible is the only value that calls a "
            "model: anthropic returns rule-written summaries and no error, so a deployment on "
            "anthropic extraction gets deterministic summaries unless it names openai_compatible "
            "here. The choice itself takes effect on the next summary; the model below waits for a "
            "restart.",
            ["", "deterministic", "openai_compatible"]),
    # summary.model was here. Node summaries are made by the extraction endpoint with the
    # extraction key, so a separate model was a second name for the same call -- and the two could
    # be set to models the same endpoint does not both serve. The summary uses the extraction model,
    # and MATRIXARK_SUMMARY_MODEL is no longer read; `_model_config_snapshot` says so if a launcher
    # has one set, because a variable that silently stopped mattering is worse than one that never
    # existed.
    Setting("summary.max_tokens", "extraction", "MATRIXARK_SUMMARY_MAX_TOKENS",
            "Summary max tokens", "int", "900", "restart",
            "Completion cap per summary call, separate from the extraction cap because a summary is "
            "the shorter of the two and is paid for on every node."),

    # ---- audit trail ---------------------------------------------------------------------------
    # Two readers, and they disagree about when a change lands. matrixark_access reads os.environ
    # inside _append_audit_record, so the records IT writes -- key management, and every refusal --
    # start or stop on the next call. matrixark_mcp_server captures the value into
    # self._audit_mode_default in its constructor, so the server's own auditing keeps whatever it
    # was built with. `restart` is the only label true of both, and the help says which half moves
    # first rather than leaving a customer to find out by not finding records.
    Setting("audit.mode", "audit", "MATRIXARK_AUDIT_MODE",
            "Audit records", "str", "off", "restart",
            "off discards every audit record, including the one written each time a key is refused "
            "-- nothing is kept and nothing says so. async writes them off the request path; full "
            "and sync make each one durable before the call returns, at the cost of that write. "
            "Records go into the main record log, so a read-heavy deployment with this on grows "
            "without bound. Setting it starts the access layer's records at once; the server's own "
            "auditing is fixed when the process starts, so restart to have all of it.",
            ["off", "async", "full", "sync"]),

    # ---- embedding (the encoder that makes retrieval semantic) ---------------------------------
    Setting("embedding.provider", "embedding", "MATRIXARK_EMBEDDING_PROVIDER",
            "Embedding provider", "str", "deterministic", "live",
            "deterministic produces hash vectors — retrieval still answers 200, but it is not "
            "semantic. openai_compatible calls the encoder below. voyage calls Voyage with the same "
            "three fields, defaulting to api.voyageai.com, VOYAGE_API_KEY and voyage-3 when they "
            "are blank. local is a synonym for deterministic and runs NO encoder — to run one on "
            "this box, choose openai_compatible and point it at the bundled encoder server, which "
            "is what the Local MiniLM preset does." + ENCODER_CHANGE_NOTE,
            ["deterministic", "openai_compatible", "voyage", "local"]),
    Setting("embedding.api_base", "embedding", "MATRIXARK_EMBEDDING_API_BASE",
            "Embedding base URL", "str", "", "live",
            "Must end in /v1: the request is built as <base>/embeddings."),
    Setting("embedding.model", "embedding", "MATRIXARK_EMBEDDING_MODEL",
            "Embedding model", "str", "", "live",
            "The encoder to use: a model name like paraphrase-multilingual-MiniLM-L12-v2, or "
            "a path to one you have downloaded -- an in-process encoder loads either, and a hosted "
            "provider is sent the name."
            + ENCODER_SERVER_NOTE + ENCODER_CHANGE_NOTE),
    # MATRIXARK_EMBEDDING_MODEL_PATH was offered here as a second encoder field. It is not a
    # second choice: the encoder reads `MODEL_PATH or MODEL`, so it only ever overrode the field
    # above -- and only on the in-process path, because a hosted provider is sent the model NAME and
    # never looks at the path at all. Two fields for one value, one of which silently won on one
    # path and did nothing on the other. The variable is still honoured where a launcher sets it,
    # and `_model_config_snapshot` says so, because a value in force that no screen shows is the
    # thing this whole panel exists to prevent.
    Setting("embedding.api_key_env", "embedding", "MATRIXARK_EMBEDDING_API_KEY_ENV",
            "Embedding key variable", "str", "", "live",
            "Name of the environment variable holding the encoder key -- the key you enter below is "
            "written into it. Leave it empty and it follows the provider: VOYAGE_API_KEY for Voyage, "
            "OPENAI_API_KEY otherwise. Set it only to point at a variable your own launcher already "
            "fills."),
    Setting("embedding.api_key", "embedding", "", "Embedding API key", "secret", "", "live",
            "Stored owner-only and never returned. A LOCAL encoder still needs a non-empty value "
            "here — the call is skipped before it is attempted when the variable is empty."),
    Setting("embedding.text_max_tokens", "embedding", "MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS",
            "Embedding text window (tokens)", "int", "128", "restart",
            "Must match what the encoder can actually take; a larger window means fewer chunks per "
            "document."),
    # The encoder call's counterpart to extraction.timeout_sec. Read inside the request function
    # rather than captured at import, so a change takes effect on the next call -- which is why this
    # one is live where the extraction timeout is not.
    Setting("embedding.timeout_sec", "embedding", "MATRIXARK_EMBEDDING_API_TIMEOUT_S",
            "Embedding timeout (s)", "float", "30", "live",
            "How long one encoder call may take before it is abandoned. A local encoder on a cold "
            "start, or a large batch, can need more than the default 30. Exceeding it is treated "
            "as an unreachable encoder: hash vectors unless Fail instead of falling back is on."),
    Setting("embedding.require_model_embeddings", "embedding", "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS",
            "Fail instead of falling back", "bool", "0", "live",
            "On, an unreachable encoder fails the request. Off, it silently degrades to hash "
            "vectors and the deployment looks healthy."),

    # ---- skill/resource retrieval budgets ------------------------------------------------------
    # The four caps below are the backstop on each section of a pack. They are NOT the way to size
    # a section -- the ratio beside each one is -- and they were doing that job by accident: at the
    # 500,000-token default they cut skills from 10% to 1.6% and shared resources from 25% to 3.3%,
    # so a customer who set a percentage got a sixth of it and nothing said so. Raised to sit above
    # what each ratio yields at the default total, which is asserted rather than eyeballed, and
    # offered here so the backstop is a decision rather than a surprise.
    Setting("skills.shared_skill_max_budget_tokens", "skills",
            "MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS",
            "Skill budget ceiling (tokens)", "int", "262144", "live",
            "A hard ceiling on the skill section, whatever the percentage works out to. It exists "
            "for an unusually large request, not to size the section: leave it above what your "
            "percentage yields, or the percentage stops meaning anything."),
    Setting("skills.shared_resource_max_budget_tokens", "skills",
            "MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS",
            "Resource budget ceiling (tokens)", "int", "262144", "live",
            "The same backstop for shared resource chunks. Above what the resource percentage "
            "yields at your context size, so the percentage is what decides."),
    Setting("retrieval.cross_session_max_budget_tokens", "retrieval",
            "MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS",
            "Cross-session ceiling (tokens)", "int", "262144", "live",
            "A hard ceiling on what other sessions may contribute, whatever the cross-session "
            "percentage works out to."),
    Setting("retrieval.cross_session_profile_max_budget_tokens", "retrieval",
            "MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS",
            "Profile ceiling (tokens)", "int", "327680", "live",
            "The same backstop for the durable profile -- the aggregation that carries memory into "
            "a fresh session, and the section most likely to want room."),
    Setting("skills.shared_skill_budget_ratio", "skills", "MATRIXARK_SHARED_SKILL_BUDGET_RATIO",
            "Skill share of the context budget", "float", "0.10", "live",
            "Fraction of a context pack reserved for skill sections. Raise it and the next pack "
            "carries more skill content, up to the guard below."),
    Setting("skills.shared_skill_max_budget_ratio", "skills",
            "MATRIXARK_SHARED_SKILL_MAX_BUDGET_RATIO",
            "Skill share guard", "float", "0.50", "live",
            "The most of a pack skills may take, whatever the share above asks for. It stops one "
            "section crowding out the rest and is not the place to set the share. It used to sit "
            "at exactly the share's own default, which is why raising the share did nothing."),
    Setting("skills.chunks_per_skill", "skills", "MATRIXARK_SKILL_CHUNKS_PER_SKILL",
            "Chunks per skill", "int", "3", "live",
            "How many sections of one skill a pack may carry."),
    Setting("skills.reserved_refs", "skills", "MATRIXARK_SKILL_RESERVED_REFS",
            "Reserved skill references", "int", "3", "live",
            "Skill references held back for the pack even under budget pressure."),
    Setting("skills.description_always", "skills", "MATRIXARK_SKILL_DESCRIPTION_ALWAYS",
            "Always list skill descriptions", "bool", "1", "live",
            "Include every visible skill's name and description in each pack even when no section "
            "matches. A description is 5-30 tokens, so the model learns a skill EXISTS cheaply and "
            "its content is fetched only when a section actually matches. This is what makes "
            "hundreds of skills affordable."),
    Setting("skills.discovery", "skills", "MATRIXARK_SKILL_DISCOVERY",
            "Skill discovery", "bool", "0", "restart",
            "Let a retrieve surface skills the query did not name, from the skill index rather "
            "than only from the current scope."),
    Setting("skills.shared_resource_budget_ratio", "skills",
            "MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO",
            "Resource share of the context budget", "float", "0.25", "live",
            "Fraction of a pack reserved for shared resource chunks, separately from skills."),
    Setting("skills.shared_resource_max_budget_ratio", "skills",
            "MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_RATIO",
            "Resource share guard", "float", "0.50", "live",
            "The same guard for resources: the most of a pack they may take however high the "
            "share above is set."),

    # ---- retrieval and context budget ----------------------------------------------------------
    Setting("retrieval.timeout_ms", "retrieval", "MATRIXARK_RETRIEVAL_TIMEOUT_MS",
            "Retrieval deadline (ms)", "int", "0", "live",
            "How long a retrieve may keep working before it returns what it has. 0 means no "
            "deadline. It is COOPERATIVE: it is checked between stages and every 64 or 128 "
            "candidates, never around a call to the store -- so it bounds the work a retrieve "
            "does, and cannot bound one slow backend call. The transport timeout below is what "
            "does that, and the pack panel reports which of the two is the smaller."),
    Setting("limits.transport_request_timeout_ms", "limits",
            "MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS",
            "Transport request timeout (ms)", "int", "60000", "live",
            "The per-request timeout on the call to the store. This is the one that decides how "
            "long a single slow call can take, whatever the deadline above says. It answered "
            "20000 in the adapter and 60000 in the agent hooks for the same variable until they "
            "were given one number."),
    Setting("limits.transport_io_timeout_ms", "limits",
            "MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS",
            "Transport I/O timeout (ms)", "int", "60000", "live",
            "The I/O timeout underneath the request timeout above. Same story, same fix."),
    # ---- offered by the config file since it existed; the portal caught up ----------------------
    Setting("limits.backend_readiness_timeout_ms", "limits",
            "MATRIXARK_BACKEND_READINESS_TIMEOUT_MS",
            "Backend readiness timeout (ms)", "int", "30000", "restart",
            "How long to keep waiting for the store to answer a readiness probe before giving up "
            "on it. A cold store with a large log takes tens of seconds to load, so a timeout "
            "below that turns a slow start into a failed one."),
    Setting("limits.backend_readiness_backoff_ms", "limits",
            "MATRIXARK_BACKEND_READINESS_BACKOFF_MS",
            "Backend readiness retry gap (ms)", "int", "200", "restart",
            "How long to wait between readiness probes while the store is still coming up."),
    Setting("limits.direct_record_bundle_max_bytes", "limits",
            "MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES",
            "Record bundle ceiling (bytes)", "int", "65536", "restart",
            "The largest bundle of records fetched in one round trip. Bigger bundles mean fewer "
            "round trips and more memory held per request."),
    Setting("limits.direct_record_hot_cache_max_records", "limits",
            "MATRIXARK_DIRECT_RECORD_HOT_CACHE_MAX_RECORDS",
            "Hot record cache (records)", "int", "20000", "restart",
            "How many records the read path keeps hot. This is resident memory traded for read "
            "latency, and the footprint panel is where you can see what it costs."),
    Setting("retrieval.cross_session_max_candidates", "retrieval",
            "MATRIXARK_CROSS_SESSION_MAX_CANDIDATES",
            "Cross-session candidates", "int", "24", "restart",
            "How many candidates a retrieve may draw from sessions other than the current one "
            "before ranking them. Each one costs a read."),
    Setting("retrieval.cross_session_min_score", "retrieval",
            "MATRIXARK_CROSS_SESSION_MIN_SCORE",
            "Cross-session score floor", "float", "0.20", "restart",
            "The score a candidate from another session must reach to be considered at all. "
            "Raising it makes long-term memory pickier rather than smaller."),
    Setting("retrieval.cross_session_parallelism", "retrieval",
            "MATRIXARK_CROSS_SESSION_PARALLELISM",
            "Cross-session parallelism", "int", "4", "restart",
            "How many other sessions are searched at once. Raising it shortens a retrieve and "
            "raises the load one retrieve puts on the store."),
    Setting("retrieval.cross_session_profile_max_sessions", "retrieval",
            "MATRIXARK_CROSS_SESSION_PROFILE_MAX_SESSIONS",
            "Profile sessions searched", "int", "6", "restart",
            "How many past sessions a profile query reaches into. The durable profile is meant to "
            "answer most of these without a scan, so this is the fallback breadth."),
    Setting("retrieval.max_children_scored_per_parent", "retrieval",
            "MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT",
            "Children scored per parent", "int", "100000", "restart",
            "How many children of one node a traversal will score. Scoring a child costs a page "
            "read, so this is the ceiling on what one branch can cost."),
    Setting("retrieval.shared_context_min_score", "retrieval",
            "MATRIXARK_SHARED_CONTEXT_MIN_SCORE",
            "Shared content score floor", "float", "0.20", "restart",
            "The score a shared skill or resource chunk must reach before it may take part of the "
            "pack. The share settings decide how much room it has; this decides whether it "
            "deserves any."),
    Setting("retrieval.mode_dependent_quota", "retrieval",
            "MATRIXARK_MODE_DEPENDENT_QUOTA",
            "Mode-dependent quota", "bool", "0", "restart",
            "Off, every caller gets the same cross-session share. On, the share follows the "
            "context source mode: a caller that already has the current session in its own prompt "
            "gets the budget spent on long-term memory instead. Shipped off pending a measured "
            "comparison."),
    Setting("extraction.entity_merge_operator", "extraction",
            "MATRIXARK_ENTITY_MERGE_OPERATOR",
            "Entity merge operator", "str", "EUA_MERGE", "restart",
            "Which operator decides that two extracted entities are the same one. EUA_MERGE "
            "applies field patches deterministically, in the serving path, with no model call. "
            "LLM_MERGE asks the extraction model instead -- and it needs the setting below turned "
            "on as well: with that off, choosing LLM_MERGE silently gets you EUA_MERGE, which is "
            "why the two are offered together. Confirmations and corrections always use LATEST "
            "whatever is set here, so it is not offered as a choice.",
            ("EUA_MERGE", "LLM_MERGE")),
    Setting("extraction.enable_llm_merge_operator", "extraction",
            "MATRIXARK_ENABLE_LLM_MERGE_OPERATOR",
            "Ask the model to merge entities", "bool", "0", "restart",
            "Off, entities are merged by the deterministic operator above. On, the extraction "
            "model is asked as well, which costs a call per ambiguous pair."),
    Setting("extraction.time_compression_min_events", "extraction",
            "MATRIXARK_TIME_COMPRESSION_MIN_EVENTS",
            "Events before compressing a window", "int", "8", "restart",
            "How many events must gather in a window before they are compressed into a summary. "
            "Lower compresses sooner and loses raw detail sooner."),
    Setting("extraction.time_compression_max_raw_events_per_node", "extraction",
            "MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE",
            "Raw events kept per node", "int", "256", "restart",
            "The ceiling on raw events one node keeps before compression is forced."),
    # ---- three audit modes, three vocabularies ---------------------------------------------------
    # `audit.mode` above governs the ACCESS layer. These two govern different layers and take
    # different words, which is the sharp edge: `sync` and `full` appear in more than one of them
    # meaning different things, and a value borrowed from the wrong one is rejected outright.
    Setting("retrieval.context_audit_mode", "retrieval",
            "MATRIXARK_CONTEXT_AUDIT_MODE",
            "Retrieval audit mode", "str", "telemetry_only", "live",
            "Whether a retrieve records what it did. off keeps nothing; telemetry_only keeps the "
            "shape of the answer; full keeps the evidence, which is what makes a pack "
            "reconstructable afterwards and what makes the record log grow. "
            "This is NOT the access-layer audit above and does not take its words: async and "
            "sync are refused here. "
            "With nothing set the answer depends on which path served the retrieve -- the request "
            "path and the direct read use telemetry_only, the local adapter uses off -- so "
            "setting this is how a deployment makes them agree.",
            ("off", "telemetry_only", "full")),
    Setting("retrieval.context_audit_sample_rate", "retrieval",
            "MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE",
            "Retrieval audit sample rate", "float", "0.01", "live",
            "What fraction of retrieves the mode above applies to. Selecting full raises this to "
            "1.0 for the retrieves it keeps evidence for, so a deployment that wants full "
            "auditing on a sample sets both."),
    Setting("limits.direct_audit_mode", "limits",
            "MATRIXARK_DIRECT_AUDIT_MODE",
            "Store audit write mode", "str", "buffered", "restart",
            "How audit records reach the store. buffered writes them off the request path; sync "
            "makes each durable before the call returns; drop discards them and logs that it did. "
            "A third vocabulary: off and full are not words this one knows.",
            ("buffered", "sync", "drop")),
    Setting("retrieval.hook_max_context_tokens", "retrieval",
            "MATRIXARK_HOOK_MAX_CONTEXT_TOKENS",
            "Agent hook context budget (tokens)", "int", "128000", "live",
            "The budget the agent hooks ask for. They have never used the setting below, and the "
            "installation manual sets this to 10000 -- so on a deployment installed by the book "
            "the agent path runs a budget fifty times smaller than the one the panels quoted. "
            "Every share on the budget panel is a percentage of whichever of the two the caller "
            "uses."),
    Setting("retrieval.default_max_context_tokens", "retrieval",
            "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS",
            "Default context budget (tokens)", "int", "500000", "restart",
            "Budget applied when a caller does not name one. Callers can ask for less per "
            "request; this is the ceiling they inherit. The agent hooks are not such a "
            "caller: they prefer MATRIXARK_HOOK_MAX_CONTEXT_TOKENS where it is set, which "
            "the installation manual sets to 10000, and matrixark_codex_dual_hook.sh "
            "passes its own 10000 without consulting this at all. Changing this does not "
            "change what a hook sends."),
    Setting("retrieval.gateway_default_max_context_tokens", "retrieval",
            "MATRIXARK_GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS",
            "Gateway default context budget", "int", "", "restart",
            "Overrides the budget above for requests arriving through this gateway, leaving direct "
            "backend callers on the wider default."),
    Setting("retrieval.context_source_mode", "retrieval", "MATRIXARK_CONTEXT_SOURCE_MODE",
            "Context source mode", "str", "auto", "restart",
            "local_and_remote assumes the agent already has the current session in its own prompt, "
            "so the budget goes to cross-session and profile memory. remote_only assumes it does "
            "not and returns the session too. auto decides per request.",
            ["auto", "local_and_remote", "remote_only"]),
    Setting("retrieval.min_score", "retrieval", "MATRIXARK_RETRIEVAL_MIN_SCORE",
            "Minimum similarity score", "float", "0.20", "restart",
            "Candidates scoring below this are dropped before packing. Raising it returns less but "
            "more relevant context; lowering it fills the budget with weaker matches."),
    Setting("retrieval.budget_fill_policy", "retrieval", "MATRIXARK_BUDGET_FILL_POLICY",
            "Budget fill policy", "str", "quality_first", "restart",
            "quality_first leaves the budget underfilled rather than packing weak candidates. "
            "force_fill uses the whole budget even when nothing scored well.",
            ["quality_first", "force_fill"]),
    Setting("retrieval.near_duplicate_overlap_threshold", "retrieval",
            "MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD",
            "Near-duplicate threshold", "float", "0.85", "restart",
            "A candidate this similar to an already-selected, higher-ranked one is dropped. Stops "
            "a pack paying twice for one fact."),
    Setting("retrieval.cross_session_budget_ratio", "retrieval",
            "MATRIXARK_CROSS_SESSION_BUDGET_RATIO",
            "Cross-session share of the budget", "float", "0.12", "live",
            "Fraction of a pack that may come from sessions other than the current one. This is "
            "what carries long-term memory into a fresh session."),
    Setting("retrieval.cross_session_max_budget_ratio", "retrieval",
            "MATRIXARK_CROSS_SESSION_MAX_BUDGET_RATIO",
            "Cross-session share guard", "float", "0.50", "live",
            "The most of a pack sessions other than the current one may take, whatever the share "
            "above asks for."),
    Setting("retrieval.cross_session_profile_budget_ratio", "retrieval",
            "MATRIXARK_CROSS_SESSION_PROFILE_BUDGET_RATIO",
            "Profile share of the budget", "float", "0.30", "live",
            "The cross-session share used instead of the one above when a query is asking about "
            "the durable profile rather than about recent work. Profile queries are the case "
            "where long-term memory IS the answer, so the share is larger."),
    Setting("retrieval.cross_session_profile_max_budget_ratio", "retrieval",
            "MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO",
            "Profile share guard", "float", "0.60", "live",
            "The guard on the profile share. Higher than the others because on a profile query "
            "that section is what the caller came for."),
    Setting("retrieval.cross_session_max_sessions", "retrieval",
            "MATRIXARK_CROSS_SESSION_MAX_SESSIONS",
            "Cross-session session ceiling", "int", "", "restart",
            "How many past sessions a single retrieve may draw from."),
    Setting("retrieval.query_rewrite", "retrieval", "MATRIXARK_QUERY_REWRITE",
            "Query rewriting", "bool", "0", "restart",
            "Expand the caller's query before matching. Costs a model call per retrieve; helps "
            "when queries are terse."),
    Setting("retrieval.pack_precision_expand", "retrieval", "MATRIXARK_PACK_PRECISION_EXPAND",
            "Precision expansion", "bool", "0", "restart",
            "After selecting refs, pull in adjacent content so a selected fragment arrives with "
            "the sentences around it."),

    # ---- ingestion pipeline --------------------------------------------------------------------
    Setting("ingestion.root", "ingestion", "MATRIXARK_INGESTION_ROOT",
            "Ingestion root directory", "str", "", "live",
            "Every path the ingestion page accepts is resolved inside this directory. With it "
            "unset, submitting server-side paths is refused outright rather than defaulting to the "
            "whole filesystem — so bulk import does not work until you set it."),
    Setting("ingestion.bulk_ingest", "ingestion", "MATRIXARK_BULK_INGEST",
            "Bulk ingest mode", "bool", "0", "restart",
            "Trades per-record durability acknowledgement for throughput during a large import. "
            "Turn it off again for steady-state serving."),
    Setting("ingestion.resource_async_default_bytes", "ingestion",
            "MATRIXARK_RESOURCE_ASYNC_DEFAULT_BYTES",
            "Async parse threshold (bytes)", "int", "2097152", "restart",
            "A resource larger than this is parsed in the background instead of inline, so a big "
            "document does not hold the ingest call open."),
    Setting("ingestion.summary_refresh_interval_ms", "ingestion",
            "MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS",
            "Summary refresh interval (ms)", "int", "1000", "restart",
            "How often the background refresher rebuilds node summaries. Longer means summaries "
            "lag further behind new events; shorter costs more model calls."),
    Setting("ingestion.summary_refresh_limit", "ingestion", "MATRIXARK_SUMMARY_REFRESH_LIMIT",
            "Summaries refreshed per pass", "int", "64", "restart",
            "Ceiling on how many nodes one refresh pass touches."),
    # ---- storage engine -------------------------------------------------------------------
    # These are TS_* knobs the engine reads directly. Seven of eighty, chosen rather than exported:
    # directories, bind addresses, cluster identity and the metaserver admin token are set by
    # whoever provisions a node, and knobs with no documented default have nothing truthful to show
    # in a form. Every default below is the engine's own -- see
    # test_matrixark_engine_settings_match_the_engine.
    # ---- storage engine: page and slab geometry -------------------------------------------
    # These are read through StorageTuningConfig::from_getter, which passes a name constant to a
    # closure -- so they never appear beside env::var and a name-literal scan of the engine does
    # not see them at all. Eighteen knobs hide that way; these nine are the tuning family.
    Setting("storage_engine.context_page_target_bytes", "storage_engine",
            "TS_CONTEXT_PAGE_TARGET_BYTES",
            "Target size of a context page", "int", "65536", "live",
            "How large a page grows before the engine starts another. Smaller pages read less per "
            "lookup and cost more of them; larger pages amortise better and waste more on a partial "
            "read. Values below 1 KiB are raised to it."),
    Setting("storage_engine.block_slab_target_bytes", "storage_engine",
            "TS_BLOCK_SLAB_TARGET_BYTES",
            "Target size of a block segment", "int", "1073741824", "live",
            "The unit page-slab reclaim works in. A slab is retained while any loaded shard still "
            "references it, so larger slabs mean fewer, coarser reclaims. Values below 1 KiB are "
            "raised to it."),
    Setting("storage_engine.storage_zone_size", "storage_engine",
            "TS_STORAGE_ZONE_SIZE",
            "Storage zone size", "int", "1073741824", "live",
            "The span the band and zone catalog accounts in. It sets the granularity of what "
            "compaction and the catalog can talk about, not how much is stored. Values below 1 KiB "
            "are raised to it."),
    Setting("storage_engine.stream_max_blob_size", "storage_engine",
            "TS_STREAM_MAX_BLOB_SIZE",
            "Largest streamed blob", "int", "10485760", "live",
            "The ceiling on a single streamed payload. Raising it admits larger objects and raises "
            "the peak memory a single request can hold. Values below 1 KiB are raised to it."),
    Setting("storage_engine.compaction_watermark_bytes", "storage_engine",
            "TS_COMPACTION_WATERMARK_BYTES",
            "Compaction watermark", "int", "268435456", "live",
            "How much uncompacted growth is tolerated before compaction is due. Lower compacts more "
            "often for steadier size; higher defers the work and lets the store run larger."),
    Setting("storage_engine.cold_scan_no_cache_fill", "storage_engine",
            "TS_COLD_SCAN_NO_CACHE_FILL",
            "Cold scans do not fill the cache", "bool", "1", "live",
            "On, a scan that misses does not evict warm entries to make room for pages it will not "
            "read again. Turn it off only if your workload rescans the same cold range."),
    Setting("storage_engine.page_index_cache_bytes", "storage_engine",
            "TS_PAGE_INDEX_CACHE_BYTES",
            "Page index cache", "int", "67108864", "live",
            "Memory held for page index entries. This is resident memory the process keeps, so it "
            "is a direct trade of footprint against lookup latency."),
    Setting("storage_engine.block_index_cache_bytes", "storage_engine",
            "TS_BLOCK_INDEX_CACHE_BYTES",
            "Block index cache", "int", "67108864", "live",
            "Memory held for block index entries. Same trade as the page index cache, on the block "
            "side."),
    Setting("storage_engine.index_dump_wal_gap_bytes", "storage_engine",
            "TS_INDEX_DUMP_WAL_GAP_BYTES",
            "WAL growth before an index dump", "int", "1048576", "live",
            "How much undumped WAL growth triggers a background index dump. A dump writes the "
            "served index, so lowering this makes dumps more frequent and each recovery shorter."),
    Setting("storage_engine.wal_binary_records", "storage_engine",
            "TS_WAL_BINARY_RECORDS",
            "Write log records as protobuf", "bool", "1", "restart",
            "On, a write-ahead log record is protobuf rather than JSON. Reading never consults "
            "this: a payload is decoded by what its first byte says it is, so a log written "
            "across a change reads end to end and turning it back off is not a one-way door. "
            "Worth about 1.5x on the records themselves, measured over 2,000 identical "
            "writes: 787,433 bytes of text against 524,735 binary. Not more than that, and "
            "it is worth knowing why before turning it on expecting a store to halve: the "
            "encoding gives a numeric code only to the field kinds common enough to earn "
            "one and keeps the string otherwise, so the repeated field names that make a "
            "log compress well survive it."),
    Setting("storage_engine.wal_binary_frame", "storage_engine",
            "TS_WAL_BINARY_FRAME",
            "Frame log records by length", "bool", "1", "restart",
            "On, each record carries a length and a checksum in about seven bytes. Off, records "
            "are delimited by a newline in about twenty, and every newline inside a payload has to "
            "be escaped out and back -- two more copies of every record. Decoding handles either, "
            "so a log holding both still loads. Turning it OFF on a store already written with "
            "length frames is NOT supported: finding the tail of the log follows this setting, and "
            "off it searches backwards for a newline, which is not a record boundary in a "
            "length-framed log."),
    Setting("storage_engine.wal_compress_records", "storage_engine",
            "TS_WAL_COMPRESS_RECORDS",
            "Compress log records", "bool", "1", "restart",
            "On, a log record is zstd-compressed before it is written. Measured on one live "
            "segment -- 151 records averaging 13.5 KB -- this is 8.61x, which on a 698 MB log is "
            "about 617 MB back. It ships ON: it spends write CPU to buy disk, and 8.61x is a large "
            "enough return that a deployment which would rather keep the CPU is the one that "
            "should have to say so. Turn it off where write latency is the scarce thing. Reading "
            "never "
            "consults it: a payload says what encoding it is in, so a log written across a change "
            "reads end to end and turning it off again is not a one-way door. Records below 256 "
            "bytes, and any record the codec fails to shrink, are written uncompressed. Each "
            "record is compressed on its own rather than the log in blocks: blocks reach 22.28x "
            "and would end record-level addressing, which page references depend on."),
    Setting("storage_engine.vector_scaled", "storage_engine",
            "TS_VECTOR_SCALED",
            "Store vectors in the scaled form", "bool", "1", "live",
            "On, a vector is stored uniformly scaled, which halves its bytes and leaves ranking "
            "identical. Reads understand both forms, so this can be turned off again without "
            "stranding anything already written."),
    Setting("storage_engine.vector_int8", "storage_engine",
            "TS_VECTOR_INT8",
            "Store vectors quantized to int8", "bool", "0", "live",
            "Off by default, and this is a measured trade rather than a caution. On 713 document "
            "chunks with 8 queries, int8 held 7 of 8 top-1 results and reproduced 3 of 8 result "
            "orders exactly, where the scaled form above held 8 of 8 on both. The cause is "
            "structural: int8 divides each vector by its own peak, so two vectors are not on a "
            "common scale and their order can swap. Turn it on to halve the bytes again when "
            "approximate ranking is acceptable. Reads understand both forms, so it can be turned "
            "off again without stranding anything already written."),
    Setting("storage_engine.node_summary_vector", "storage_engine",
            "TS_NODE_SUMMARY_VECTOR",
            "Nodes carry a copy of their summary vector", "bool", "1", "live",
            "On, a node record carries a copy of its L1 summary's vector so the node can be scored "
            "before its summary is fetched. Only writes consult this: a node either carries the "
            "copy or it does not, and reads handle both."),
    Setting("storage_engine.malloc_trim", "storage_engine",
            "TS_MALLOC_TRIM",
            "Return freed memory to the operating system", "bool", "1", "live",
            "On, the engine asks the allocator to hand back memory it is no longer using. What is "
            "returned is memory the process was not using, so the trade is one-sided -- the escape "
            "hatch exists because a trim costs a walk of the allocator's free lists."),
    Setting("storage_engine.scratch_sweep", "storage_engine",
            "TS_SCRATCH_SWEEP",
            "Reclaim scratch directories from dead processes", "bool", "1", "live",
            "On, scratch directories whose owning process is gone are reclaimed. Turn it off only "
            "to keep an abandoned directory for inspection."),
    Setting("storage_engine.max_retained_finished_jobs", "storage_engine",
            "TS_MAX_RETAINED_FINISHED_JOBS",
            "Completed job statuses kept queryable", "int", "64", "live",
            "Each retained status carries its full output, and a dump output carries a serialized "
            "index -- so an unbounded history meant one index copy retained per dump for the life "
            "of the node. Raise it to keep more history, at that cost per entry."),
    Setting("storage_engine.metrics_max_slot_series", "storage_engine",
            "TS_METRICS_MAX_SLOT_SERIES",
            "Per-slot metric series allowed per shard", "int", "1024", "live",
            "A routing slot is derived per key, so per-slot metrics grow with key count rather "
            "than with anything bounded. This caps how many a single shard may emit; lower it if "
            "your metrics backend is straining on cardinality."),
    Setting("ingestion.require_model_summaries", "ingestion",
            "MATRIXARK_REQUIRE_MODEL_SUMMARIES",
            "Fail instead of writing rule summaries", "bool", "0", "live",
            "On, a summary that cannot reach the model errors instead of silently falling back to "
            "the local rule summariser."),
    Setting("ingestion.time_compression_window_events", "ingestion",
            "MATRIXARK_TIME_COMPRESSION_WINDOW_EVENTS",
            "Time compression window (events)", "int", "", "restart",
            "How many consecutive events are rolled into one compressed node. Larger windows store "
            "less and lose more detail."),
    Setting("ingestion.embed_drainer", "ingestion", "MATRIXARK_EMBED_DRAINER",
            "Background embedding drainer", "bool", "0", "restart",
            "Encode chunks in the background after the write is acknowledged, instead of inline. "
            "Ingest returns sooner; a retrieve issued before the drain still sees the chunk, just "
            "not yet its vector."),
    Setting("ingestion.embed_drainer_batch", "ingestion", "MATRIXARK_EMBED_DRAINER_BATCH",
            "Drainer batch size", "int", "", "restart",
            "Chunks per encoder call. Bigger batches are cheaper per chunk and hold more memory."),
    Setting("ingestion.embed_drainer_interval_ms", "ingestion",
            "MATRIXARK_EMBED_DRAINER_INTERVAL_MS",
            "Drainer interval (ms)", "int", "", "restart",
            "How often the drainer wakes to encode what ingest left behind."),

    # ---- what the local store keeps -------------------------------------------------------------
    # These two together are the deployment's disk ceiling and, less obviously, its ingest cost:
    # the retained window is what a read has to hold and what compaction has to walk, so per
    # document ingest time rises with it until rotation starts discarding. Measured on 1 MB
    # documents, ingest cost per document climbed 2.68 s -> 4.83 s -> 7.35 s at 15, 30 and 60
    # documents and then flattened, and it flattened because rotation had begun. An operator
    # sizing a box needs to see both numbers, and until now neither was offered.
    Setting("ingestion.local_log_max_bytes", "ingestion", "MATRIXARK_LOCAL_JSONL_MAX_BYTES",
            "Event log shard size (bytes)", "int", "67108864", "restart",
            "How large one event-log shard grows before the store rotates to a new one. This "
            "times the retained count below is the disk the event log will occupy."),
    Setting("ingestion.local_log_retention_count", "ingestion",
            "MATRIXARK_LOCAL_JSONL_RETENTION_COUNT",
            "Event log shards retained", "int", "4", "restart",
            "How many rotated shards are kept behind the live one. Records in a dropped shard are "
            "gone from the store, so this is a retention policy and not only a disk setting: at "
            "the defaults the store keeps roughly 336 MB and discards the oldest records past it."),
    # The sharpest memory/latency trade in the local store, and it was not offered at all.
    # Measured on a 100,105-record store, warm read, three alternating runs each:
    #   sharing on : 8.00 s, 2,305 B/record, 761 MB resident
    #   sharing off: 4.58 s, 4,173 B/record, 1,041 MB resident
    # So it costs ~3.4 s on the read that loads the cache and returns ~45% of the cache. Which way
    # that should go depends on the deployment, which is exactly why it belongs on the page.
    Setting("ingestion.share_repeated_values", "ingestion",
            "MATRIXARK_SHARE_REPEATED_VALUES",
            "Share repeated values in the read cache", "bool", "1", "restart",
            "Give every record carrying the same value one shared object instead of a private "
            "copy. On a 100,105-record store this held the cache at 2,305 bytes per record "
            "instead of 4,173, and resident memory at 761 MB instead of 1,041 MB, for about 3.4 "
            "seconds more on the read that loads the cache. Turn it off if that load latency "
            "matters more than the memory."),
    Setting("ingestion.durable_read_cache", "ingestion",
            "MATRIXARK_LOCAL_DURABLE_READ_CACHE_ENABLED",
            "Durable read snapshot", "bool", "1", "restart",
            "Keep a snapshot of the compacted read view beside the event log so a restart serves "
            "reads without replaying it. Turning it off makes the first read after a restart "
            "much slower and saves the disk the snapshot occupies, which is comparable to the "
            "event log itself."),
    Setting("ingestion.durable_read_cache_min_write_ms", "ingestion",
            "MATRIXARK_LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS",
            "Durable read snapshot rewrite floor (ms)", "float", "0", "restart",
            "How long to leave between full rewrites of that snapshot. At 0, every write the "
            "append-only path cannot apply rewrites the whole record set. Measured over 12 "
            "queries interleaved with ingest across 128 documents: a floor of 5000 removed all "
            "12 rewrites and took the median query from 2,352 ms to 1,922 ms, while the cold "
            "load rose from 1,391 ms to 1,507 ms. Queries are frequent and restarts are not, so "
            "a floor pays for most deployments -- but it buys query time with startup time, "
            "which is why it ships at 0 and is offered here."),

    # ---- edge limits ---------------------------------------------------------------------------
    Setting("limits.ingest_rps", "limits", "MATRIXARK_RL_INGEST_RPS",
            "Ingest rate limit (req/s)", "float", "5000", "restart",
            "Token-bucket refill rate for ingest, per key."),
    Setting("limits.ingest_burst", "limits", "MATRIXARK_RL_INGEST_BURST",
            "Ingest burst", "float", "10000", "restart",
            "How far ingest may run ahead of the refill rate before it is throttled."),
    Setting("limits.retrieve_rps", "limits", "MATRIXARK_RL_RETRIEVE_RPS",
            "Retrieve rate limit (req/s)", "float", "6000", "restart",
            "Token-bucket refill rate for retrieve, per key."),
    Setting("limits.retrieve_burst", "limits", "MATRIXARK_RL_RETRIEVE_BURST",
            "Retrieve burst", "float", "12000", "restart",
            "How far retrieve may run ahead of the refill rate."),
    Setting("limits.blob_streams", "limits", "MATRIXARK_RL_BLOB_STREAMS",
            "Concurrent blob streams", "int", "500", "restart",
            "Simultaneous blob uploads or downloads the edge will proxy."),
    Setting("limits.max_body_bytes", "limits", "MATRIXARK_QUOTA_MAX_BODY_BYTES",
            "Max request body (bytes)", "int", "16777216", "restart",
            "Bodies above this are refused with 413 before they are parsed."),
    Setting("limits.max_batch", "limits", "MATRIXARK_QUOTA_MAX_BATCH",
            "Max records per batch", "int", "1000", "restart",
            "Records or messages in one ingest call."),
    Setting("limits.max_blob_bytes", "limits", "MATRIXARK_QUOTA_MAX_BLOB_BYTES",
            "Max blob size (bytes)", "int", "5368709120", "restart",
            "Ceiling on a single streamed blob."),
    Setting("limits.backend_timeout_ms", "limits", "MATRIXARK_GATEWAY_BACKEND_TIMEOUT_MS",
            "Backend timeout (ms)", "int", "30000", "restart",
            "How long the edge waits for the backend before answering 504. Worth raising only if "
            "an extraction model in the path is genuinely slower than this."),
]

# ---- retrieval behaviour, DERIVED from the tenant-policy registry ------------------------------
# matrixark_tenant_policy.KNOBS already carries every one of these with its type, default, choices
# and a written rationale -- several of them recording a measurement that explains why the default
# is what it is. Re-typing them here would give the portal a second copy to drift from, and the
# copy customers read would be the one nobody updates. Derive instead, and skip any knob whose env
# var an explicit Setting above already drives, so no two settings write the same variable.
def _humanize(name: str) -> str:
    return name.replace("_", " ").replace("max ", "max ").capitalize()


_KNOB_ENV_FROZEN_AT_IMPORT = {
    "MATRIXARK_TOP_K_PER_LAYER",
    "MATRIXARK_MAX_CANDIDATES_PER_NODE",
    "MATRIXARK_MAX_GLOBAL_CANDIDATES",
    "MATRIXARK_MAX_SELECTED_REFS",
    "MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES",
}


def _build_constant(constant: str, module: str = "matrixark_mcp_runtime_config") -> Optional[str]:
    """The value a build constant holds, read from the module that decides it.

    Never re-typed here. A table of numbers in this file would be the second copy the section above
    refuses to keep, and it would drift the moment a budget moved.
    """
    try:
        try:
            source = __import__("tools." + module, fromlist=[module])
        except ImportError:
            source = __import__(module)
    except Exception:  # pragma: no cover - portal still works without the retrieval modules
        return None
    value = getattr(source, constant, None)
    return None if value is None else str(value)


def _build_default(name: str) -> Optional[str]:
    """What retrieval uses for this knob when nobody has set anything."""
    return _build_constant("DEFAULT_" + name.upper())


def _knob_settings() -> List[Setting]:
    try:
        try:
            from tools import matrixark_tenant_policy as policy  # type: ignore
        except ImportError:
            import matrixark_tenant_policy as policy  # type: ignore
    except Exception:  # pragma: no cover - portal still works without the policy module
        return []
    taken = {s.env for s in SETTINGS if s.env}
    derived: List[Setting] = []
    for name, knob in sorted(getattr(policy, "KNOBS", {}).items()):
        env = getattr(knob, "env", "")
        if not env or env in taken:
            continue
        taken.add(env)
        kind = {"bool": "bool", "int": "int", "choice": "str"}.get(knob.kind, "str")
        choices = sorted(knob.choices) if getattr(knob, "choices", None) else None
        default = knob.default
        default = ("1" if default else "0") if isinstance(default, bool) else str(default)
        description = knob.description
        # For the five frozen budgets the registry's default is NOT what a deployment gets.
        # `_tenant_retrieval_limit` in matrixark_local_adapter_retrieve deliberately does not
        # resolve() them -- resolving would "silently multiply the budget for every deployment
        # that never configured one" -- so with nothing set retrieval uses the build constant,
        # which is 10x to 156x smaller. Showing the registry number told an operator their
        # deployment does something it does not, and export_settings(include_defaults=True) then
        # wrote that number to the target as an EXPLICIT value, so cloning a deployment raised
        # its budgets by up to 156x. Show what this build uses, and say where the other number
        # applies.
        if env in _KNOB_ENV_FROZEN_AT_IMPORT:
            build = _build_default(name)
            if build is not None and build != default:
                description = (
                    knob.description
                    + " -- With nothing set this deployment uses %s, not the %s above: retrieval "
                      "reads an explicit tenant record or an explicit value here, and otherwise "
                      "its own build default. The number above applies where someone sets it."
                    % (build, default))
                default = build
        derived.append(Setting(
            "behaviour." + name, "behaviour", env, _humanize(name), kind, default,
            # tenant_policy.resolve() reads os.environ per call, so most of these are live.
            # Five are not: matrixark_mcp_core and matrixark_mcp_runtime_config capture their env
            # DEFAULT into a module constant at import, so a deployment-wide change waits for a
            # restart. The audit test (test_matrixark_gateway_config_audit) keeps this list honest.
            #
            # This used to add "even though a per-tenant policy record still applies immediately".
            # That was false: retrieval read none of these knobs at all, so a tenant setting one
            # got nothing. Four are now wired
            # (test_matrixark_retrieval_budgets_follow_the_tenant); the fifth,
            # cross_session_profile_max_candidates, is still unread in
            # matrixark_mcp_budget_policies. A comment asserting a broken thing works is what
            # stops anyone checking, so it is worth more care than the code it sits beside.
            "restart" if env in _KNOB_ENV_FROZEN_AT_IMPORT else "live",
            description, choices))
    return derived


# Settings written by hand above whose declared default is not the one the build runs. Same
# hazard as the frozen knobs, and it is not only a display problem: `export_settings` with
# include_defaults writes a declared default to the target as an EXPLICIT value, so cloning a
# deployment used to hand it a shared-resource budget of 0.10 where the build runs 0.25, and three
# blanks where the build has numbers. Read them from the module that decides them, for the same
# reason the knobs are read rather than re-typed.
_EXPLICIT_BUILD_DEFAULT = {
    "retrieval.cross_session_budget_ratio": "DEFAULT_CROSS_SESSION_BUDGET_RATIO",
    "retrieval.cross_session_max_sessions": "DEFAULT_CROSS_SESSION_MAX_SESSIONS",
    "skills.shared_resource_budget_ratio": "DEFAULT_SHARED_RESOURCE_BUDGET_RATIO",
    "ingestion.time_compression_window_events": "TIME_COMPRESSION_WINDOW_EVENTS",
    # The extraction call has no guard: with nothing set it posts to
    # http://127.0.0.1:8000/v1/chat/completions asking for qwen2.5:1.5b. The portal showed both
    # fields empty and the connection test refused to run, so the one deployment shape most likely
    # to be misconfigured -- a cloud provider selected, endpoint never filled in -- was described
    # as having no endpoint at all rather than as pointed at a local port.
    "extraction.base_url": ("EXTRACTION_LLM_BASE_URL", "matrixark_mcp_extraction_provider"),
}


def _apply_build_defaults(settings: List[Setting]) -> None:
    """Point each of the above at what the build actually runs, and say so in the help.

    A blank default is the worse of the two cases it fixes: the field reads as "nothing is in
    force" on a deployment that is, in fact, running a number.
    """
    by_key = {s.key: s for s in settings}
    for key, constant in _EXPLICIT_BUILD_DEFAULT.items():
        setting = by_key.get(key)
        if setting is None:  # pragma: no cover - a renamed setting fails its own test, not here
            continue
        build = (_build_constant(*constant) if isinstance(constant, tuple)
                 else _build_constant(constant))
        if build is None:
            continue
        # Correct the default only when it needs correcting, but say what the build runs either
        # way. A hand-written default that happens to be right today is still a second copy of the
        # number, and the help is where an operator finds out which one is in force.
        if build != setting.default:
            setting.default = build
        setting.help = setting.help.rstrip() + (
            " With nothing set this deployment runs %s." % build)


_apply_build_defaults(SETTINGS)
SETTINGS.extend(_knob_settings())
SETTINGS_BY_KEY: Dict[str, Setting] = {s.key: s for s in SETTINGS}


# ================================================================================================
# The read-only deployment inventory
# ================================================================================================
# Storage locations, cluster addresses, worker counts and the auth mode are things a customer must
# be able to SEE -- half of every support conversation is "what is this deployment actually
# pointed at" -- and must never be able to CHANGE from a web page. Repointing a storage directory
# or a metaserver address from a browser form does not reconfigure a running deployment, it strands
# its data; turning auth off from inside the authenticated surface is a hole with no upside. So
# these live outside the writable registry entirely rather than being a writable field with a
# warning label on it.
INVENTORY: List[Json] = [
    {"group": "Serving", "vars": [
        ("MATRIXARK_HTTP_HOST", "Bind address"),
        ("MATRIXARK_HTTP_PORT", "Port"),
        ("MATRIXARK_HTTP_MODE", "Mode"),
        ("MATRIXARK_GATEWAY_THREADS", "Gateway threads"),
        ("MATRIXARK_DATANODE_URL", "Datanode URL"),
        ("MATRIXARK_GATEWAY_DIRECT_BACKEND", "Direct backend path"),
        ("MATRIXARK_GATEWAY_DIRECT_URL", "Direct backend URL"),
        ("WEB_CONCURRENCY", "Worker processes"),
    ]},
    {"group": "Access", "vars": [
        ("MATRIXARK_REQUIRE_AUTH", "Auth required"),
        ("MATRIXARK_ACCESS_MODE", "Access mode"),
        ("MATRIXARK_API_KEYS_FILE", "Key store"),
        ("MATRIXARK_API_KEYS_HASHED_FILE", "Hashed key store"),
        ("MATRIXARK_HTTP_ALLOWED_ORIGIN", "Allowed origin"),
        ("MATRIXARK_GATEWAY_FORWARD_API_KEY", "Forward key to backend"),
    ]},
    {"group": "Storage", "vars": [
        ("TS_STORAGE_BACKEND", "Backend"),
        ("TS_SHARED_STORE_DIR", "Shared store"),
        ("TS_PAGE_STORE_DIR", "Block store"),
        ("TS_BLOB_STORE_DIR", "Blob store"),
        ("TS_INDEX_DIR", "Index"),
        ("TS_CACHE_DIR", "Cache"),
        ("TS_CACHE_MEMORY_BYTES", "Cache budget"),
    ]},
    {"group": "Cluster", "vars": [
        ("TS_META_ADDR", "Metaserver"),
        ("TS_SHARD_ID", "Shard"),
        ("TS_SERVER_NODE_ID", "Node"),
        ("TS_META_RAFT", "Meta replication"),
        ("TS_RAFT_AUTO_FAILOVER", "Auto failover"),
        ("TS_DATA_RAFT_READ_MODE", "Read mode"),
    ]},
]

_SECRETISH = ("KEY", "SECRET", "TOKEN", "PASSWORD", "CREDENTIAL")


def inventory() -> List[Json]:
    """The read-only deployment view. Anything whose NAME looks like it holds a credential is
    reported as configured/not rather than by value -- `MATRIXARK_API_KEYS_FILE` is a path and is
    safe, but the rule is on the name so a variable added here later cannot leak by oversight."""
    out: List[Json] = []
    for block in INVENTORY:
        rows: List[Json] = []
        for name, label in block["vars"]:
            raw = os.environ.get(name, "")
            secretish = any(token in name for token in _SECRETISH) and not name.endswith("_FILE")
            rows.append({
                "env": name, "label": label,
                "value": ("set" if raw else "") if secretish else raw,
                "set": bool(raw),
                "redacted": bool(secretish and raw),
            })
        out.append({"group": block["group"], "rows": rows})
    return out

# One-click starting points. A preset is only a set of values for the keys above — applying one is
# exactly the same write as typing the fields by hand, so nothing here can set anything the closed
# registry does not already allow. Secrets are never part of a preset.
PRESETS: Dict[str, Json] = {
    "deepseek": {
        "label": "DeepSeek (extraction)",
        "note": "DeepSeek supplies extraction only — it has no embeddings API, so pair it with a "
                "local encoder or an OpenAI-compatible embedding endpoint.",
        "values": {
            "extraction.provider": "openai_compatible",
            "extraction.base_url": "https://api.deepseek.com/v1",
            "extraction.model": "deepseek-chat",
            "extraction.api_key_env": "DEEPSEEK_API_KEY",
        },
    },
    "openai": {
        "label": "OpenAI (extraction + embedding)",
        "note": "One provider for both sides.",
        "values": {
            "extraction.provider": "openai_compatible",
            "extraction.base_url": "https://api.openai.com/v1",
            "extraction.model": "gpt-4o-mini",
            "extraction.api_key_env": "OPENAI_API_KEY",
            "embedding.provider": "openai_compatible",
            "embedding.api_base": "https://api.openai.com/v1",
            "embedding.model": "text-embedding-3-small",
            "embedding.api_key_env": "OPENAI_API_KEY",
            "embedding.require_model_embeddings": "1",
        },
    },
    "local_minilm": {
        "label": "Local MiniLM encoder (embedding)",
        "note": "The co-located CPU encoder started by tools/context_minilm_embed_server.py. "
                "Multilingual, so it holds up on mixed Chinese/English corpora where an "
                "English-only MiniLM does not.",
        "values": {
            "embedding.provider": "openai_compatible",
            "embedding.api_base": "http://127.0.0.1:8400/v1",
            "embedding.model": "paraphrase-multilingual-MiniLM-L12-v2",
            "embedding.api_key_env": "LOCAL_ENCODER_KEY",
            "embedding.require_model_embeddings": "1",
        },
    },
    "voyage": {
        "label": "Voyage (embedding)",
        "note": "Voyage supplies embeddings only -- pair it with an extraction provider. The key "
                "variable is left to follow the provider, so it lands in VOYAGE_API_KEY.",
        "values": {
            "embedding.provider": "voyage",
            "embedding.api_base": "https://api.voyageai.com/v1",
            "embedding.model": "voyage-3",
            "embedding.require_model_embeddings": "1",
        },
    },
    "anthropic": {
        "label": "Anthropic (extraction)",
        "note": "Anthropic has no embeddings API, so pair it with a local encoder or an "
                "OpenAI-compatible embedding endpoint.",
        "values": {
            "extraction.provider": "anthropic",
            "extraction.anthropic_base_url": "https://api.anthropic.com",
            "extraction.model": "claude-sonnet-5",
        },
    },
    "ollama": {
        "label": "Ollama (local extraction)",
        "note": "Ollama's OpenAI-compatible shim, on the default port.",
        "values": {
            "extraction.provider": "openai_compatible",
            "extraction.base_url": "http://127.0.0.1:11434/v1",
            "extraction.model": "qwen2.5:7b",
            "extraction.api_key_env": "OLLAMA_API_KEY",
        },
    },
}


# Extraction models worth suggesting. There is no measured catalogue for these -- unlike the
# encoders, where `encoder_catalog()` in the gateway holds hit@1/throughput/footprint from a real
# corpus and the picker serves THAT rather than a second list written from general knowledge.
# `serves` names the dispatch the model belongs to, in the vocabulary of
# `extraction_provider_effect`. A suggestion the selected provider cannot serve is not a suggestion:
# the whole list was OpenAI-compatible, so an Anthropic deployment was offered six models Anthropic
# does not have, and none of the ones it does.
EXTRACTION_CATALOGUE: List[Json] = [
    {"model": "deepseek-chat", "serves": "openai",
     "note": "DeepSeek's general model. No embeddings API — pair it with a local encoder."},
    {"model": "deepseek-reasoner", "serves": "openai",
     "note": "Slower and more deliberate; raise the timeout."},
    {"model": "gpt-4o-mini", "serves": "openai",
     "note": "OpenAI, inexpensive, good enough for extraction."},
    {"model": "gpt-4o", "serves": "openai", "note": "OpenAI, stronger and dearer."},
    {"model": "qwen2.5:7b", "serves": "openai", "note": "Runs locally under Ollama."},
    {"model": "qwen2.5:1.5b", "serves": "openai",
     "note": "Small enough for a laptop; extraction quality drops."},
    {"model": "claude-opus-5", "serves": "anthropic",
     "note": "Anthropic's strongest, and its dearest."},
    {"model": "claude-sonnet-5", "serves": "anthropic",
     "note": "Anthropic's default here — the balance most extraction wants."},
    {"model": "claude-haiku-4-5-20251001", "serves": "anthropic",
     "note": "Anthropic's fastest and cheapest; extraction quality drops."},
]


def _get_json(url: str, headers: Dict[str, str], timeout: float) -> Tuple[int, Any]:
    request = urllib.request.Request(url, method="GET", headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:  # nosec B310 - operator URL
        raw = response.read()
        try:
            return response.getcode(), json.loads(raw.decode("utf-8"))
        except ValueError:
            return response.getcode(), None


def _configured_extraction_provider() -> str:
    """The extraction provider a deployment has named, or "" when it has named none.

    The specific name wins over the general one, which is the precedence the provider modules
    themselves use. Three resolvers in this module each spelled the pair out, so a change to that
    precedence would have had to be made three times.

    Empty rather than "deterministic" on purpose, and the difference matters here: these callers
    need to know whether anything is set AT ALL, and a resolver answering "deterministic" for an
    unset deployment could not tell the two apart.
    """
    return os.environ.get(
        "MATRIXARK_UNDERSTANDING_PROVIDER",
        os.environ.get("MATRIXARK_EXTRACTION_PROVIDER", ""))


def discover_models(target: str, timeout: float = 8.0) -> Json:
    """Ask the configured endpoint what it serves.

    An OpenAI-compatible server answers `GET <base>/models`. Asking beats typing: a name that is
    merely misspelled fails at ingest time, hours later, as a silent fall back to the deterministic
    path -- which answers 200 and looks like a working deployment.
    """
    document = load()
    values: Dict[str, str] = {k: str(v) for k, v in (document.get("values") or {}).items()}
    auth = "bearer"
    if target == "embedding":
        base = (os.environ.get("MATRIXARK_EMBEDDING_API_BASE", "").strip()
                or os.environ.get("MATRIXARK_EMBED_BASE_URL", "").strip())
        key_env = _env_name(SETTINGS_BY_KEY["embedding.api_key"], values)
    elif extraction_provider_effect(_configured_extraction_provider()) == "anthropic":
        # Anthropic reads its own base URL and authenticates with x-api-key. Asking the OpenAI base
        # URL, which this deployment never sets, answered "Set the base URL first" -- pointing at a
        # field that provider does not read, in the one panel that exists to stop a misspelt model
        # name reaching ingest.
        base = os.environ.get("MATRIXARK_ANTHROPIC_API_BASE",
                              "https://api.anthropic.com").strip().rstrip("/") + "/v1"
        key_env = _env_name(SETTINGS_BY_KEY["extraction.api_key"], values)
        auth = "anthropic"
    else:
        base = os.environ.get("MATRIXARK_EXTRACTION_BASE_URL", "").strip()
        key_env = _env_name(SETTINGS_BY_KEY["extraction.api_key"], values)
    base = base.rstrip("/")
    if not base:
        return {"available": False, "reason": "no_base_url",
                "detail": "Set the base URL first; there is nothing to ask."}
    key = os.environ.get(key_env, "")
    if not key:
        headers: Dict[str, str] = {}
    elif auth == "anthropic":
        headers = {"x-api-key": key,
                   "anthropic-version": os.environ.get("MATRIXARK_ANTHROPIC_VERSION",
                                                       "2023-06-01")}
    else:
        headers = {"Authorization": "Bearer " + key}
    try:
        status, parsed = _get_json(base + "/models", headers, timeout)
    except urllib.error.HTTPError as exc:
        return {"available": False, "reason": "http_%d" % exc.code,
                "detail": "The endpoint refused the model listing (HTTP %d). Some servers do not "
                          "implement it; the field is still free text." % exc.code}
    except Exception as exc:
        return {"available": False, "reason": exc.__class__.__name__,
                "detail": "Could not reach %s/models: %s" % (base, str(exc)[:160])}
    if not isinstance(parsed, dict):
        return {"available": False, "reason": "unexpected_body",
                "detail": "The endpoint answered %d with something that is not a model list."
                          % status}
    rows = parsed.get("data")
    models = []
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and row.get("id"):
                models.append(str(row["id"]))
            elif isinstance(row, str):
                models.append(row)
    return {"available": True, "base_url": base, "models": sorted(set(models))[:200],
            "count": len(set(models))}


# What each provider's own code falls back to when no model is named. Read from that code, for
# the reason the budgets are: a number re-typed here is the second copy this file refuses to keep.
_EXTRACTION_MODEL_DEFAULT = {
    "anthropic": ("ANTHROPIC_LLM_MODEL", "matrixark_mcp_core"),
    "openai": ("EXTRACTION_LLM_MODEL", "matrixark_mcp_extraction_provider"),
}


def extraction_model_default(provider: Optional[str] = None) -> str:
    """The model the selected provider calls when the field is blank.

    One field is offered, so the portal has to be able to say what blank means on the provider in
    use -- and the two providers do not agree, which is why the field declares no default of its own.
    """
    resolved = provider if provider is not None else _configured_extraction_provider()
    effect = extraction_provider_effect(resolved)
    named = _EXTRACTION_MODEL_DEFAULT.get(effect if effect == "anthropic" else "openai")
    return _build_constant(*named) or ""


def model_catalogue(target: str, provider: Optional[str] = None) -> List[Json]:
    """Extraction models to suggest, narrowed to the ones the selected provider serves.

    Embeddings are NOT served from here. The gateway builds that list from `encoder_catalog()`,
    which carries a measurement over a real corpus; a second hand-written list beside it did not
    stay agreed with it, and the disagreement was about which encoder to choose.

    A provider that calls no model at all gets the whole list rather than an empty one: there is
    nothing wrong to narrow towards yet, and an empty picker beside a provider dropdown reads as a
    broken page rather than as a choice still to be made.
    """
    if target == "embedding":
        raise ValueError(
            "embedding models come from the gateway's measured encoder_catalog(), not from here")
    entries = [dict(entry) for entry in EXTRACTION_CATALOGUE]
    if provider is None:
        provider = _configured_extraction_provider()
    effect = extraction_provider_effect(provider)
    if effect == "rules":
        return entries
    return [entry for entry in entries if entry.get("serves") == effect]


# ================================================================================================
# Persistence
# ================================================================================================
# Knobs the portal offers, the policy resolves, and nothing reads. Setting one writes a value
# that comes back when you ask for it and changes nothing about how the deployment behaves.
#
# This is a static list against a fact about the code, which is exactly the kind of note that goes
# stale. It cannot here: test_matrixark_a_knob_the_portal_offers_is_read_by_something DERIVES the
# same set -- accessor reachable from a production caller, variable read directly, or knob asked
# for by name -- and fails if this disagrees with it in EITHER direction. Wiring one up forces its
# removal from here; a knob that quietly stops being read forces its addition.
#
# Not a reason to hide the control. A deployment may already have one of these set, and a field
# that vanishes takes its value out of view while leaving it in the file.
KNOBS_READ_BY_NOTHING = frozenset({
    "embed_node_path_prefix",
    "generate_l1_summaries",
    "max_event_text_chars",
    "max_summary_text_chars",
    "recall_reinforcement",
    "return_all_candidate_threshold",
    "return_all_candidates",
    "skill_description_always",
    "summarize_aggregation_only_nodes",
    "summary_levels",
    "write_secondary_index",
})


_UNREAD_ENVS: Optional[frozenset] = None


def _unread_envs() -> frozenset:
    """The environment variables behind the knobs nothing reads.

    Matched by VARIABLE, not by key prefix. Ten of the eleven surface as `behaviour.<knob>` and
    `skill_description_always` surfaces as `skills.description_always`, so a prefix rule marked
    ten of them and left the eleventh looking like a working control -- which is the exact defect
    this badge exists to end, reintroduced by the code drawing it.
    """
    global _UNREAD_ENVS
    if _UNREAD_ENVS is None:
        try:
            import matrixark_tenant_policy as _tp
            _UNREAD_ENVS = frozenset(
                _tp.KNOBS[name].env for name in KNOBS_READ_BY_NOTHING
                if name in _tp.KNOBS and _tp.KNOBS[name].env)
        except Exception:  # pragma: no cover - the policy module is optional at import time
            _UNREAD_ENVS = frozenset()
    return _UNREAD_ENVS


def setting_is_read(env: str) -> bool:
    """Whether anything in this build reads the setting whose variable is `env`."""
    return (env or "") not in _unread_envs()


def config_path() -> str:
    """Where the runtime config lives. ``MATRIXARK_RUNTIME_CONFIG_FILE`` wins; else ~/.matrixark/."""
    explicit = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE", "").strip()
    if explicit:
        return explicit
    home = os.environ.get("MATRIXARK_HOME", "").strip() or os.path.join(
        os.path.expanduser("~"), ".matrixark")
    return os.path.join(home, DEFAULT_CONFIG_FILENAME)


#: Variables that WIN over a setting's own, and are offered nowhere themselves.
#:
#: `matrixark_mcp_core` reads MATRIXARK_ANTHROPIC_TIMEOUT_SEC first and falls back to
#: MATRIXARK_EXTRACTION_TIMEOUT_SEC, so on an Anthropic deployment the field the portal shows is
#: not what decides. With the override set the page displayed the fallback -- 30 where the
#: extraction path was using 5 -- with no badge, and editing the field changed nothing.
#:
#: Written here because the portal needs it per request and deriving it means parsing the tree.
#: `test_matrixark_a_variable_the_portal_does_not_offer` derives the same set from the source and
#: fails in either direction, so this cannot drift from what the code does.
#:
#: key -> (overriding variable, the setting whose value decides, the EFFECT that makes it apply)
#:
#: An effect, not a list of provider names. `extraction_provider_effect` maps anthropic,
#: anthropic_messages and claude onto the same path, so a literal ("anthropic",) here would have
#: missed two names whose deployments take that path -- the badge would not fire on exactly the
#: deployments it exists for. That is this file's own defect, reintroduced by its fix, and the
#: no-classifying-by-hand guard caught it before it shipped.
#: Named fields rather than a bare tuple. A tuple of strings holding a provider name reads to
#: `test_matrixark_the_encoder_summary_asks_the_classifier` as a surface classifying a provider by
#: hand, which is what its whole guard exists to stop -- and it was right to look twice. `effect`
#: is the answer `extraction_provider_effect` gives, not a name to match against.
_UNOFFERED_OVERRIDES: Dict[str, Json] = {
    "extraction.timeout_sec": {"env": "MATRIXARK_ANTHROPIC_TIMEOUT_SEC",
                               "depends_on": "extraction.provider",
                               "effect": "anthropic"},
    "extraction.max_tokens": {"env": "MATRIXARK_ANTHROPIC_MAX_TOKENS",
                              "depends_on": "extraction.provider",
                              "effect": "anthropic"},
}


def unoffered_override(key: str, values: Dict[str, str]) -> Optional[Json]:
    """The variable currently overriding `key`, or None.

    None unless BOTH hold: the variable is set, and the setting it depends on has a value that
    makes the override apply. A variable that is set on a deployment the override does not reach
    is not overriding anything, and saying it is would be the same false alarm as reporting every
    healthy plan as a substitution.
    """
    entry = _UNOFFERED_OVERRIDES.get(key)
    if entry is None:
        return None
    variable, depends_on, effect = entry["env"], entry["depends_on"], entry["effect"]
    raw = os.environ.get(variable)
    if raw is None or not str(raw).strip():
        return None
    setting = SETTINGS_BY_KEY.get(depends_on)
    if setting is None:
        return None
    current, _source = _effective(setting, values)
    # Through the classifier, so every name that reaches this path is covered and a new alias
    # needs no change here.
    if extraction_provider_effect(current) != effect:
        return None
    return {"env": variable, "value": str(raw).strip(), "depends_on": depends_on,
            "when": "%s reaches the %s path" % (depends_on, effect)}


def config_file_status() -> Json:
    """Whether the stored configuration is being applied, and when it is not, why.

    ``load()`` returns the same empty document for a file that is absent, unreadable, unparsable
    or the wrong shape, and it is right to: a deployment must start whatever state that file is
    in. But those are four situations and only one of them is normal. In the other three every
    stored setting is being ignored and the deployment is running built-in defaults -- which on
    the screen is indistinguishable from having configured nothing at all.

    Read separately rather than reported by ``load()``, so the hot path keeps its one job and its
    promise never to raise. This is asked for once, by the page that draws it.
    """
    path = config_path()
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = handle.read()
    except FileNotFoundError:
        # Nothing stored, so nothing is being ignored. This is the ordinary state of a fresh
        # deployment and must NOT be reported as a problem.
        return {"state": "absent", "path": path, "applied": True}
    except OSError as exc:
        return {"state": "unreadable", "path": path, "applied": False,
                "detail": exc.strerror or exc.__class__.__name__}
    try:
        document = json.loads(raw)
    except ValueError as exc:
        return {"state": "unparsable", "path": path, "applied": False,
                "detail": str(exc)[:200]}
    if not isinstance(document, dict):
        return {"state": "wrong_shape", "path": path, "applied": False,
                "detail": "the file holds a %s, not an object" % type(document).__name__}
    values = document.get("values")
    if values is not None and not isinstance(values, dict):
        return {"state": "wrong_shape", "path": path, "applied": False,
                "detail": "\"values\" holds a %s, not an object" % type(values).__name__}
    return {"state": "ok", "path": path, "applied": True,
            "settings": len(values or {})}


def load() -> Json:
    """The stored document, or an empty one. A corrupt file is treated as empty, never fatal: a
    deployment must still start when someone hand-edits this file and breaks it."""
    path = config_path()
    try:
        with open(path, "r", encoding="utf-8") as handle:
            document = json.load(handle)
    except (OSError, ValueError):
        return {"values": {}, "updated_at": None, "updated_by": None}
    if not isinstance(document, dict):
        return {"values": {}, "updated_at": None, "updated_by": None}
    values = document.get("values")
    document["values"] = values if isinstance(values, dict) else {}
    return document


def _store(document: Json) -> None:
    """Write the document 0600, atomically. Owner-only because it holds provider keys."""
    path = config_path()
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    handle_fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".runtime_config.", suffix=".tmp")
    try:
        os.chmod(temp_path, 0o600)
        with os.fdopen(handle_fd, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_path, path)
        os.chmod(path, 0o600)
    except BaseException:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


# ================================================================================================
# Applying stored values to the process environment
# ================================================================================================
# What each provider reads when nothing names the variable. These mirror the fallbacks written in
# the provider code -- matrixark_mcp_embeddings._api_embedding_config and the ANTHROPIC_/EXTRACTION_
# module constants in matrixark_mcp_core -- and a test parses those modules and fails if they drift.
# Flattening them to one name is how the key ended up in a variable the provider never read: pick
# Voyage or Anthropic, type the key, and it landed in OPENAI_API_KEY while the encoder looked in
# VOYAGE_API_KEY and found nothing. Nothing reports that; embeddings just quietly go back to hash
# vectors unless "Fail instead of falling back" is on.
_PROVIDER_KEY_VARIABLE: Dict[str, Dict[str, str]] = {
    "embedding.api_key": {"voyage": "VOYAGE_API_KEY"},
    "extraction.api_key": {"anthropic": "ANTHROPIC_API_KEY"},
}
_KEY_VARIABLE_FALLBACK = "OPENAI_API_KEY"


def _selected_provider(secret_key: str, values: Dict[str, str]) -> str:
    """Which provider the deployment is on, by the same precedence the key itself uses: what the
    portal stored, else what the launcher set, else the registry default."""
    setting = SETTINGS_BY_KEY[secret_key.rsplit(".", 1)[0] + ".provider"]
    return (values.get(setting.key)
            or os.environ.get(setting.env, "").strip()
            or setting.default).strip()


def _env_name(setting: Setting, values: Dict[str, str]) -> str:
    """The environment variable a setting writes to. For the two secrets that is not fixed: the key
    lands in whatever variable ``*.api_key_env`` names, so a customer on DeepSeek gets their key in
    DEEPSEEK_API_KEY rather than a MatrixArk-specific name the provider code never reads.

    Left empty -- the default -- the name follows the selected provider, so the common case needs no
    decision at all. Every other provider-dependent field here already defaults to empty for the
    same reason; these two used to pin one provider's name.
    """
    if setting.key == "extraction.model":
        # The same reason the key below is routed: the value a customer types has to reach the code
        # that reads it, and which variable that is is not the customer's problem. Anthropic reads
        # MATRIXARK_ANTHROPIC_MODEL and ignores MATRIXARK_EXTRACTION_MODEL entirely, so a model
        # typed into one field on that deployment configured nothing and said nothing.
        provider = (values.get("extraction.provider")
                    or _configured_extraction_provider())
        if extraction_provider_effect(provider) == "anthropic":
            return "MATRIXARK_ANTHROPIC_MODEL"
        return "MATRIXARK_EXTRACTION_MODEL"
    if setting.key not in _PROVIDER_KEY_VARIABLE:
        return setting.env
    group = setting.key.rsplit(".", 1)[0]
    chosen = (values.get(group + ".api_key_env")
              or os.environ.get(SETTINGS_BY_KEY[group + ".api_key_env"].env, "").strip())
    if chosen:
        return chosen
    provider = _selected_provider(setting.key, values)
    return _PROVIDER_KEY_VARIABLE[setting.key].get(provider, _KEY_VARIABLE_FALLBACK)


def apply_boot(document: Optional[Json] = None) -> List[str]:
    """Seed the process environment from the stored file, WITHOUT overriding the launcher.

    Called once at gateway startup. A variable already present in the boot environment was set
    deliberately by whoever started the process and keeps precedence, matching
    ``matrixark_load_config``. Returns the environment variable names actually seeded.
    """
    document = load() if document is None else document
    values: Dict[str, str] = {k: str(v) for k, v in (document.get("values") or {}).items()}
    seeded: List[str] = []
    _BOOT_EFFECTIVE.clear()
    for setting in SETTINGS:
        if setting.key not in values:
            continue
        value = values[setting.key]
        name = _env_name(setting, values)
        if not name or name in _BOOT_ENV:
            continue
        if value == "":
            continue
        os.environ[name] = value
        seeded.append(name)
    # The provider modules read MATRIXARK_EXTRACTION_PROVIDER as the fallback name for
    # MATRIXARK_UNDERSTANDING_PROVIDER; keep both in step so a portal write is not half-applied.
    if "extraction.provider" in values and "MATRIXARK_EXTRACTION_PROVIDER" not in _BOOT_ENV:
        os.environ["MATRIXARK_EXTRACTION_PROVIDER"] = values["extraction.provider"]
        seeded.append("MATRIXARK_EXTRACTION_PROVIDER")
    # After the seeding, because this has to be the value the process will actually read -- the
    # stored one where it was applied, the launcher's where that took precedence.
    for setting in SETTINGS:
        if setting.applies == "restart":
            _BOOT_EFFECTIVE[setting.key] = _effective(setting, values)[0]
    return seeded


# ================================================================================================
# Reads and writes
# ================================================================================================
def _effective(setting: Setting, values: Dict[str, str]) -> Tuple[str, str]:
    """(value, source) for one setting. Source is where the effective value came from."""
    name = _env_name(setting, values)
    live = os.environ.get(name, "") if name else ""
    stored = values.get(setting.key, "")
    if live:
        # A variable set before this process started is the launcher's; one that matches what the
        # portal stored (and was not in the boot env) came from the portal.
        if name in _BOOT_ENV and _BOOT_ENV.get(name) == live:
            return live, "environment"
        if stored and stored == live:
            return live, "portal"
        return live, "environment"
    if stored:
        return stored, "portal"
    return setting.default, "default"


def _override_layers_by_env() -> Dict[str, List[str]]:
    """For each env var that a policy knob reads, which layers can override what is set here.

    Derived from the knob registry, not written into a group note. The knobs are spread across two
    settings groups, so a note on one of them left the other three saying nothing -- and they are
    the ones a user can set.
    """
    try:
        from tools import matrixark_tenant_policy as policy  # type: ignore
    except Exception:
        try:
            import matrixark_tenant_policy as policy  # type: ignore
        except Exception:
            return {}
    out: Dict[str, List[str]] = {}
    for knob in policy.KNOBS.values():
        layers = ["tenant"] + (["user"] if knob.layer == "read" else [])
        for name in (knob.env,) + tuple(knob.env_aliases):
            out[name] = layers
    return out


def _is_pending_restart(setting: "Setting", value: str) -> bool:
    """Stored value differs from the one this process started with.

    False when there is nothing to compare: a setting that applies live is never pending, and a
    process that never called apply_boot has no record of what it started with. Reporting a change
    on missing evidence would put an alarm on every deployment that imports this module.
    """
    if setting.applies != "restart":
        return False
    if setting.key not in _BOOT_EFFECTIVE:
        return False
    return str(value) != _BOOT_EFFECTIVE[setting.key]


def pending_restart_keys() -> List[str]:
    """Settings written since startup and not yet in effect.

    The same question `snapshot()` answers per field, without building the catalogue to ask it: the
    live strip asks this every couple of seconds on every open page, and that document costs most
    of a frame on its own.
    """
    document = load()
    values: Dict[str, str] = {k: str(v) for k, v in (document.get("values") or {}).items()}
    waiting: List[str] = []
    for setting in SETTINGS:
        if setting.applies != "restart" or setting.key not in _BOOT_EFFECTIVE:
            continue
        if _effective(setting, values)[0] != _BOOT_EFFECTIVE[setting.key]:
            waiting.append(setting.key)
    return sorted(waiting)


def snapshot(include_catalog: bool = True) -> Json:
    """The full settable-config view for the portal: effective value, where it came from, and when
    a change takes effect. Secret VALUES are never present -- only ``configured``."""
    document = load()
    values: Dict[str, str] = {k: str(v) for k, v in (document.get("values") or {}).items()}
    override_layers = _override_layers_by_env()
    # Whether what is on disk is actually in force. Without this a corrupt file renders exactly
    # like a deployment nobody has configured yet, and every value in it is being ignored.
    file_status = config_file_status()
    groups: Dict[str, List[Json]] = {}
    for setting in SETTINGS:
        value, source = _effective(setting, values)
        entry: Json = {
            "key": setting.key,
            "label": setting.label,
            "kind": setting.kind,
            "env": _env_name(setting, values),
            "applies": setting.applies,
            "help": setting.help,
            "choices": list(setting.choices),
            "source": source if (value or setting.secret) else "default",
            "essential": setting.key in ESSENTIAL_KEYS,
            # The built-in default travels with every field so the portal can offer "reset to
            # default" and show what a value is being changed FROM -- without that, a customer
            # editing a tuning knob has no way back short of remembering the number.
            "default": setting.default,
            # Which layers can override this for a tenant or a person. Empty for a setting that
            # only exists at the deployment level. What is set here is the fallback, and a field
            # that does not say so reads as the final answer.
            "overridable_by": override_layers.get(_env_name(setting, values), []),
            # Whether the launcher already set this variable before the process started.
            #
            # apply_boot deliberately does not overwrite a variable the operator's launcher set, so
            # a change made here applies immediately and is then LOST at the next restart, when the
            # launcher's value is used again and the stored one is skipped. That is the documented
            # precedence and it is not wrong -- it is just invisible, and invisible at exactly the
            # wrong moment: the "environment" source badge disappears the instant a customer
            # overrides the value, which is when they most need to know.
            #
            # Reported for every setting whose variable is in the boot environment, regardless of
            # what the value currently is or where it came from.
            "boot_pinned": bool(_env_name(setting, values)
                                and _env_name(setting, values) in _BOOT_ENV),
            # Changed since this process started, and frozen at import, so the deployment is still
            # running the old value. Distinct from the `applies` badge, which says what KIND of
            # setting this is and says it before anybody touches anything.
            "pending_restart": _is_pending_restart(setting, value),
            # Offered here, resolved by the policy, and read by nothing in this build. A control
            # that accepts a value and confirms it is indistinguishable from one that works.
            "read_by_nothing": not setting_is_read(_env_name(setting, values)),
            # Whether the FILE holds a value for this key, which is not the same question as
            # whether the value differs from the default. The page inferred one from the other
            # and hid its reset control for every setting stored at the build's own default --
            # the only control that removes such an entry, missing from every field that has one.
            "stored": setting.key in values,
            # A variable the portal does NOT offer, winning over this field right now. Without
            # it the page shows a value the deployment is not using and an editable control that
            # changes nothing for the configured provider.
            "overridden_by": unoffered_override(setting.key, values),
        }
        if setting.secret:
            entry["configured"] = bool(value)
            entry["value"] = None
        else:
            entry["value"] = value
        groups.setdefault(setting.group, []).append(entry)
    result: Json = {
        "status": "ok",
        "essential_keys": sorted(ESSENTIAL_KEYS),
        # Which settings have been written since this process started and are still not in effect.
        # The write that made each of them said so once, to whoever made it; this says it to
        # whoever looks next.
        "pending_restart": sorted(entry["key"] for group in groups.values() for entry in group
                                  if entry.get("pending_restart")),
        # In the file and not in this build: renamed or dropped since it was written, still
        # sitting there, doing nothing. Reported rather than removed -- deleting a customer's
        # stored values because this build does not recognise them is the wrong answer to a
        # downgrade or a partial rollout.
        "unknown_stored": sorted(set(values) - set(SETTINGS_BY_KEY)),
        "config_file": config_path(),
        "updated_at": document.get("updated_at"),
        "updated_by": document.get("updated_by"),
        "groups": groups,
        "history": history(),
        "group_meta": {
            name: dict(meta, count=len(groups.get(name, [])))
            for name, meta in GROUPS.items() if name in groups
        },
        "inventory": inventory(),
        # Whether the file on disk is in force. `absent` is the ordinary state of a deployment
        # nobody has configured and reports applied; the other states mean every value stored
        # there is being ignored while the deployment runs built-in defaults.
        "config_file_status": file_status,
    }
    if include_catalog:
        result["presets"] = {
            name: {"label": preset["label"], "note": preset["note"], "values": preset["values"]}
            for name, preset in PRESETS.items()
        }
    return result


class UnknownSetting(ValueError):
    pass


class InvalidValue(ValueError):
    pass


def _coerce(setting: Setting, raw: Any) -> str:
    """Validate at the boundary. A knob that reaches the provider code as a non-number would raise
    deep inside an ingest, where it reads as a backend fault rather than a bad setting.

    JSON ``null`` means RESET: the stored override is dropped and the variable unset, so the
    built-in default takes over. That is different from ``""``, which stores an explicit empty
    value -- the distinction matters for a knob whose empty string is itself meaningful."""
    if raw is None:
        return ""
    if setting.kind == "bool":
        if isinstance(raw, bool):
            return "1" if raw else "0"
        return "1" if str(raw).strip().lower() in _TRUTHY else "0"
    text = str(raw).strip()
    if setting.kind == "int" and text:
        try:
            int(text)
        except ValueError:
            raise InvalidValue(f"{setting.key} must be an integer, got {text!r}")
    if setting.kind == "float" and text:
        try:
            float(text)
        except ValueError:
            raise InvalidValue(f"{setting.key} must be a number, got {text!r}")
    if setting.choices and text and text not in setting.choices:
        raise InvalidValue(
            f"{setting.key} must be one of {', '.join(setting.choices)}, got {text!r}")
    return text


def update(patch: Json, actor: Optional[str] = None) -> Json:
    """Write settings. Unknown keys are refused, so this is not a general env-var setter.

    Applies to the live process environment unconditionally (a portal write is a deliberate action
    taken now) and persists so a restart keeps it. The result reports, per key, whether the change
    is already in effect or waits on a restart -- see the module docstring for why that distinction
    is not cosmetic.
    """
    if not isinstance(patch, dict):
        raise InvalidValue("body must be a JSON object of setting keys")
    unknown = [k for k in patch if k not in SETTINGS_BY_KEY]
    if unknown:
        raise UnknownSetting("unknown setting(s): " + ", ".join(sorted(unknown)))

    document = load()
    values: Dict[str, str] = {k: str(v) for k, v in (document.get("values") or {}).items()}
    # What the FILE held before this write. Not the same as the effective values below: storing a
    # value identical to the build default changes the file and not the effective value, and that
    # is a real change -- it pins the setting against a later build. So "did anything change" is
    # asked of this, and "changed from what" is answered by `previous`.
    stored_before: Dict[str, str] = dict(values)
    # The effective values BEFORE this write, so the log can say what each setting changed from --
    # including a value that came from the launcher environment rather than a previous write.
    previous: Dict[str, str] = {}
    for setting in SETTINGS:
        if not setting.secret:
            previous[setting.key] = _effective(setting, values)[0]

    coerced: Dict[str, str] = {}
    reset: List[str] = []
    for key, raw in patch.items():
        if raw is None:
            reset.append(key)
            coerced[key] = ""
        else:
            coerced[key] = _coerce(SETTINGS_BY_KEY[key], raw)
            # The one thing that makes assignment into the environment raise. Refused here, with
            # the other value checks, so that neither half of the write can fail once it starts.
            if "\x00" in coerced[key]:
                raise InvalidValue("%s must not contain a NUL byte" % key)
    # The *.api_key_env settings decide where the secrets land, so fold the whole patch in before
    # resolving any target name -- otherwise a single call that sets both the key and its variable
    # name would write the key to the OLD variable.
    values.update(coerced)

    applied: List[Json] = []
    # What the environment would become, worked out but not yet done. The write to disk happens
    # first: if it fails, the running process must not already be carrying settings the operator
    # has just been told were not saved.
    env_ops: List[Tuple[str, Optional[str]]] = []
    for key in sorted(coerced):
        setting = SETTINGS_BY_KEY[key]
        value = coerced[key]
        name = _env_name(setting, values)
        if name:
            env_ops.append((name, None if value == "" else value))
        if key == "extraction.provider":
            env_ops.append(("MATRIXARK_EXTRACTION_PROVIDER", value or None))
        # A value the engine will raise is reported back with what it will really be. The
        # write is still accepted -- the engine takes it and raises it, and refusing here would be
        # this file inventing a stricter rule than the thing it configures.
        raised_to = None
        floor = engine_minimum(name) if name else None
        if floor is not None and value not in ("", None):
            try:
                if int(str(value).strip()) < floor:
                    raised_to = floor
            except (TypeError, ValueError):
                raised_to = None
        applied.append({
            "key": key, "env": name, "applies": setting.applies,
            "raised_to": raised_to,
            "in_effect": setting.applies == "live",
            "secret": setting.secret,
        })

    for key in reset:
        values.pop(key, None)
    # What changed, and from what. "Who changed the embedding model, and when" is a real support
    # question, and until now the file recorded only the latest state -- which answers neither.
    # Secrets are recorded by KEY only: the fact that a key was rotated is exactly the useful part,
    # and the value is exactly the part that must not be written down.
    changes: List[Json] = []
    for key in sorted(coerced):
        setting = SETTINGS_BY_KEY[key]
        # Nothing moved in the file, so nothing is recorded. A caller re-POSTing the values it
        # already holds is common -- a periodic reconciler, a page saving a form nobody edited --
        # and each such write used to append a full entry with `from` equal to `to`. On the
        # deployment this was found on, 45 of the 50 retained entries recorded no change at all,
        # 254 of 269 individual changes had from == to, and the window they had pushed out was
        # down to eleven hours. The log exists to answer "who changed the embedding model, and
        # when"; it had evicted the answer with writes that changed nothing.
        #
        # A SECRET is exempt: its value is never compared, so re-setting one is indistinguishable
        # from rotating it, and the fact of the write is the useful half.
        if not setting.secret:
            if key in reset:
                if key not in stored_before:
                    continue
            elif stored_before.get(key) == coerced[key]:
                continue
        entry: Json = {"key": key, "env": _env_name(setting, values)}
        if key in reset:
            entry["action"] = "reset"
        elif setting.secret:
            entry["action"] = "set"
            entry["secret"] = True
        else:
            entry["action"] = "set"
            entry["from"] = str(previous.get(key, ""))[:_HISTORY_VALUE_MAX]
            entry["to"] = coerced[key][:_HISTORY_VALUE_MAX]
        changes.append(entry)

    history = document.get("history")
    history = history if isinstance(history, list) else []
    # An entry with no changes in it is the same noise one level up.
    if changes:
        history.append({
            "at": time.time(),
            "by": actor or "portal",
            "changes": changes,
            "restart_required": sorted({e["key"] for e in applied if not e["in_effect"]}),
        })
    document["history"] = history[-HISTORY_LIMIT:]

    document["values"] = values
    document["updated_at"] = time.time()
    document["updated_by"] = actor or "portal"
    _store(document)

    # Only now. _store is the half that can fail; assignment into the environment is the half that
    # cannot be undone, and it has been checked for the one input that would make it raise.
    for name, value in env_ops:
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value

    restart_required = sorted({e["key"] for e in applied if not e["in_effect"]})
    return {
        "status": "ok",
        "applied": applied,
        "reset": sorted(reset),
        "restart_required": restart_required,
        "config_file": config_path(),
        "updated_at": document["updated_at"],
    }


def history(limit: int = 20) -> List[Json]:
    """The most recent configuration writes, newest first. Never contains a secret VALUE."""
    document = load()
    entries = document.get("history")
    entries = entries if isinstance(entries, list) else []
    return list(reversed(entries))[:max(1, min(int(limit), HISTORY_LIMIT))]


def export_settings(include_defaults: bool = False) -> Json:
    """The settings as a patch body: `{"settings": {...}}`, ready to POST at another deployment.

    Secrets are omitted, never blanked: a blank would be a WRITE that clears the target's working
    key, so an import that "just applied everything" would silently break the deployment it was
    meant to configure. The result names the secrets it left out so the operator knows what is
    still theirs to set.
    """
    document = load()
    stored: Dict[str, str] = {k: str(v) for k, v in (document.get("values") or {}).items()}
    settings: Dict[str, str] = {}
    omitted: List[str] = []
    for setting in SETTINGS:
        if setting.secret:
            value, _source = _effective(setting, stored)
            if value:
                omitted.append(setting.key)
            continue
        value, source = _effective(setting, stored)
        if source == "default" and not include_defaults:
            continue
        settings[setting.key] = value
    return {
        "status": "ok",
        "exported_at": time.time(),
        "settings": settings,
        "secrets_omitted": sorted(omitted),
        "note": "POST this document to /v1/admin/config on another deployment. Secrets are never "
                "exported; set them there afterwards.",
    }


def apply_preset(name: str, actor: Optional[str] = None) -> Json:
    preset = PRESETS.get(name)
    if preset is None:
        raise UnknownSetting(f"unknown preset {name!r}; known: {', '.join(sorted(PRESETS))}")
    result = update(dict(preset["values"]), actor=actor)
    result["preset"] = name
    result["note"] = preset["note"]
    return result


# ================================================================================================
# Live probes
# ================================================================================================
# Reading configuration back proves only that it was stored. These call the endpoints as configured
# and report what actually happened, which is the only way to tell a working DeepSeek key from one
# that is present and rejected -- both of which look identical in the snapshot, and both of which
# degrade to the deterministic path at ingest time without an error the caller ever sees.
def _post_json(url: str, payload: Json, headers: Dict[str, str], timeout: float) -> Tuple[int, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Content-Type": "application/json", **headers})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # nosec B310 - operator URL
        raw = response.read()
        try:
            return response.getcode(), json.loads(raw.decode("utf-8"))
        except ValueError:
            return response.getcode(), raw[:512].decode("utf-8", "replace")


# A probe is only worth anything if it exercises the path the deployment really uses, so these
# mirror the dispatch sets in the provider code -- matrixark_mcp_core for extraction,
# _API_EMBEDDING_PROVIDERS in matrixark_mcp_embeddings for the encoder -- and a test parses those
# modules and fails if they drift. Anything outside them reaches no endpoint at all: "local" is not
# a local server, it is the token-hash fallback, and probing an endpoint the encoder never calls
# reported a green connection test on a deployment running entirely on hash vectors.
_OPENAI_EXTRACTION_PROVIDERS = {"openai", "openai_compatible", "openai_compatible_llm"}
_ANTHROPIC_EXTRACTION_PROVIDERS = {"anthropic", "claude", "anthropic_messages"}
_API_EMBEDDING_PROVIDERS = {"openai", "openai_compatible", "openai-compatible", "azure_openai",
                            "voyage", "api"}


# The encoder also has an in-process branch that loads a model instead of calling one. It is not
# an endpoint, so the probe skips it -- but it produces REAL embeddings, so it is not the hash
# fallback either, and the two must not be reported as the same thing.
_OSS_EMBEDDING_PROVIDERS = {"oss", "open_source", "sentence_transformers", "sentence-transformers"}

# Names a customer sets on purpose to get the local rules. Everything else that ends up there ended
# up there by accident.
_DELIBERATE_FALLBACK_PROVIDERS = {"", "deterministic", "rules", "local", "hash"}


def embedding_provider_effect(provider: str) -> str:
    """What the ENCODER does with this name: "api", "local_model" or "hash".

    Three places asked this separately and each answered with the same hand-written literal set,
    which caught "local" and nothing else. A name the encoder does not recognise -- a typo, or a
    provider that simply is not supported -- falls through to the hash fallback exactly as
    "deterministic" does, and none of the three knew it.
    """
    name = (provider or "").strip().lower()
    if name in _API_EMBEDDING_PROVIDERS:
        return "api"
    if name in _OSS_EMBEDDING_PROVIDERS:
        return "local_model"
    return "hash"


def extraction_provider_effect(provider: str) -> str:
    """What EXTRACTION does with this name: "openai", "anthropic" or "rules", matching the dispatch
    in matrixark_mcp_core -- where an unrecognised name falls past both branches to the same
    rule-based path a customer gets by asking for it."""
    name = (provider or "").strip().lower()
    if name in _OPENAI_EXTRACTION_PROVIDERS:
        return "openai"
    if name in _ANTHROPIC_EXTRACTION_PROVIDERS:
        return "anthropic"
    return "rules"


def provider_is_unrecognised(group: str, provider: str) -> bool:
    """True when this name yields the fallback WITHOUT anyone having asked for it.

    The distinction is the whole point. "deterministic" is a choice, and the portal should explain
    it. "openai_compatibl" is a mistake, and the portal should say so and name the value -- it used
    to report that deployment as a working API encoder.
    """
    name = (provider or "").strip().lower()
    if name in _DELIBERATE_FALLBACK_PROVIDERS:
        return False
    if group == "embedding":
        return embedding_provider_effect(name) == "hash"
    return extraction_provider_effect(name) == "rules"


def recognised_providers(group: str) -> List[str]:
    """What to offer someone whose value matched nothing, so the message can be acted on."""
    if group == "embedding":
        return sorted(_API_EMBEDDING_PROVIDERS | _OSS_EMBEDDING_PROVIDERS)
    return sorted(_OPENAI_EXTRACTION_PROVIDERS | _ANTHROPIC_EXTRACTION_PROVIDERS)


def _probe(kind: str, url: str, payload: Json, headers: Dict[str, str], timeout: float) -> Json:
    started = time.time()
    result: Json = {"target": kind, "url": url, "ok": False}
    try:
        status, parsed = _post_json(url, payload, headers, timeout)
        result["status"] = status
        result["ok"] = 200 <= status < 300
        result["response"] = (parsed if isinstance(parsed, str)
                              else _summarize(kind, parsed, payload.get("model")))
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read()[:512].decode("utf-8", "replace")
        except Exception:  # pragma: no cover - body already consumed/closed
            detail = ""
        result["status"] = exc.code
        result["error"] = f"HTTP {exc.code}"
        result["detail"] = detail
    except Exception as exc:
        result["error"] = exc.__class__.__name__
        result["detail"] = str(exc)[:512]
    result["latency_ms"] = round((time.time() - started) * 1000.0, 1)
    return result


def _with_match(summary: Json, requested: Any) -> Json:
    """Add what was asked for, and whether the answer matches it.

    Absent rather than False when either side is unknown: a provider that returns no model name
    has not disagreed with anything, and saying it did would put a warning on every endpoint that
    is merely terse.
    """
    served = summary.get("model")
    summary["requested_model"] = requested or None
    if requested and served:
        summary["model_matches_request"] = _same_model(requested, served)
    return summary


def _same_model(asked: Any, served: Any) -> bool:
    """Whether two model names denote the same model.

    An organisation prefix is not a difference: `sentence-transformers/paraphrase-multilingual-
    MiniLM-L12-v2` and `paraphrase-multilingual-MiniLM-L12-v2` are one model under two spellings,
    and the shipped local_minilm preset uses the short one while the encoder catalogue lists the
    long one. Reporting that pair as a mismatch would be a false alarm on a supported setup, and
    a warning that cries wolf on the shipped configuration is one nobody reads.
    """
    left = str(asked or "").strip().lower().rsplit("/", 1)[-1]
    right = str(served or "").strip().lower().rsplit("/", 1)[-1]
    return left == right


def _summarize(kind: str, parsed: Any, requested: Any = None) -> Json:
    """Keep only what an operator needs from a provider response -- never the whole body, which for
    an embedding probe is a full vector.

    `requested` is carried in so the answer can be compared with the question. Reporting only the
    model that came back reads as confirmation that the endpoint serves it, and against a server
    that echoes the request that confirmation is of the operator's own input.
    """
    if not isinstance(parsed, dict):
        return {"raw": str(parsed)[:200]}
    if kind == "embedding":
        data = parsed.get("data")
        vector = data[0].get("embedding") if isinstance(data, list) and data and isinstance(data[0], dict) else None
        return _with_match({
            "model": parsed.get("model"),
            "dimensions": len(vector) if isinstance(vector, list) else None,
        }, requested)
    if kind == "extraction_anthropic":
        # Messages API: the reply is a list of content blocks, not a choices list.
        blocks = parsed.get("content")
        text = "".join(str(b.get("text", "")) for b in blocks
                       if isinstance(b, dict) and b.get("type", "text") == "text") \
            if isinstance(blocks, list) else ""
        return _with_match({"model": parsed.get("model"), "sample": text[:120]}, requested)
    choices = parsed.get("choices")
    text = ""
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        message = choices[0].get("message") or {}
        text = str(message.get("content", ""))[:120]
    return _with_match({"model": parsed.get("model"), "sample": text}, requested)


def probe(targets: Optional[List[str]] = None, timeout: float = 10.0) -> Json:
    """Call the configured extraction and/or embedding endpoints once and report what came back."""
    document = load()
    values: Dict[str, str] = {k: str(v) for k, v in (document.get("values") or {}).items()}
    wanted = set(targets or ["extraction", "embedding"])
    results: List[Json] = []

    if "extraction" in wanted:
        provider = (os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER")
                    or os.environ.get("MATRIXARK_EXTRACTION_PROVIDER") or "deterministic").strip()
        base = os.environ.get("MATRIXARK_EXTRACTION_BASE_URL", "").strip().rstrip("/")
        model = os.environ.get("MATRIXARK_EXTRACTION_MODEL", "").strip()
        key_env = _env_name(SETTINGS_BY_KEY["extraction.api_key"], values)
        key = os.environ.get(key_env, "")
        if provider in _ANTHROPIC_EXTRACTION_PROVIDERS:
            # Anthropic reads its own base URL and model, and authenticates with x-api-key rather
            # than a bearer token. Probing it as though it were OpenAI-compatible asked for a base
            # URL and model this provider never reads, so the test told a customer to fill in two
            # fields that would not have changed anything.
            anthropic_base = os.environ.get(
                "MATRIXARK_ANTHROPIC_API_BASE", "https://api.anthropic.com").strip().rstrip("/")
            anthropic_model = os.environ.get("MATRIXARK_ANTHROPIC_MODEL", "").strip()
            version = os.environ.get("MATRIXARK_ANTHROPIC_VERSION", "2023-06-01")
            if not key:
                results.append({"target": "extraction", "ok": False, "skipped": True,
                                "error": "no_api_key",
                                "detail": "The Anthropic provider needs a key. It is read from "
                                          + key_env + "."})
            else:
                results.append(_probe(
                    "extraction_anthropic", f"{anthropic_base}/v1/messages",
                    {"model": anthropic_model or extraction_model_default("anthropic"),
                     "max_tokens": 1, "messages": [{"role": "user", "content": "ping"}]},
                    {"x-api-key": key, "anthropic-version": version}, timeout))
        elif provider not in _OPENAI_EXTRACTION_PROVIDERS:
            results.append({"target": "extraction", "ok": False, "skipped": True,
                            "error": "provider_is_deterministic",
                            "detail": "No model is called on this setting, so there is nothing to "
                                      "probe. Set the extraction provider to openai_compatible."})
        else:
            # No refusal for an unset endpoint. The extraction call has no such guard -- it posts to
            # the build default -- so refusing here described a deployment that does not exist and
            # withheld the one result that would have shown where the calls are really going.
            base = base or str(SETTINGS_BY_KEY["extraction.base_url"].default).rstrip("/")
            model = model or str(SETTINGS_BY_KEY["extraction.model"].default)
            headers = {"Authorization": f"Bearer {key}"} if key else {}
            results.append(_probe(
                "extraction", f"{base}/chat/completions",
                {"model": model, "messages": [{"role": "user", "content": "ping"}],
                 "max_tokens": 1, "temperature": 0.0},
                headers, timeout))

    if "embedding" in wanted:
        provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip()
        base = (os.environ.get("MATRIXARK_EMBEDDING_API_BASE", "").strip()
                or os.environ.get("MATRIXARK_EMBED_BASE_URL", "").strip()).rstrip("/")
        model = os.environ.get("MATRIXARK_EMBEDDING_MODEL", "").strip()
        key_env = _env_name(SETTINGS_BY_KEY["embedding.api_key"], values)
        key = os.environ.get(key_env, "")
        if provider not in _API_EMBEDDING_PROVIDERS:
            # "local" lands here, and that is the point: it is not a local server, it is the
            # token-hash fallback. Probing the base URL anyway returned ok for a deployment whose
            # encoder was never called.
            results.append({"target": "embedding", "ok": False, "skipped": True,
                            "error": "provider_is_deterministic",
                            "detail": "The encoder makes no call on this setting -- retrieval is "
                                      "running on hash vectors, whatever the base URL points at. "
                                      "Set the embedding provider to openai_compatible or voyage."})
        elif not base:
            results.append({"target": "embedding", "ok": False, "skipped": True,
                            "error": "incomplete_config",
                            "detail": "Set the embedding base URL first."})
        else:
            headers = {"Authorization": f"Bearer {key}"} if key else {}
            results.append(_probe(
                "embedding", f"{base}/embeddings",
                {"model": model or "unknown", "input": "matrixark configuration probe"},
                headers, timeout))

    return {
        "status": "ok",
        "results": results,
        "all_ok": bool(results) and all(r.get("ok") for r in results),
    }

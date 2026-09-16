#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""What a retrieve actually applies, readable without importing the retrieve path.

Every value here was already decided somewhere; what was missing was a way to ASK. The retrieve
path lives in ``matrixark_local_adapter_retrieval``, which is a mixin split out of
``matrixark_mcp_local_adapter`` and imports back from it -- a deliberate cycle, and one that
cannot be entered from outside. ``import matrixark_local_adapter_retrieval`` in a fresh process
raises ``ImportError: cannot import name '_LocalAdapterRetrievalMixin' from partially initialized
module``.

So every surface that wanted to report what retrieval does re-derived it instead, and each
derivation was wrong in its own way:

* **The metric** ``matrixark_gateway_onebox_embedding_first`` re-implemented the boolean parse as
  ``value in ("1", "true", "yes", "on")``. The serving path's rule is the opposite shape -- ON
  unless the value is one of ``0 false no off ""``. The two agree on the eight words both list and
  disagree on everything else, so ``MATRIXARK_ONEBOX_EMBEDDING_FIRST=disabled`` serves dense-only
  scoring while the dashboard reports it off, confirming the operator in the belief that they
  turned it off. Measured over a 16-value sample: 5 disagreed.
* **The one-box portal page** asked the settings registry, which is a different question again --
  "is there a knob by this name", not "what does a retrieve apply". When the registry stopped
  offering those knobs the page's answer changed from a wrong number to no number, and the caps
  went on cutting exactly as before.

The fix is not a better copy. It is that there is one function, and the serving path and every
surface reporting on it call that same one. A surface can then be wrong only by not calling it,
which is visible, rather than by parsing differently, which is not.

Nothing here imports the adapter, the retrieve path, or the gateway, and nothing here writes: the
point is to be importable from anywhere, including from inside the cycle.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

try:  # package path
    from tools.matrixark_mcp_runtime_config import (
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_TOP_K_PER_LAYER,
    )
except ImportError:  # direct execution from tools/
    from matrixark_mcp_runtime_config import (  # noqa: F401
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_TOP_K_PER_LAYER,
    )


def flag_enabled(name: str, default: str = "0") -> bool:
    """Read a boolean environment switch.

    ON unless the value spells one of the off words. Note the asymmetry, because it is the reason
    the metric and this disagreed: an unrecognised value such as ``enabled`` or ``2`` reads as ON,
    not as OFF and not as an error. That is the rule the serving path has always applied; it is
    reproduced here rather than corrected, because correcting it would change what is served to
    deployments that set an unrecognised value. What is fixed here is that only one copy runs.
    """
    return os.environ.get(name, default).strip().lower() not in {"0", "false", "no", "off", ""}


# ON by default. One-box deployments serve the ranking from the embeddings: scoring is dense-only,
# which is the half that is measured and safe. Set MATRIXARK_ONEBOX_EMBEDDING_FIRST=0 for the
# hybrid weights instead.
#
# What the default rests on, stated so it can be re-checked rather than assumed: over 269 queries
# against this repo's own markdown with multilingual-e5-large, the query sentence deleted from its
# own target so no verbatim span survives, hit@1 moved by -0.011 with a 95% interval of
# [-0.049, +0.027]. That interval contains zero -- the difference is indistinguishable from none at
# this sample size, which is not the same as proven absent. It buys 4.11 MB held -> 2.65 MB and 21x
# less scoring cost.
ONEBOX_EMBEDDING_FIRST_DEFAULT = "1"


def onebox_embedding_first() -> bool:
    """Read at call time, never captured at import.

    A module-level constant freezes the flag at whichever moment the module first loaded. Under
    the full suite that is decided by import order, so the same test passes alone and fails in
    the suite; in a deployment it means the profile cannot be changed without a restart.
    """
    return flag_enabled("MATRIXARK_ONEBOX_EMBEDDING_FIRST", ONEBOX_EMBEDDING_FIRST_DEFAULT)


def retrieval_scan_projection() -> bool:
    """True when the scan should carry only the fields it reads. OPT-IN, and gated on the profile.

    The dependency runs one way -- narrowing the row is only safe once the lexical term that reads
    the text is gone -- and it was once written as though it ran both ways. The row the scan
    carries is the row the pack is built from, so every field the projection drops is a field the
    answer cannot print; it shipped dropping the text and retrieval returned an empty string for
    every hit. It stays off until the field list is derived from what the serving path actually
    reads at runtime, rather than extended one failing test at a time.
    """
    return onebox_embedding_first() and flag_enabled("MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS")


def tenant_retrieval_limit(name: str, scope: Any, fallback: int) -> int:
    """A retrieval budget: an explicit tenant override, else an explicit env var, else this build.

    Deliberately NOT the knob registry's ``resolve()``. That returns the registry's default when
    nobody has set anything, and for these budgets the registry disagreed with what retrieval
    actually uses by up to 156x. Wiring to it would read as "the knob works now" while silently
    multiplying the budget for every deployment that never configured one.

    Both explicit levels are read per call, so a change at either applies with no restart. With
    nothing set the build default is used exactly as before, so no existing deployment moves.

    Anything unexpected -- no policy module, a non-numeric value, a nonsensical zero -- falls back
    to the build default: a budget that came out empty would return nothing at all, which is worse
    than ignoring a bad setting.
    """
    return tenant_retrieval_limit_with_source(name, scope, fallback)[0]


def tenant_retrieval_limit_with_source(name: str, scope: Any, fallback: int) -> tuple:
    """The budget, with the level that supplied it: (value, "tenant"|"env"|"default").

    The same three words ``describe_effective_policy`` uses, so the one-box page can render a cap
    and a policy knob through one map instead of translating between two vocabularies.

    Asked of the function that decides, never worked out here. A tenant override beats the
    environment variable, and a tenant policy carrying a zero or a non-number for the knob is
    ignored and falls through to it -- so "the policy names this knob" and "the policy supplied
    this value" are different questions, and only the resolver can answer the second.

    Falls back to ``default`` for the source whenever the value fell back too, so a surface never
    shows a level that did not actually answer.
    """
    try:
        from matrixark_tenant_policy import explicit_int_with_source
    except Exception:  # pragma: no cover - policy module absent
        return fallback, "default"
    try:
        return explicit_int_with_source(name, scope, fallback)
    except Exception:  # pragma: no cover - a malformed policy must not break retrieval
        return fallback, "default"


# The three caps a reader asks about, each with the build default it falls back to. Ordered as
# somebody reads them: how wide each layer is scored, how many candidates survive to ranking, how
# many refs reach the answer.
RETRIEVAL_CAPS = (
    ("top_k_per_layer", DEFAULT_TOP_K_PER_LAYER,
     "How many candidates are scored at each layer of the tree."),
    ("max_global_candidates", DEFAULT_MAX_GLOBAL_CANDIDATES,
     "How many candidates survive the scan and reach ranking."),
    ("max_selected_refs", DEFAULT_MAX_SELECTED_REFS,
     "How many refs ranking may put in the answer. Measured on this build, this is the one that "
     "does the cutting: raising the other two changed nothing."),
)

# The cap the measurement blamed, so a surface can say so beside the row rather than in prose the
# reader has to connect to it themselves.
THE_CAP_THAT_CUTS = "max_selected_refs"


def _cap(name: str, scope: Any, fallback: int, help_text: str) -> Dict[str, Any]:
    value, source = tenant_retrieval_limit_with_source(name, scope, fallback)
    return {
        "name": name,
        "value": value,
        "build_default": fallback,
        "env": "MATRIXARK_" + name.upper(),
        # Which level answered. Without it the page can name the variable beside a value the
        # variable did not supply: a tenant override wins, so an operator can set that variable,
        # watch nothing change, and have nothing on the page to explain why.
        "source": source,
        "cuts": name == THE_CAP_THAT_CUTS,
        "help": help_text,
    }


def effective_retrieval(scope: Optional[Any] = None) -> Dict[str, Any]:
    """Everything a surface needs to say what this deployment does when it answers a question.

    ``scope`` is the tenant scope a retrieve would run under, or None for the deployment-wide
    values -- the same argument the serving path passes, so a per-tenant override appears here
    exactly where it would apply.
    """
    dense_only = onebox_embedding_first()
    return {
        "profile": {
            "embedding_first": dense_only,
            "env": "MATRIXARK_ONEBOX_EMBEDDING_FIRST",
            "default_is_on": ONEBOX_EMBEDDING_FIRST_DEFAULT == "1",
            "dense_weight": 1.00 if dense_only else 0.72,
            "lexical_weight": 0.00 if dense_only else 0.28,
            "scan_projection": retrieval_scan_projection(),
        },
        "caps": [
            _cap(name, scope, fallback, help_text)
            for name, fallback, help_text in RETRIEVAL_CAPS
        ],
    }

#!/usr/bin/env python3
# examples/mem0/python_sdk_example.py
"""mem0-compatible Python SDK example (drop-in replacement for mem0's Memory).

Talks to the MatrixArk HTTP gateway. In dev mode the API key is optional.
    pip install matrixark            # (SDK; today: import matrixark_mem0_compat)
"""
from matrixark_mem0_compat import Memory   # future: `from matrixark import Memory`

m = Memory(
    base_url="https://api.saparo.ai",      # your gateway
    api_key="mk_live_...",                 # optional in dev mode
)

# ── INGESTION ──────────────────────────────────────────────────────────────
# add(messages, user_id=..., agent_id=..., run_id=..., metadata=...) -> {"results":[{"id","event"}]}
res = m.add(
    [{"role": "user", "content": "I'm vegetarian and allergic to peanuts"}],
    user_id="alice",          # the subject (required)
    run_id="s1",              # conversation/session (optional; mem0 run_id)
    agent_id="support-bot",   # which agent (optional; mem0 agent_id)
)
mid = res["results"][0]["id"]

# MatrixArk extensions (optional): TTL + keyed-upsert
m.add([{"role": "user", "content": "Use gate B12 today"}], user_id="alice", ttl=86400)      # auto-expires
m.add([{"role": "user", "content": "Preferred seat: aisle"}], user_id="alice",
      identity_key="pref.seat", truth_class="asserted")                                     # keyed-upsert

# ── RETRIEVAL ──────────────────────────────────────────────────────────────
hits = m.search("dietary restrictions", user_id="alice", limit=10)   # {"results":[{id,memory,score,metadata}]}
for r in hits["results"]:
    print(r["memory"], r["score"])
one = m.get(mid)                       # fetch one memory by id
everything = m.get_all(user_id="alice")  # list all live memories for the user

# ── UPDATE / HISTORY ───────────────────────────────────────────────────────
m.update(mid, "I'm vegan now, still allergic to peanuts")   # supersedes; re-extracts derived memory
m.history(mid)                                              # [ingested, superseded, ...]

# ── DELETE / FORGET / RESET ────────────────────────────────────────────────
m.delete(mid)                 # remove one memory (cascades to its derived entities/embeddings)
m.delete_all(user_id="alice") # forget the subject (all of alice's memory)
m.reset()                     # wipe the whole tenant  (mem0 reset)

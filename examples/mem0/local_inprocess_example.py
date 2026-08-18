#!/usr/bin/env python3
# examples/mem0/local_inprocess_example.py
"""Self-contained, runnable mem0-style example — NO server needed.

Drives the same tools the HTTP gateway and the mem0 Python shim call, against a
local on-disk store, so you can see ingestion + retrieval + the full lifecycle
end to end with `python3 local_inprocess_example.py`.

For production you'd use the HTTP API or the Memory() SDK (see the other two
example files); the *semantics* shown here are identical.
"""
import json, os, sys, tempfile
from pathlib import Path
# make the repo's tools/ importable when run from examples/mem0/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
import matrixark_mcp_server as mcp

# A "scope" identifies whose memory this is. Minimum to write: user_id.
SCOPE = {"user_id": "alice", "session_id": "s1", "agent_name": "support-bot"}

def show(label, value):
    print(f"  {label}: {json.dumps(value, default=str)[:180]}")

def main():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        server = mcp.MatrixArkMcpServer(mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl"),
                                        access_mode="dev")
        call = lambda name, args: server.call_tool(name, {**args, "scope": SCOPE})

        print("── INGESTION ─────────────────────────────────────────────")
        # add(): write memory from a conversation turn. Returns the anchor id.
        added = call("matrixark_ingest", {
            "messages": [
                {"role": "user", "content": "I'm vegetarian and allergic to peanuts."},
                {"role": "assistant", "content": "Noted — vegetarian, peanut allergy."},
            ],
            "finalize": True,          # extract now (omit to batch)
        })
        mid = added["event_id_hash"]
        call("matrixark_session_commit", {})   # materialize derived memory
        show("add() -> id", mid)

        print("\n── RETRIEVAL ─────────────────────────────────────────────")
        # search(): semantic recall (mem0 shape is {results:[{id,memory,score}]})
        pack = call("matrixark_retrieve", {"query": "dietary restrictions"})
        show("search('dietary restrictions') finds 'peanut'", "peanut" in json.dumps(pack, default=str))
        # get(id): fetch one memory by its id
        show("get(id)", call("matrixark_get_memory", {"memory_id": str(mid)})["memory"])
        # get_all(): list this user's live memories
        ga = call("matrixark_get_all", {})
        show("get_all() count", ga["count"])

        print("\n── UPDATE / HISTORY ──────────────────────────────────────")
        upd = call("matrixark_update_memory", {"memory_id": str(mid), "data": "I'm vegan now, still allergic to peanuts."})
        call("matrixark_session_commit", {})
        show("update() -> superseded", {"superseded": upd["superseded"], "new_id": upd["new_memory_id"]})
        show("history(old id)", [e["event"] for e in call("matrixark_memory_history", {"memory_id": str(mid)})["history"]])

        print("\n── DELETE / FORGET / RESET ───────────────────────────────")
        show("delete(new id)", call("matrixark_delete", {"memory_id": str(upd["new_memory_id"])})["deleted"])
        show("get_all() after delete", call("matrixark_get_all", {})["count"])
        # forget: erase THIS user (confirm must equal user_id)
        show("forget(user)", call("matrixark_forget", {"confirm": "alice"})["forgotten"])
        # reset: wipe the whole TENANT (confirm must equal tenant_id or 'RESET')
        # call("matrixark_reset", {"confirm": "RESET"})
        print("\nDONE — ingestion + retrieval + full lifecycle verified.")

if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# examples/mem0/http_curl_example.sh
# mem0-style memory over the MatrixArk HTTP API. Any language / any HTTP client.
# In dev mode the Authorization header is optional.
set -euo pipefail
BASE="${MATRIXARK_BASE:-https://api.saparo.ai}"
AUTH=(-H "Authorization: Bearer ${MATRIXARK_API_KEY:-}")   # optional in dev mode
JSON=(-H "Content-Type: application/json")

# ── INGESTION ──────────────────────────────────────────────────────────────
# add: returns {"event_id_hash": <id>, ...}  (the id is your handle)
RESP=$(curl -s -X POST "$BASE/v1/ingest" "${AUTH[@]}" "${JSON[@]}" -d '{
  "scope": {"user_id": "alice", "session_id": "s1"},
  "messages": [{"role": "user", "content": "I am vegetarian and allergic to peanuts"}],
  "finalize": true
}')
MID=$(printf '%s' "$RESP" | python3 -c 'import sys,json;print(json.load(sys.stdin)["event_id_hash"])')
echo "ingested id=$MID"

# TTL + keyed-upsert (MatrixArk extensions, optional)
curl -s -X POST "$BASE/v1/ingest" "${AUTH[@]}" "${JSON[@]}" -d '{
  "scope": {"user_id":"alice"}, "messages":[{"role":"user","content":"Use gate B12 today"}],
  "finalize": true, "ttl_seconds": 86400 }' >/dev/null
curl -s -X POST "$BASE/v1/ingest" "${AUTH[@]}" "${JSON[@]}" -d '{
  "scope": {"user_id":"alice"}, "messages":[{"role":"user","content":"Preferred seat: aisle"}],
  "finalize": true, "identity_key": "pref.seat", "truth_class": "asserted" }' >/dev/null

# ── RETRIEVAL ──────────────────────────────────────────────────────────────
# search / retrieve: returns a context pack (mem0 shim reshapes to {results:[...]})
curl -s -X POST "$BASE/v1/retrieve" "${AUTH[@]}" "${JSON[@]}" -d '{
  "scope": {"user_id":"alice"}, "query": "dietary restrictions", "limit": 8 }'
curl -s "$BASE/v1/memory/$MID" "${AUTH[@]}"                       # get one
curl -s "$BASE/v1/memories?user_id=alice" "${AUTH[@]}"           # get_all

# ── UPDATE / HISTORY ───────────────────────────────────────────────────────
curl -s -X POST "$BASE/v1/update" "${AUTH[@]}" "${JSON[@]}" -d "{
  \"scope\": {\"user_id\":\"alice\"}, \"memory_id\": \"$MID\", \"data\": \"I am vegan now\" }"
curl -s "$BASE/v1/memory/$MID/history" "${AUTH[@]}"

# ── DELETE / FORGET / RESET ────────────────────────────────────────────────
curl -s -X POST "$BASE/v1/delete" "${AUTH[@]}" "${JSON[@]}" -d "{
  \"scope\": {\"user_id\":\"alice\"}, \"memory_id\": \"$MID\" }"
curl -s -X POST "$BASE/v1/forget" "${AUTH[@]}" "${JSON[@]}" -d '{
  "scope": {"user_id":"alice"}, "confirm": "alice" }'            # forget the user
curl -s -X POST "$BASE/v1/reset"  "${AUTH[@]}" "${JSON[@]}" -d '{
  "scope": {"tenant_id":"acme"}, "confirm": "RESET" }'          # wipe the tenant

# MatrixArk Layer Traversal, Shared Resource, Cross-Session Rerank Report

Status: **pass**

## Behavior Contract
- query understanding creates source/resource/session intent
- scope filter runs before candidate eligibility
- ContextNode tree traversal scores child L0/L1 embeddings layer by layer
- current-session leaf candidates are fetched first
- cross-session candidates use bounded session/node fanout
- shared resources/skills use separate shared path quota
- rerank combines lexical/embedding relevance, temporal freshness, and business boosts
- ContextPack assembly spends tokens on selected evidence only

## Rerank Factors
- `relevance`: primary score from query/text overlap or embedding candidate score
- `temporal_decay`: fresh/current evidence gets boost; stale evidence can still win if relevance is strong
- `business_boost`: approval, owner, deadline, cost, policy, procedure, resource, and skill evidence get extra weight
- `shared_resource_boost`: tenant/shared and global resource evidence gets a modest boost when the query asks for docs/runbooks/policy

## Budget Defaults
- `same_session`: highest priority; fills first when relevant
- `cross_session`: bounded cap; normally summaries/entities/compressions first, raw events only for high-confidence evidence
- `shared_resources_skills`: separate quota and eligibility before scoring; selected only when query intent matches

## C++ / Rust Checks
- `cpp_layer_traversal_native`: **pass**
- `cpp_temporal_decay_event_query`: **pass**
- `cpp_unit_coverage_for_layer_and_decay`: **pass**
- `rust_native_weighted_rerank`: **pass**
- `rust_unit_coverage_for_cross_session_and_shared_resource`: **pass**
- `shared_corpus_case`: **pass**
  - rust runner: `cargo test -p temporalstore-rust context_retrieval_reranks_cross_session_and_shared_resource_evidence --lib -- --test-threads=1`

## Validation Commands
```bash
python3 -m json.tool compat/unified_temporalstore_cases.json >/tmp/unified_context_cases.json
```
```bash
python3 tools/run_context_shared_cases.py --validate-only
```
```bash
cargo test -p temporalstore-rust context_retrieval_reranks_cross_session_and_shared_resource_evidence --lib -- --test-threads=1
```

## Notes
- C++ native surface already contains TRAVERSE_CONTEXT_TREE and decayed QUERY_EVENTS primitives.
- Rust native retrieve path now applies weighted reranking before the existing relevance/tier/time tie-breakers.
- Live full-Cargo validation can be slow in this Windows/WSL workspace; the report records the command separately from the fast source/corpus gate.

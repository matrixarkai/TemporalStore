# Context Injection Prompt Pack Contract

This is the shared C++/Rust contract for `context_injection_prompt_pack_ordering`.

The case validates:

- retrieval ranks the current/latest memory before stale conflicting memory
- prompt injection preserves the retrieval order inside `<context>`
- `ContextPackAudit.selected_refs` preserves the same selected evidence order
- stale evidence can still be included after the current evidence when the token budget permits it

Rust validates this with:

```bash
cargo test -p temporalstore-rust context_injection_prompt_pack_preserves_retrieved_evidence_ordering --lib -- --test-threads=1
```

C++ should run the same current-versus-stale memory scenario and compare injected prompt order plus
pack-audit selected refs.

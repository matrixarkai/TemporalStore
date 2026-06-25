# Context OpenViking Parity Cases

This page is the shared C++/Rust contract for the
`context_openviking_reasoning_vlm_parity` corpus case. It complements the broader Context workflow
manual by naming the OpenViking/VikingMem-style reasoning categories that both implementations
should expose and score.

Rust exposes these cases through `context_workflow_state_report().openviking_parity_cases` and
validates them with:

```bash
cargo test -p temporalstore-rust context_openviking_reasoning_vlm_cases_cover_required_gaps --lib -- --test-threads=1
```

C++ should mirror the same case matrix in its MatrixArk/VikingMem Context pipeline tests.

| Category | Required evidence |
| --- | --- |
| `multi_hop_reasoning` | A query must connect at least two memories, for example who suggested a project and which project was chosen. |
| `temporal` | A query must prefer the later timestamped fact over an older fact. |
| `memory_update` | A query must retrieve the updated value after a correction or new score. |
| `stale_memory` | A query must suppress an older conflicting memory in favor of the latest one. |
| `open_domain_retrieval` | A query must retrieve social/open-domain evidence such as recommendations or introductions. |
| `vlm_image_content_understanding` | A VLM-derived memory shape must exist with merchant/total or equivalent image-content facts. |

The VLM case is currently a configuration and retrieval-shape proof. It is not production benchmark
evidence until a real local OSS VLM or OpenAI-compatible VLM gateway is run and its report is
archived. Until then, Rust reports `vlm_provider_configured=true` and `vlm_benchmark_proven=false`.

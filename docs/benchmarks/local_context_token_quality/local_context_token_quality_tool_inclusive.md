# Local Context vs TemporalStore-Managed Context: Token & Quality Sweep

Generated `2026-08-09T08:08:00.462817+00:00` | reader `ollama:qwen2.5:1.5b` | judge `ollama:qwen2.5:1.5b` | tag `tool_inclusive`

## Ingestion coverage (all local context -> Context model)

- Codex: **1296** events across **38** sessions
- Claude: **2800** events across **25** sessions
- Total events: **4096**; segments **437**; entities **15**; summaries **64**
- Full replay tokens (all events): **661347**; baseline fed to reader: **194981** (905 events)

## Headline

- Queries where managed pack matched/beat full-context quality: **6/6**
- Avg baseline tokens fed: **194981.0** -> avg minimal managed pack: **999.333**
- **Avg token saving at equal-or-better quality: 99.5%**
- Avg quality (0-10): baseline **8.46** vs managed **9.313**

## Per-query minimal-token result

| Query | Baseline tokens | Baseline score | Min pack tokens | Managed score | Token saving |
|---|---:|---:|---:|---:|---:|
| q_hooks | 194981 | 8.666 | 1000 | 8.666 | 99.5% |
| q_parity | 194981 | 8.0 | 999 | 8.5 | 99.5% |
| q_windows | 194981 | 8.0 | 1000 | 9.666 | 99.5% |
| q_context | 194981 | 8.666 | 999 | 9.334 | 99.5% |
| q_bench | 194981 | 9.428 | 999 | 9.714 | 99.5% |
| q_storage | 194981 | 8.0 | 999 | 10.0 | 99.5% |

## Full sweep (per query, per budget)

### q_hooks: What did the user decide about Rust TemporalStore hooks and real-time Codex prompt ingestion?

Baseline (fed 194981 tok) score 8.666 — judge 10/10, coverage 0.333

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 24000 | 23999 | 17 | 2/2 | 0.0 | 1.6 | 87.7% |
| 16000 | 16000 | 13 | 10/10 | 0.833 | 9.666 | 91.8% |
| 8000 | 8000 | 7 | 10/10 | 0.5 | 9.0 | 95.9% |
| 4000 | 4000 | 7 | 10/10 | 0.667 | 9.334 | 97.9% |
| 2000 | 1999 | 7 | 10/10 | 0.167 | 8.334 | 99.0% |
| 1000 | 1000 | 6 | 10/10 | 0.333 | 8.666 | 99.5% |

### q_parity: What did the user want regarding C++ vs Rust TemporalStore parity and fair performance comparison?

Baseline (fed 194981 tok) score 8.0 — judge 10/10, coverage 0.0

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 24000 | 24000 | 11 | 10/10 | 0.375 | 8.75 | 87.7% |
| 16000 | 16000 | 8 | 10/10 | 0.5 | 9.0 | 91.8% |
| 8000 | 7999 | 5 | 10/10 | 0.25 | 8.5 | 95.9% |
| 4000 | 4000 | 4 | 10/10 | 0.25 | 8.5 | 97.9% |
| 2000 | 1999 | 4 | 10/10 | 0.25 | 8.5 | 99.0% |
| 1000 | 999 | 5 | 10/10 | 0.25 | 8.5 | 99.5% |

### q_windows: How should Windows users install TemporalStore, and what about WSL and Docker?

Baseline (fed 194981 tok) score 8.0 — judge 10/10, coverage 0.0

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 24000 | 24000 | 29 | 10/10 | 0.333 | 8.666 | 87.7% |
| 16000 | 16000 | 23 | 10/10 | 0.333 | 8.666 | 91.8% |
| 8000 | 7999 | 11 | 10/10 | 0.667 | 9.334 | 95.9% |
| 4000 | 3999 | 10 | 10/10 | 0.667 | 9.334 | 97.9% |
| 2000 | 1998 | 8 | 10/10 | 0.667 | 9.334 | 99.0% |
| 1000 | 1000 | 6 | 10/10 | 0.833 | 9.666 | 99.5% |

### q_context: What did the user ask about context management data fields, compaction, and backfill?

Baseline (fed 194981 tok) score 8.666 — judge 10/10, coverage 0.333

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 24000 | 23999 | 17 | 10/10 | 0.333 | 8.666 | 87.7% |
| 16000 | 16000 | 13 | 2/2 | 0.167 | 1.934 | 91.8% |
| 8000 | 7998 | 5 | 10/10 | 0.667 | 9.334 | 95.9% |
| 4000 | 4000 | 5 | 10/10 | 0.667 | 9.334 | 97.9% |
| 2000 | 1999 | 5 | 10/10 | 0.667 | 9.334 | 99.0% |
| 1000 | 999 | 5 | 10/10 | 0.667 | 9.334 | 99.5% |

### q_bench: What did the user ask about Qwen, Ollama, vLLM, LoCoMo, LongMemEval, and OSS benchmarking?

Baseline (fed 194981 tok) score 9.428 — judge 10/10, coverage 0.714

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 24000 | 23998 | 22 | 10/10 | 0.0 | 8.0 | 87.7% |
| 16000 | 15998 | 11 | 10/10 | 1.0 | 10.0 | 91.8% |
| 8000 | 7998 | 6 | 10/10 | 0.857 | 9.714 | 95.9% |
| 4000 | 3999 | 5 | 10/10 | 0.857 | 9.714 | 97.9% |
| 2000 | 1999 | 5 | 10/10 | 0.857 | 9.714 | 99.0% |
| 1000 | 999 | 5 | 10/10 | 0.857 | 9.714 | 99.5% |

### q_storage: What did the user ask about MatrixObject, shared storage, S3, and object storage?

Baseline (fed 194981 tok) score 8.0 — judge 10/10, coverage 0.0

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 24000 | 23999 | 14 | 10/10 | 0.0 | 8.0 | 87.7% |
| 16000 | 15999 | 9 | 10/10 | 1.0 | 10.0 | 91.8% |
| 8000 | 7998 | 5 | 10/10 | 1.0 | 10.0 | 95.9% |
| 4000 | 3999 | 5 | 10/10 | 1.0 | 10.0 | 97.9% |
| 2000 | 1999 | 5 | 10/10 | 1.0 | 10.0 | 99.0% |
| 1000 | 999 | 5 | 10/10 | 1.0 | 10.0 | 99.5% |


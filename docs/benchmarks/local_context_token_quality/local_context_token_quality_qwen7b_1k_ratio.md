# Local Context vs TemporalStore-Managed Context: Token & Quality Sweep

Generated `2026-08-09T12:02:10.876077+00:00` | reader `ollama:qwen2.5:7b` | judge `ollama:qwen2.5:7b` | tag `qwen7b_1k_ratio`

## Ingestion coverage (all local context -> Context model)

- Codex: **4336** events across **72** sessions
- Claude: **8048** events across **25** sessions
- Total events: **12384**; segments **1293**; entities **15**; summaries **98**
- Full replay tokens (all events): **1698940**; baseline fed to reader: **7879** (31 events)

## Headline

- Queries where managed pack matched/beat full-context quality: **6/6**
- Avg baseline tokens fed: **7879.0** -> avg minimal managed pack: **1333.333**
- **Avg token saving at equal-or-better quality: 83.067%**
- Avg quality (0-10): baseline **6.592** vs managed **8.297**

## Per-query minimal-token result

| Query | Baseline tokens | Baseline score | Min pack tokens | Managed score | Token saving |
|---|---:|---:|---:|---:|---:|
| q_hooks | 7879 | 6.05 | 1000 | 6.584 | 87.3% |
| q_parity | 7879 | 5.25 | 1000 | 8.05 | 87.3% |
| q_windows | 7879 | 5.25 | 1000 | 6.45 | 87.3% |
| q_context | 7879 | 8.666 | 2000 | 8.984 | 74.6% |
| q_bench | 7879 | 8.286 | 1000 | 9.714 | 87.3% |
| q_storage | 7879 | 6.05 | 2000 | 10.0 | 74.6% |

## Full sweep (per query, per budget)

### q_hooks: What did the user decide about Rust TemporalStore hooks and real-time Codex prompt ingestion?

Baseline (fed 7879 tok) score 6.05 — judge 8/7, coverage 0.0

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 4000 | 4000 | 10 | 9/8 | 0.667 | 8.184 | 49.2% |
| 2000 | 2000 | 10 | 8/7 | 0.667 | 7.384 | 74.6% |
| 1000 | 1000 | 11 | 7/6 | 0.667 | 6.584 | 87.3% |

### q_parity: What did the user want regarding C++ vs Rust TemporalStore parity and fair performance comparison?

Baseline (fed 7879 tok) score 5.25 — judge 7/6, coverage 0.0

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 4000 | 4000 | 11 | 8/7 | 0.5 | 7.05 | 49.2% |
| 2000 | 2000 | 9 | 8/7 | 1.0 | 8.05 | 74.6% |
| 1000 | 1000 | 8 | 8/7 | 1.0 | 8.05 | 87.3% |

### q_windows: How should Windows users install TemporalStore, and what about WSL and Docker?

Baseline (fed 7879 tok) score 5.25 — judge 7/6, coverage 0.0

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 4000 | 4000 | 17 | 7/6 | 1.0 | 7.25 | 49.2% |
| 2000 | 2000 | 12 | 7/6 | 1.0 | 7.25 | 74.6% |
| 1000 | 1000 | 6 | 6/5 | 1.0 | 6.45 | 87.3% |

### q_context: What did the user ask about context management data fields, compaction, and backfill?

Baseline (fed 7879 tok) score 8.666 — judge 10/10, coverage 0.333

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 4000 | 3999 | 15 | 8/7 | 0.667 | 7.384 | 49.2% |
| 2000 | 2000 | 11 | 10/9 | 0.667 | 8.984 | 74.6% |
| 1000 | 998 | 12 | 0/2 | 1.0 | 2.7 | 87.3% |

### q_bench: What did the user ask about Qwen, Ollama, vLLM, LoCoMo, LongMemEval, and OSS benchmarking?

Baseline (fed 7879 tok) score 8.286 — judge 10/10, coverage 0.143

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 4000 | 4000 | 14 | 6/4 | 0.857 | 5.814 | 49.2% |
| 2000 | 2000 | 10 | 10/10 | 0.857 | 9.714 | 74.6% |
| 1000 | 1000 | 11 | 10/10 | 0.857 | 9.714 | 87.3% |

### q_storage: What did the user ask about MatrixObject, shared storage, S3, and object storage?

Baseline (fed 7879 tok) score 6.05 — judge 8/7, coverage 0.0

| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |
|---:|---:|---:|---|---:|---:|---:|
| 4000 | 4000 | 11 | 5/3 | 0.4 | 4.1 | 49.2% |
| 2000 | 2000 | 9 | 10/10 | 1.0 | 10.0 | 74.6% |
| 1000 | 1000 | 9 | 5/3 | 1.0 | 5.3 | 87.3% |


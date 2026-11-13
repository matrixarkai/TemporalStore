# Context Model Source Prompts

Summary field instructions belong in the model's system turn. The source turn is
serialized source data: its body, title, kind and richness hint. Batches add an ordinal
for mapping outputs to sources. Storage identifiers stay outside model input; the
original requests still supply every persisted source identifier and link.

Separating the turns avoids presenting field definitions as source content and removes
repeated batch instructions. Output schemas require nonempty summary and entity
strings. Minified JSON reduces formatting tokens without removing fields. Entity
types, source isolation checks, maximum output bounds and provider deadlines remain
unchanged. Embedding models and vector spaces are unchanged.

Independent sources use separate Ollama summary requests by default. A source can
contain an entire conversation: its messages are still synthesized together. This
prevents a second source's facts from entering the model input for the first source.
Embedding requests remain batched. A transport failure stops further summary calls
for that window instead of repeating an unavailable endpoint for every source.
Isolated calls share the window's provider time budget: each request receives the
remaining time, and unstarted sources fail when that time is exhausted. Configured
HTTP retry behavior is unchanged. A large source window is not a promise that a local
model can process it synchronously within that budget.

`TEMPORALSTORE_CONTEXT_SUMMARY_MAX_BATCH_SOURCES` explicitly enables multi-source
model prompts when greater than one. Measure source-attribution quality for the chosen
model before using that mode; schema-valid output alone cannot prove correct attribution.

The live Ollama check verifies that concrete source facts survive in summaries and
entities, not merely that the provider returned valid JSON. It also covers separate
sources with distinct facts and a source containing multiple independent constraints.
Run it with an installed chat model:

```bash
TEMPORALSTORE_CONTEXT_SUMMARY_MAX_BATCH_SOURCES=1 \
MATRIXARK_REQUIRE_MODEL_SUMMARIES=1 TS_TEST_OLLAMA_CHAT_MODEL=qwen2.5:7b \
  cargo test -p temporalstore-rust --lib \
  live_ollama_summary_keeps_durable_facts_in_entities -- --ignored --nocapture --test-threads=1
```

This small quality check is not a large-corpus latency or throughput result.

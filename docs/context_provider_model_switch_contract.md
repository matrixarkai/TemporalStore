# Context Provider And Model Switch Contract

This is the shared C++/Rust contract for `context_openviking_blocks_provider_switches`.

The case validates that a Context ingest/extract batch can mix:

- the VikingMem parity reader profile using `gpt-4o-mini`
- the legacy MatrixArk/C++ open-source text profile using `google/flan-t5-small`
- the OpenViking-style VLM profile using `Vision-CAIR/MiniGPT-4`
- provider/model accounting in ingest summaries
- OpenViking-style L2 context blocks for both text and VLM-shaped memories

Rust validates this with:

```bash
cargo test -p temporalstore-rust context_openviking_blocks_and_provider_model_switches_are_reported --lib -- --test-threads=1
```

C++ should expose the same contract by proving that provider switches preserve the selected model,
embedding model, VLM model, source kind, and retrieved Context block text.

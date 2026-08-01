# Goal: Codex ↔ Claude Hook Parity (ingestion / extraction / retrieval)

## Objective

Both the Codex hook and the Claude Code hook capture the full conversation —
**user prompts and LLM responses** — into the **rust TemporalStore** backend, and
retrieve prior context on the next prompt, with identical behavior across the two
agents (differing only by agent identity / session scope). Works on both WSL/Linux
and native Windows.

## Acceptance criteria

1. **User prompts** are ingested for both agents on `UserPromptSubmit`.
2. **LLM responses** are ingested for both agents on `Stop` (Claude Code sends the
   assistant text as `last_assistant_message`; Codex via rollout / `assistant_message`).
   The clean assistant text is stored — not the raw hook JSON blob.
3. **Extraction** (batch commit) runs on turn boundaries for both agents.
4. **Retrieval** on the next `UserPromptSubmit` returns prior user + assistant
   context, injected as Claude Code `hookSpecificOutput.additionalContext`.
5. Everything is persisted to the **rust TemporalStore** backend
   (`temporalstore-rust` / local `TemporalEngine`).
6. **Both agents are installable** on WSL/Linux (`install.sh`) and native Windows
   (`install.ps1`) with the full lifecycle.
7. A parity test asserts (1)-(5) for `codex` and `claude` and that result shapes match.

## Work items

- [x] W1. Claude hook wrapper + settings + WSL installer + docs (initial parity).
- [x] W2. Rust engine agent-generic (scope key) + surface retrieved additionalContext.
- [x] W3. Universal adapter renders additionalContext + emits hookSpecificOutput.
- [x] W4. **LLM-response capture**: assistant/response keys added to rust `payload_text`
        and python `payload_text`; `Stop` stores the clean LLM response for both agents
        (verified: no raw-JSON-blob fallback). Includes UTF-8 BOM stripping (Windows stdin).
- [x] W5. **install.ps1**: full Claude Code lifecycle (native Windows via `wsl.exe`),
        `-Agent both` installs codex + claude; `install.sh` also gained `--agent both`.
        Invocations call `bash <wrapper>` so a missing exec bit can't break them.
- [x] W6. Parity test covers LLM-response ingestion on `Stop` (`last_assistant_message`)
        for both agents.
- [x] W7. Verified end-to-end on WSL and native Windows: user prompt + LLM response →
        rust TemporalStore → retrieved and injected on the next prompt.

## Status

COMPLETE. Gates: `tools/test_matrixark_popular_agent_hooks.py` (5/5) and the rust bin
tests (4/4). Integration reference: `docs/matrixark_claude_hook_integration.md`.

# TemporalStore Agent Hooks

Cross-platform agent hook integration for MatrixArk context memory backed by
TemporalStore. This package starts in-tree so the install flow, schemas, and
smoke tests are versioned with TemporalStore. It is designed to be split into a
standalone `temporalstore-agent-hooks` repository once stable.

## What This Installs

- A portable Node.js hook launcher for Codex, Claude, and other agent runtimes.
- Codex plugin templates modeled after the OpenViking plugin layout.
- Claude settings templates.
- Shared event normalization and session resolution.
- Windows PowerShell and Unix shell installers.
- Smoke tests that prove a hook can normalize a message and call the configured
  TemporalStore backend.

## Layout

```text
integrations/agent-hooks/
  README.md
  DESIGN.md
  VERIFICATION.md
  config/
    temporalstore-agent.example.env
  shared/
    agent_event_schema.json
    context_weighting_policy.json
  codex/
    plugin/
      .codex-plugin/plugin.json
      hooks/hooks.template.json
      hooks/hooks.json
      scripts/
        payload_normalizer.mjs
        session_resolver.mjs
        temporalstore_client.mjs
        temporalstore_hook_launcher.mjs
  claude/
    settings.example.json
  install/
    install.ps1
    install.sh
    smoke_test.ps1
    smoke_test.sh
```

## Modes

```text
wsl      Windows host, TemporalStore repo and Python hook run inside WSL.
native   Linux/macOS host, TemporalStore repo and Python hook run locally.
remote   Any host, launcher calls a hosted TemporalStore/MatrixArk HTTP API.
dry-run  Normalize and print the event without calling TemporalStore.
```

## Quick Start

Windows with WSL:

```powershell
.\integrations\agent-hooks\install\install.ps1 -Agent codex -Mode wsl
.\integrations\agent-hooks\install\smoke_test.ps1 -Mode dry-run
```

Linux/macOS native:

```bash
bash ./integrations/agent-hooks/install/install.sh --agent codex --mode native
bash ./integrations/agent-hooks/install/smoke_test.sh --mode dry-run
```

Remote TemporalStore:

```bash
TEMPORALSTORE_AGENT_MODE=remote \
TEMPORALSTORE_AGENT_ENDPOINT=https://temporalstore.example.com \
bash ./integrations/agent-hooks/install/smoke_test.sh
```

## Required Runtime

The hook launcher requires Node.js 18 or newer. Codex desktop normally provides
a bundled Node runtime; installers can use `CODEX_NODE_PATH` or fall back to
`node` on `PATH`.

## Hook Lifecycle

Recommended lifecycle mapping:

```text
SessionStart      optional session metadata and recovery
UserPromptSubmit  ingest user prompt and retrieve context
PostToolUse       ingest tool result
Stop              ingest assistant answer / commit turn
PreCompact        force session commit before compaction
```

## Safety

Installers only write agent integration files. They do not mutate the
TemporalStore source tree outside `integrations/agent-hooks/`, and smoke tests
default to `dry-run` unless a backend mode is selected.

# Security Policy

## Supported versions

TemporalStore is pre-1.0. Security fixes target the latest `main` and the most recent
published `0.x` release.

## Reporting a vulnerability

Please report suspected vulnerabilities **privately** via GitHub Security Advisories
— use **"Report a vulnerability"** on the Security tab of
<https://github.com/matrixarkai/TemporalStore/security/advisories/new> — rather than
opening a public issue or pull request.

Include, if possible:

- a description of the issue and its impact,
- the affected version or commit,
- steps to reproduce or a proof of concept.

We aim to acknowledge reports within a few business days and will coordinate a fix
and a disclosure timeline with you.

## Scope

Because TemporalStore is the durable store under a serving engine, **defects that
cost or misrepresent committed data are treated as security issues, not merely
bugs**: a write acknowledged and then lost, a recovery that does not reproduce what
the log committed, an index that outlives or misnames the pages it points at, a read
served stale data as fresh, or a dump or index image that installs over live state
without being refused. The same applies to an unbounded resident structure that a
request can grow without limit, because availability of the store is part of its
contract.

Defects in a deployment's own operating decisions are out of scope, but a
configuration this project ships as a **default** is in scope: if a default is
unsafe, the default is the bug.

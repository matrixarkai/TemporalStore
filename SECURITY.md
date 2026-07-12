# Security Policy

## Supported Versions

Security fixes target the active `rust-main` branch.

## Reporting A Vulnerability

Please report suspected vulnerabilities through GitHub private vulnerability
reporting or a private maintainer channel for this repository. Do not open a
public issue with exploit details. Do not open a public issue with exploit
details until maintainers have coordinated disclosure.

Please include:

- affected component or crate
- reproduction steps
- impact and exploitability notes
- whether credentials, private data, or service availability are affected

## Secrets And Credentials

Do not commit API keys, tokens, TLS private keys, cloud credentials, local model
gateway secrets, benchmark dataset credentials, or generated service configs
containing secrets.

Common environment variables used by tests and local runs include
`OPENAI_API_KEY`, `OPENVIKING_MODEL_API_KEY`, `MATRIXARK_MODEL_API_KEY`,
`TS_RAFT_AUTH_TOKEN`, and TLS certificate paths. They must stay outside git.

## Security Boundaries

Rust TemporalStore currently treats brpc/thrift compatibility and live
MatrixObjectStore/S3 production integration as out of scope unless explicitly re-added.
Security review for those integrations is required before claiming production
readiness.

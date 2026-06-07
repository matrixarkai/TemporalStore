# Workspace AWS And Packaging Helpers

These scripts were promoted from the local workspace into source control so deployment, package staging, AWS diagnostics, and comparison runs are repeatable.

They intentionally do not include runtime binaries, static libraries, shared libraries, archives, generated build outputs, third-party dependencies, credentials, or local logs.

Most scripts assume the same one-cluster AWS topology used during the MatrixArk test runs: one meta/client/UI node and two data nodes. Override paths, instance ids, and artifact locations with environment variables before running them in a different environment.

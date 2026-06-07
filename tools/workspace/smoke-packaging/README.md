# Smoke Packaging Helpers

This folder keeps the local smoke-package scripts and safe SSM command templates
that were used to build, stage, install, and validate TemporalStore/MatrixKV
runtime and client artifacts on the shared AWS test cluster.

Included files are source scripts or reusable command templates only. Do not add
runtime archives, presigned URL files, binary outputs, client logs, release/debug
directories, or generated build trees here.

The SSM templates that embed presigned URLs are intentionally excluded because
they contain temporary AWS security tokens.

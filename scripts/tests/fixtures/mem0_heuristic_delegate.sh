#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
set -euo pipefail
python3 - "$@" <<'PY'
import json
import os
import sys

arguments = sys.argv[1:]
assert arguments.count('--provider-json') == 1
provider = json.loads(arguments[arguments.index('--provider-json') + 1])
assert provider['provider_kind'] == 'mock' and provider['mock_mode'] is True
assert provider['base_url'] == '' and provider['fallback_provider'] is None
assert provider['embedding_model'] == 'mock-context-embedding'
assert json.loads(os.environ['TS_MEM0_PROVIDER_JSON']) == provider
for name in ('TEMPORALSTORE_CONTEXT_PROVIDER', 'MATRIXARK_CONTEXT_PROVIDER'):
    assert os.environ[name] == 'mock'
for name in ('TEMPORALSTORE_EMBEDDING_API_BASE', 'MATRIXARK_EMBEDDING_API_BASE', 'MATRIXARK_EMBED_BASE_URL'):
    assert os.environ[name] == ''
for name in ('TEMPORALSTORE_EMBEDDING_MODEL', 'MATRIXARK_EMBEDDING_MODEL'):
    assert os.environ[name] == provider['embedding_model']
for name in ('MATRIXARK_REQUIRE_MODEL_SUMMARIES', 'MATRIXARK_REQUIRE_MODEL_EMBEDDINGS'):
    assert os.environ[name] == 'false'
assert arguments.count('--infer') == 1
assert arguments[arguments.index('--infer') + 1] == os.environ.get('EXPECTED_INFER', 'false')
assert arguments[arguments.index('--work-dir') + 1] == 'directory with spaces'
print('delegate-ok')
sys.exit(int(os.environ.get('DELEGATE_EXIT', '0')))
PY

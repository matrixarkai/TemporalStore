#!/usr/bin/env bash
set -euo pipefail
cd /home/vj/tslink
cmake --build build-ubuntu22/release --parallel 4 --target bcache2-shared client_example proxy_smoke_example customer_client_example customer_c_client_example > /tmp/temporalstore-release-client-build.log 2>&1
status=$?
echo $status > /tmp/temporalstore-release-client-build.status
tail -120 /tmp/temporalstore-release-client-build.log
find output-ubuntu22/release build-ubuntu22/release -maxdepth 5 -type f \( -name 'libbcache2.so' -o -name 'client_example' -o -name 'proxy_smoke_example' -o -name 'customer_client_example' -o -name 'customer_c_client_example' \) -exec ls -lh {} \;

#!/bin/bash

set -e
set -o pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

apt update
apt install -y libunwind-dev python3-pip libnuma-dev

export PATH=/usr/local/tao/agent/modules/bvc/bin:$PATH

set +e
pkill "nosetests"
set -e

${SCRIPT_DIR}/run_onebox.sh remote ./test_chaos.py:test_chaos >/dev/null

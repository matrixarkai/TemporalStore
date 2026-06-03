#!/bin/bash

set -ex

cd $(dirname $0)

JOBS=$1

export ASAN_OPTIONS=detect_leaks=false,abort_on_error=true
cd build && make cpplint && make -j${JOBS}

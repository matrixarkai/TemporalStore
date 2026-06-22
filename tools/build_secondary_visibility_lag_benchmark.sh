#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:-/home/vj/tslink}
BUILD=${2:-"$ROOT/build-ubuntu22/release-mtcache-ssd"}
FLAGS="$BUILD/src/client/example/CMakeFiles/replication_smoke_example.dir/flags.make"
LINK="$BUILD/src/client/example/CMakeFiles/replication_smoke_example.dir/link.txt"
MANUAL="$BUILD/manual"

mkdir -p "$MANUAL"

CXX_DEFINES=$(sed -n 's/^CXX_DEFINES = //p' "$FLAGS")
CXX_INCLUDES=$(sed -n 's/^CXX_INCLUDES = //p' "$FLAGS")
CXX_FLAGS=$(sed -n 's/^CXX_FLAGS = //p' "$FLAGS")
CXX_DEFINES=${CXX_DEFINES//-DBCACHE2_VERSION=\\\"mtcache-ssd\\\"/-DBCACHE2_VERSION='"mtcache-ssd"'}

/usr/bin/c++ $CXX_DEFINES $CXX_INCLUDES $CXX_FLAGS \
    -MD -MT manual_secondary_visibility_lag_benchmark.o \
    -MF "$MANUAL/secondary_visibility_lag_benchmark.o.d" \
    -o "$MANUAL/secondary_visibility_lag_benchmark.o" \
    -c "$ROOT/src/client/example/secondary_visibility_lag_benchmark.cc"

python3 - "$LINK" "$MANUAL/secondary_visibility_lag_benchmark.o" "$MANUAL/secondary_visibility_lag_benchmark" "$MANUAL/secondary_visibility_lag_benchmark.link.sh" <<'PY'
import shlex
import sys
from pathlib import Path

link_path = Path(sys.argv[1])
new_obj = sys.argv[2]
out = sys.argv[3]
out_script = Path(sys.argv[4])
cmd = shlex.split(link_path.read_text())
cmd = [new_obj if part.endswith("replication_smoke_example.cc.o") else part for part in cmd]
cmd = [out if part == "replication_smoke_example" else part for part in cmd]
out_script.write_text(" ".join(shlex.quote(part) for part in cmd) + "\n")
PY

(
    cd "$BUILD/src/client/example"
    bash "$MANUAL/secondary_visibility_lag_benchmark.link.sh"
)

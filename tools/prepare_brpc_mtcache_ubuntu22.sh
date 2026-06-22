#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_LIB="${BRPC_STATIC_LIBRARY:?set BRPC_STATIC_LIBRARY to the existing libbrpc.a}"
OUT_DIR="${BRPC_MTCACHE_DIR:-${ROOT}/.local/ubuntu22/brpc-mtcache}"
OUT_LIB="${OUT_DIR}/lib/libbrpc.a"

mkdir -p "${OUT_DIR}/lib" "${OUT_DIR}/output"
cp -a "${SOURCE_LIB}" "${OUT_LIB}"

work_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${work_dir}"
}
trap cleanup EXIT

(
  cd "${work_dir}"
  ar x "${OUT_LIB}" logging.cc.o
  python3 - <<'PY'
from pathlib import Path
import subprocess

path = Path("logging.cc.o")

# MtCache links glog, which registers --v and --vmodule. The prebuilt BRPC
# archive was built with butil logging and registers the same gflags names.
# Patch only BRPC's string-literal sections in the copied archive; symbols such
# as logging::FLAGS_v are intentionally left unchanged for BRPC internals.
replacements = {
    b"v": b"V",
    b"vmodule": b"Vmodule",
    b"minloglevel": b"Minloglevel",
    b"logtostderr": b"Logtostderr",
    b"alsologtostderr": b"Alsologtostderr",
    b"colorlogtostderr": b"Colorlogtostderr",
    b"stderrthreshold": b"Stderrthreshold",
    b"log_dir": b"Log_dir",
    b"logbuflevel": b"Logbuflevel",
    b"logbufsecs": b"Logbufsecs",
    b"log_prefix": b"Log_prefix",
    b"max_log_size": b"Max_log_size",
    b"stop_logging_if_full_disk": b"Stop_logging_if_full_disk",
}
required_replacements = {b"v", b"vmodule", b"minloglevel"}
seen = {old: 0 for old in replacements}

def replace_c_string(data: bytes, old: bytes, new: bytes) -> tuple[bytes, int]:
    if len(old) != len(new):
        raise ValueError("replacement must preserve section size")
    needle = old + b"\x00"
    replacement = new + b"\x00"
    count = 0
    if data.startswith(needle):
        data = replacement + data[len(needle):]
        count += 1
    marker = b"\x00" + needle
    pos = 0
    while True:
        pos = data.find(marker, pos)
        if pos < 0:
            break
        data = data[:pos] + b"\x00" + replacement + data[pos + len(marker):]
        pos += len(marker)
        count += 1
    return data, count

sections = subprocess.check_output(["objdump", "-h", str(path)], text=True)
for line in sections.splitlines():
    fields = line.split()
    if len(fields) < 2:
        continue
    section = fields[1]
    if not section.startswith(".rodata"):
        continue

    dump = Path(f"{section.replace('/', '_')}.bin")
    updated = Path(f"{dump}.updated")
    try:
        subprocess.run(
            ["objcopy", f"--dump-section", f"{section}={dump}", str(path)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        continue

    data = dump.read_bytes()
    original = data
    for old, new in replacements.items():
        data, count = replace_c_string(data, old, new)
        seen[old] += count
    data, _ = replace_c_string(data, b"novmodule", b"NoVmodule")
    if data != original:
        updated.write_bytes(data)
        subprocess.run(
            ["objcopy", f"--update-section", f"{section}={updated}", str(path)],
            check=True,
        )

for old, count in seen.items():
    if old in required_replacements and count == 0:
        raise SystemExit(f"missing BRPC flag registration string: {old!r}")
PY
  ar rcs "${OUT_LIB}" logging.cc.o >/dev/null
  ranlib "${OUT_LIB}"
)

source_output_dir="$(cd "$(dirname "${SOURCE_LIB}")/.." && pwd)"
if [[ -d "${source_output_dir}/include" ]]; then
  ln -sfn "${source_output_dir}/include" "${OUT_DIR}/include"
  ln -sfn "${source_output_dir}/include" "${OUT_DIR}/output/include"
fi
ln -sfn "${OUT_DIR}/lib" "${OUT_DIR}/output/lib"

echo "${OUT_LIB}"

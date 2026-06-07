#!/usr/bin/env python3
from pathlib import Path

root = Path("/home/vj/bytekv-rocksdb-server")

flags = root / "bytekv/kvmaster/common/flags.cc"
text = flags.read_text()
text = text.replace(
    "DEFINE_uint32(\n    num_internal_table_shard, 512,",
    "DEFINE_uint32(\n    num_internal_table_shard, 16,",
)
flags.write_text(text)

cmake = root / "bytekv/kvmaster/CMakeLists.txt"
text = cmake.read_text()
extra = "target_link_libraries(kvmaster z)\ntarget_link_libraries(tso z)\n"
if extra not in text:
    text = text.replace(
        "endif()\n\nif(BYTEKV_BUILD_TESTS)",
        "endif()\n" + extra + "\nif(BYTEKV_BUILD_TESTS)",
    )
cmake.write_text(text)

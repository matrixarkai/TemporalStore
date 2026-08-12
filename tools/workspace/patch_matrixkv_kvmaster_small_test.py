#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from pathlib import Path

root = Path("/home/vj/matrixkv-rocksdb-server")

flags = root / "matrixkv/kvmaster/common/flags.cc"
text = flags.read_text()
text = text.replace(
    "DEFINE_uint32(\n    num_internal_table_shard, 512,",
    "DEFINE_uint32(\n    num_internal_table_shard, 16,",
)
flags.write_text(text)

cmake = root / "matrixkv/kvmaster/CMakeLists.txt"
text = cmake.read_text()
extra = "target_link_libraries(kvmaster z)\ntarget_link_libraries(tso z)\n"
if extra not in text:
    text = text.replace(
        "endif()\n\nif(MATRIXKV_BUILD_TESTS)",
        "endif()\n" + extra + "\nif(MATRIXKV_BUILD_TESTS)",
    )
cmake.write_text(text)

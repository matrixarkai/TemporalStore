set(name terarkdb-dev.1.4)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    DOWNLOAD_COMMAND ""
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR /home/vj/tslink/thirdparty/local_terarkdb_dev_2_0
    PATCH_COMMAND ""
    CMAKE_ARGS
      ${common_cmake_args}
      -DCMAKE_BUILD_TYPE=Release
      -DWITH_LZ4=ON
      -DWITH_BOOSTLIB=OFF
      -DWITH_JEMALLOC=OFF
      -DWITH_GFLAGS=OFF
      -DWITH_TESTS=OFF
      -DWITH_TOOLS=OFF
      -DWITH_TERARK_ZIP=OFF
      -DWITH_BYTEDANCE_METRICS=OFF
      -DWITH_ASAN=${ENABLE_ASAN}
      -DWITH_TERARKDB_NAMESPACE=rocksdb
      -DTERARK_INCLUDE_PREFIX=terarkdb
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(${name} clean
    EXCLUDE_FROM_MAIN TRUE
    ALWAYS TRUE
    DEPENDEES configure
    COMMAND make clean -j
    COMMAND rm -f ${BUILD_INFO_DIR}/${name}-build
    WORKING_DIRECTORY ${source_dir}
)

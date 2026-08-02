set(name libevent-2.1.12)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz
    URL_HASH MD5=b5333f021f880fe76490d8a799cd79f4
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
        ${common_configure_envs}
        ./configure ${common_configure_args}
                    --disable-shared
                    --enable-static
                    --disable-samples
                    --disable-libevent-regress
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

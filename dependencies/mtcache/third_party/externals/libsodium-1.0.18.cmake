set(name libsodium-1.0.18)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)

ExternalProject_Add(
    ${name}
    URL https://github.com/jedisct1/libsodium/releases/download/1.0.18-RELEASE/libsodium-1.0.18.tar.gz
    DOWNLOAD_NAME libsodium-1.0.18.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
        ${common_configure_envs}
        "LIBS=${LIBS}"
        ./configure ${common_configure_args}
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    INSTALL_COMMAND make -s -j${BUILDING_JOBS_NUM} install
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(${name} remove_so
    ALWAYS TRUE
    DEPENDEES install
    LOG TRUE
    COMMENT "rm -rf ${CMAKE_INSTALL_PREFIX}/lib/libsodium.so*"
    COMMAND rm -f ${CMAKE_INSTALL_PREFIX}/lib/libsodium.so
    COMMAND rm -f ${CMAKE_INSTALL_PREFIX}/lib/libsodium.so.23
    COMMAND rm -f ${CMAKE_INSTALL_PREFIX}/lib/libsodium.so.23.3.0
    WORKING_DIRECTORY ${source_dir}
)

ExternalProject_Add_StepTargets(${name} remove_so)

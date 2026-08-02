set(name xxHash-0.6.5)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/Cyan4973/xxHash/archive/refs/tags/v0.6.5.zip
    URL_HASH MD5=133762ac7d0f00a30a7590f3e884a025
    DOWNLOAD_NAME xxHash-0.6.5.zip
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND
      "${common_configure_envs}"
      make -s -j${BUILDING_JOBS_NUM}
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM} PREFIX=${CMAKE_INSTALL_PREFIX}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(${name} remove_so
    ALWAYS TRUE
    DEPENDEES install
    LOG TRUE
    COMMENT "rm -rf ${CMAKE_INSTALL_PREFIX}/lib/libxxhash.so*"
    COMMAND rm -f ${CMAKE_INSTALL_PREFIX}/lib/libxxhash.so
    COMMAND rm -f ${CMAKE_INSTALL_PREFIX}/lib/libxxhash.so.0
    COMMAND rm -f ${CMAKE_INSTALL_PREFIX}/lib/libxxhash.so.0.6.5
    WORKING_DIRECTORY ${source_dir}
)

ExternalProject_Add_StepTargets(${name} remove_so)

set(name bytedisk.1.2.0)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
set(install_dir ${CMAKE_INSTALL_PREFIX})
execute_process(
    COMMAND bash "-c" "cat /etc/os-release | grep VERSION_CODENAME | awk -F = '{print $NF}'"
    OUTPUT_VARIABLE os_version
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
ExternalProject_Add(
    ${name}
    URL http://tosv.byted.org/obj/bytedisk/bytedisk_1.2.0%2Bbyted_amd64.tar.gz
        http://tosv.byted.org/obj/bytedisk-us/bytedisk_1.2.0%2Bbyted_amd64.tar.gz
    URL_HASH MD5=be564c8a94e8fd24dcd152192f88a092
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
  # TODO(guokuankuan@bytedance.com) change to source code compile
  # We cannot compile bytedisk from source code at the moment, but we will
  # update soon after bytedisk is ready.
    CONFIGURE_COMMAND cp -r ${source_dir}/${os_version}/lib ${install_dir} && cp -r ${source_dir}/${os_version}/include ${install_dir}
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    BUILD_ALWAYS 1
    BUILD_IN_SOURCE 1
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(${name} clean
    EXCLUDE_FROM_MAIN TRUE
    ALWAYS TRUE
    DEPENDEES configure
    COMMAND rm -f ${BUILD_INFO_DIR}/${name}-build
    WORKING_DIRECTORY ${source_dir}
)

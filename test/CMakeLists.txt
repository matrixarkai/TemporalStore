include_directories(${PROJECT_SOURCE_DIR}
                    ${PROJECT_SOURCE_DIR}/src
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/byte
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/abseil-cpp
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/json/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/libfiu/libfiu
                    ${PROJECT_BINARY_DIR}/thirdparty/brpc/output/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/benchmark/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/ev/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/gtest/include
                    ${OBJECT_STORE_COMPAT_INCLUDE_DIR}
                    ${PROJECT_SOURCE_DIR}/thirdparty
                    ${PROJECT_SOURCE_DIR}/thirdparty/rapidjson/include
                    ${OPENSSL_INCLUDE_DIR}
                    ${BoostInstallDir}/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/spdlog/include
                    ${CMAKE_BINARY_DIR}/src
                    ${PROJECT_SOURCE_DIR}/thirdparty/mtcache/src)


add_subdirectory(common)
add_subdirectory(smoketest)
add_subdirectory(mini_cluster)

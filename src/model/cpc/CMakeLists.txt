file(GLOB HDRS "*.h")
file(GLOB SRCS "cpc_dummy.cc")
add_library(cpc_model STATIC ${HDRS} ${SRCS})
add_definitions(-w)

set(BCACHE2_CPC_OPTIONAL_LIBS)
foreach(optional_lib isal-static cryptopp-static)
    if (TARGET ${optional_lib})
        list(APPEND BCACHE2_CPC_OPTIONAL_LIBS ${optional_lib})
    endif()
endforeach()

target_link_libraries(cpc_model
    common
    byte
    libprotobuf
    ${BCACHE2_CPC_OPTIONAL_LIBS})

# file(GLOB SRCS2 "cpc_test.cc")
# add_executable(cpc_test  ${HDRS} ${SRCS2})
# target_link_libraries(cpc_test
#     cpc_model
#     common
#     byte
#     libprotobuf
#     tcmalloc-static
#     isal-static
#     cryptopp-static)

# gtest_discover_tests(cpc_test DISCOVERY_MODE PRE_TEST)
# target_compile_options(cpc_test PUBLIC -fno-access-control)

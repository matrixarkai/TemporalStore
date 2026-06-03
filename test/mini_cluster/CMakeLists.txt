file(GLOB HDRS "mini_cluster.h")
file(GLOB SRCS "mini_cluster.cc")
add_library(mini_cluster_lib ${HDRS} ${SRCS})
set_target_properties(mini_cluster_lib PROPERTIES CXX_STANDARD 14)
target_link_libraries(mini_cluster_lib
    common
    test_common
    serverlib
    metaserver
    proxylib)

add_executable(mini_cluster main.cc)
set_target_properties(mini_cluster PROPERTIES CXX_STANDARD 14)
target_link_libraries(mini_cluster mini_cluster_lib)

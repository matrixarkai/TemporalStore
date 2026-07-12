// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <unistd.h>

#include "common/fiu_local.h"
#include "common/macros.h"
#include "common/status.h"
#include "server/flags_validators.h"
#include "server/server.h"

std::atomic<bool> g_stop_flag;
void SignalHandler(int /*signal*/) { g_stop_flag.store(true); }

void UserSignalHandler(int /*signal*/) {
    LOG_FLUSH();
#ifdef __CYGWIN__
    std::raise(SIGKILL);
#else
    kill(getpid(), SIGKILL);
#endif
}

int main(int args, char** argv) {
    BYTE_DEFER({ LOG_FLUSH(); });
    matrixobjectstore_init();
    BYTE_ASSERT_EQ(0, fiu_init(0));
    BYTE_ASSERT_EQ(0, fiu_rc_fifo("/tmp/fiu-ctrl"));
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&args, &argv, true);

    matrixobjectstore_set_flag("matrixobjectstore_client_max_write_size",
                       std::to_string(FLAGS_matrixobjectstore_client_max_write_size).c_str());
    matrixobjectstore_set_flag("matrixobjectstore_log_file_num",
                       std::to_string(FLAGS_matrixobjectstore_log_file_num).c_str());
    matrixobjectstore_set_flag("matrixobjectstore_log_file_size_mb",
                       std::to_string(FLAGS_matrixobjectstore_log_file_size_mb).c_str());

    bcache2::server::Server::Options options;
    options.service_thread_num = 8;  // passed to the bRPC server
    options.worker_thread_num = FLAGS_worker_num;
    options.background_thread_num = 4;
    options.host = FLAGS_host;
    options.host_v6 = FLAGS_host_v6;
    options.log_level = FLAGS_server_log_level;
    options.log_dir = FLAGS_server_log_dir;
    options.port = FLAGS_port;
    options.master_consul = FLAGS_master_consul;
    options.master_endpoint = FLAGS_master_endpoint;
    options.table_name = FLAGS_table_name;
    options.cluster_name = FLAGS_cluster_name;
    options.log_max_file_num = FLAGS_server_log_num;
    options.log_max_file_size = FLAGS_server_log_size;

    if (options.host.empty()) {
        const char* host = getenv("BYTED_HOST_IP");
        if (host != nullptr) {
            options.host = host;
        }
    }
    if (options.host_v6.empty()) {
        const char* host_v6 = getenv("BYTED_HOST_IPV6");
        if (host_v6 != nullptr) {
            options.host_v6 = host_v6;
        }
    }

    if (options.host.empty() && options.host_v6.empty()) {
        LOG_ERROR("host and host_v6 are both empty");
        return -1;
    }
    if (options.host_v6.empty()) {
        LOG_WARNING("running with v4 single stack");
    }

    g_stop_flag = false;
    signal(SIGINT, &SignalHandler);
    signal(SIGTERM, &SignalHandler);
    // for onebox test
    signal(SIGUSR1, &UserSignalHandler);

    bcache2::server::Server server;
    server.Init(options);
    bcache2::Status status = server.Start();
    if (!status.ok()) {
        LOG_ERROR("Start server failed").put("Error", status.ToString());
        std::cerr << "Start server failed, error: " << status.ToString()
                  << std::endl;  // log maybe not ready
        return -1;
    }

    LOG_INFO("Start server success!");
    while (!g_stop_flag) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG_INFO("Stopping server");
    server.Stop();

    matrixobjectstore_shutdown();
    return 0;
}

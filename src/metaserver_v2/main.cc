// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <chrono>
#include <stdio.h>
#include <iostream>
#include <string>
#include <thread>
#ifdef __CYGWIN__
extern "C" int fileno(FILE*);
#endif

#include "butil/file_util.h"
#include "byte/include/byte_log.h"
#include "byteraft/base/log_setting.h"
#include "spdlog/common.h"

#include "common/logging.h"
#include "common/pidfile.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/metaserver.h"
#include "metaserver_v2/metrics.h"

/// globals
std::atomic<bool> g_stop_flag;
bcache2::metaserver::MetaServer* g_metaserver_ptr = nullptr;  // for gdb debug

DECLARE_string(flagfile);
DECLARE_bool(version);
DECLARE_bool(help);

void SignalHandler(int /*signal*/) { g_stop_flag.store(true); }

void InitLogger() {
    // Is MonoRepo the key to solve the problem of these massive lib dependencies ???
    std::string ms_log_path =
        fmt::format("{}_{}_", bcache2::metaserver::FLAGS_metaserver_cluster_name,
                    bcache2::metaserver::FLAGS_metaserver_server_port);
    byte::SetByteLogFilePrefix(ms_log_path);
    byte::SetByteLogDir(bcache2::metaserver::FLAGS_metaserver_log_dir);
    byte::SetMinLogLevel(byte::LogLevel(bcache2::metaserver::FLAGS_metaserver_log_level));
    byte::SetByteLogMaxFileNum(bcache2::metaserver::FLAGS_metaserver_log_file_num);
    byte::SetByteLogMaxFileSize(bcache2::metaserver::FLAGS_metaserver_log_file_size);

    std::string brcp_log_path =
        fmt::format("{}/{}_{}_brpc.log", bcache2::metaserver::FLAGS_metaserver_log_dir,
                    bcache2::metaserver::FLAGS_metaserver_cluster_name,
                    bcache2::metaserver::FLAGS_metaserver_server_port);
    // google::SetLogDestination(google::INFO, brcp_log_path.c_str());

    std::string raft_log_path =
        fmt::format("{}/{}_{}_raft.log", bcache2::metaserver::FLAGS_metaserver_log_dir,
                    bcache2::metaserver::FLAGS_metaserver_cluster_name,
                    bcache2::metaserver::FLAGS_metaserver_server_port);
    byteraft::InitSizeBasedLogger(raft_log_path,
                                  bcache2::metaserver::FLAGS_metaserver_log_file_size,
                                  bcache2::metaserver::FLAGS_metaserver_log_file_num);
}

int BootMetaServer(bcache2::metaserver::MetaServer* ms) {
    LOG_INFO("init metaserver");
    auto status = ms->Init();
    if (!status.ok()) {
        LOG_ERROR("failed to init metaserver").put("status", status);
        return 1;
    }

    LOG_INFO("start metaserver");
    status = ms->Start();
    if (!status.ok()) {
        LOG_ERROR("failed to start metaserver").put("status", status);
        return 1;
    }
    return 0;
}

int PrepareWorkDir(const std::string& dir) {
    butil::FilePath bfp(dir);
    if (!butil::DirectoryExists(bfp)) {
        butil::File::Error e;
        if (!butil::CreateDirectoryAndGetError(bfp, &e, true /* recursive */)) {
            std::cerr << "failed to create work directory [" << dir << "], returned " << e;
            return 1;
        }
    }
    return 0;
}

int main(int argc, char** argv) {
    std::string help_str;
    help_str += "Usage: ";
    help_str += argv[0];
    help_str += " [OPTIONS...]";
    help_str += R"(
Options:"
    -version        Print version number.
    -flagfile=$path Load flags from file.)";
    gflags::SetUsageMessage(help_str);
    const std::string version_str = BCACHE2_VERSION;
    gflags::SetVersionString(version_str);
    gflags::AllowCommandLineReparsing();
    if (!gflags::ParseCommandLineFlags(&argc, &argv, false)) {
        std::cerr << "failed to parse command line" << std::endl;
        return -1;
    }
    gflags::SetCommandLineOption("bvar_dump", "false");
    gflags::SetCommandLineOption("bvar_dump_interval", "60");

    if (FLAGS_help) {
        std::cout << help_str << std::endl;
        return 0;
    }

    if (FLAGS_version) {
        std::cerr << version_str << std::endl;
        return 0;
    }

    InitLogger();
    bcache2::metaserver::InitMetrics(
        "bcache2.metaserver",
        {{"cluster", bcache2::metaserver::FLAGS_metaserver_cluster_name},
         {"port", std::to_string(bcache2::metaserver::FLAGS_metaserver_server_port)}});

    LOG_INFO("preparing workspace");
    g_stop_flag = false;
    signal(SIGINT, &SignalHandler);
    signal(SIGTERM, &SignalHandler);
    std::string work_dir = bcache2::metaserver::FLAGS_metaserver_work_dir;
    if (PrepareWorkDir(work_dir) != 0) {
        std::cerr << "failed to init work directory, exiting..." << std::endl;
    }
    std::string pid_filepath = fmt::format("{}/pid", work_dir);
    bcache2::PidFile pidfile(pid_filepath);
    if (!pidfile.TryLock()) {
        std::cerr << "failed to lock pid file " << pid_filepath << ", exiting..." << std::endl;
        return 1;
    }

    LOG_INFO("start to boot metaserver");
    auto ms = std::make_unique<bcache2::metaserver::MetaServer>();
    g_metaserver_ptr = ms.get();
    if (BootMetaServer(ms.get()) != 0) {
        LOG_FLUSH();
        std::exit(1);
    }

    LOG_INFO("boot metaserver success!");
    while (!g_stop_flag) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    LOG_INFO("metaserver stopping");
    ms->Stop();
    LOG_INFO("metaserver stopped");
    LOG_FLUSH();
    bcache2::metaserver::QuitMetrics();
    return 0;
}

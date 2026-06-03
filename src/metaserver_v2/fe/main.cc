// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <iostream>
#include <string>

#include "brpc/server.h"
#include "butil/file_util.h"
#include "byte/include/byte_log.h"
#include "byteraft/base/log_setting.h"
#include "gflags/gflags.h"
#include "spdlog/common.h"

#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/fe/api_gateway.h"

namespace bcache2 {
namespace metaserver {
DEFINE_string(metaserver_fe_work_dir, "./data", "dir");
DEFINE_string(metaserver_fe_log_dir, "./log", "dir");
DEFINE_int32(metaserver_fe_port, 7779, "service port");
}  // namespace metaserver
}  // namespace bcache2

int main(int argc, char** argv) {
    gflags::AllowCommandLineReparsing();
    if (!gflags::ParseCommandLineFlags(&argc, &argv, false)) {
        std::cerr << "failed to parse command line" << std::endl;
        return -1;
    }

    byte::SetByteLogDir(bcache2::metaserver::FLAGS_metaserver_fe_log_dir);
    byte::SetMinLogLevel(byte::LogLevel::LOG_LEVEL_INFO);

    const std::string data_path = bcache2::metaserver::FLAGS_metaserver_fe_work_dir + "/data";
    bcache2::metaserver::ClusterMap cluster_map(data_path);
    bcache2::Status status = cluster_map.Init();
    if (!status.ok()) {
        std::cerr << "failed to init cluster map " << status.ToString() << std::endl;
        return -1;
    }
    const std::string location_hint_path =
        bcache2::metaserver::FLAGS_metaserver_fe_work_dir + "/location_hint";
    bcache2::metaserver::LocationMap loc_map(location_hint_path);
    status = loc_map.Load();
    if (!status.ok()) {
        std::cerr << "failed to init loc map " << status.ToString() << std::endl;
        return -1;
    }

    bcache2::metaserver::HttpApiServiceImpl api_service(&cluster_map, &loc_map);
    brpc::Server server;
    if (server.AddService(&api_service, brpc::SERVER_DOESNT_OWN_SERVICE,
                          "/v1/query/*    => Query, "
                          "/v1/manage/*   => Manage, "
                          "/v1/add_cluster => AddCluster") != 0) {
        std::cerr << "failed to add service" << std::endl;
        return -1;
    }
    brpc::ServerOptions options;
    if (server.Start(bcache2::metaserver::FLAGS_metaserver_fe_port, &options) != 0) {
        std::cerr << "failed to start server" << std::endl;
        return -1;
    }
    server.RunUntilAskedToQuit();
    return 0;
}

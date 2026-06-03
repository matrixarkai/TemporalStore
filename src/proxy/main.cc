// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/logging.h"
#include "common/proto_enhance.h"
#include "proxy/flags.h"
#include "proxy/proxy.h"

namespace bcache2_proxy = bcache2::proxy;

int main(int args, char** argv) {
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&args, &argv, true);

    byte::SetByteLogDir(FLAGS_proxy_log_dir);
    byte::SetByteLogFilePrefix(std::to_string(FLAGS_port) + "_");
    byte::SetMinLogLevel(byte::LogLevel(FLAGS_proxy_log_level));
    byte::SetByteLogMaxFileNum(FLAGS_log_max_file_num);
    byte::SetByteLogMaxFileSize(FLAGS_log_max_file_size);

    bcache2_proxy::Proxy::Options opts;
    opts.cluster_name = FLAGS_proxy_cluster_name;
    opts.listen_port = FLAGS_port;
    opts.announce_port = FLAGS_port;
    opts.idc = FLAGS_idc;
    opts.log_dir = FLAGS_proxy_log_dir;
    opts.log_level = byte::LogLevel(FLAGS_proxy_log_level);
    opts.master_consul = FLAGS_master_consul;
    opts.master_endpoint = FLAGS_master_endpoint;
    opts.location.set_vregion(FLAGS_proxy_vregion);
    opts.location.set_vdc(FLAGS_proxy_vdc);
    opts.location.set_vau(FLAGS_proxy_vau);

    bcache2_proxy::Proxy proxy;
    bcache2::Status status = proxy.Start(opts);
    if (!status.ok()) {
        fprintf(stderr, "Failed to start proxy, %s", status.ToString().c_str());
        return -1;
    }

    proxy.Join();
    return 0;
}

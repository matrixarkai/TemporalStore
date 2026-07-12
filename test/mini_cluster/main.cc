// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>

#include "client/client.h"
#include "test/common/temp_dir.h"
#include "test/mini_cluster/mini_cluster.h"

DEFINE_string(host, "127.0.0.1", "host ip");
DEFINE_uint32(server_count, 2, "server count");
DEFINE_uint32(server_thread_num, 8, "server thread number");
DEFINE_uint32(server_worker_num, 8, "server worker number");
DEFINE_uint32(partition_set_num, 1, "partition set num");
DEFINE_uint32(partition_num, 2, "partition num");
DEFINE_string(table_namespace, "test", "table namespace");
DEFINE_string(table_name, "table1", "table name");
DEFINE_string(cluster_uri, "", "matrixobjectstore uri");

namespace bcache2 {

namespace metaserver {
DECLARE_uint32(metaserver_server_port);
}

int Main(int argc, char** argv) {
    // byte::SetByteLogDir("./");
    byte::SetByteLogMaxFileNum(10);
    byte::SetByteLogMaxFileSize(1UL << 30);
    byte::SetMinLogLevel(byte::LOG_LEVEL_INFO);
    FLAGS_enable_blockcache = false;
    FLAGS_blockcache_dram_capacity = 134217728;  // 128 MB
    FLAGS_blockcache_ssd_capacity = 134217728;   // 128 MB

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    matrixobjectstore_init();

    MiniCluster::Options options;
    options.server_count = FLAGS_server_count;
    options.server_thread_num = FLAGS_server_thread_num;
    options.server_worker_num = FLAGS_server_worker_num;
    TempDir temp_dir;
    if (FLAGS_cluster_uri != "") {
        options.cluster_uri = FLAGS_cluster_uri;
    } else {
        options.cluster_uri = "file://" + temp_dir.GetDir() + "/cluster/pool";
    }
    options.work_dir = temp_dir.GetDir();
    options.host = FLAGS_host;
    options.master_port = metaserver::FLAGS_metaserver_server_port;

    MiniCluster cluster;
    cluster.Init(options);
    Status status = cluster.Start();
    BYTE_ASSERT(status.ok());

    MasteWrapper* master = cluster.GetMaster();
    status = master->CreateSimpleTable(FLAGS_table_namespace, FLAGS_table_name,
                                       FLAGS_partition_set_num, FLAGS_partition_num);
    BYTE_ASSERT(status.ok());

    sleep(8640000);
    return 0;
}

}  // namespace bcache2

int main(int argc, char** argv) { return bcache2::Main(argc, argv); }

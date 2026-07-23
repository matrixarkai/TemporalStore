// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <byte/algorithm/crc32.h>
#include <gflags/gflags.h>

#include <string>

#include "common/function_closure.h"
#include "stream/tools/stream_tool.h"
#include "stream/tools/utils.h"

namespace tool = bcache2::stream::tool;

// blob://store-hl/public/bytedance.bcache2.wangtai-72057675642306578-index/
//                                                                   ^^^^^^^
//                                                       replace these by "-condition"
std::string ConjureConditionUri(const std::string& uri) {
    size_t pos = uri.rfind("-");
    return uri.substr(0, pos) + "-condition";
}

tool::DataSchema ConjureSchema(const std::string& uri) {
    if (uri.find("-index/") != std::string::npos) {
        return tool::DataSchema::IndexLog;
    }
    if (uri.find("-oplog/") != std::string::npos) {
        return tool::DataSchema::Oplog;
    }
    if (uri.find("-page") != std::string::npos) {
        return tool::DataSchema::Page;
    }
    return tool::DataSchema::Unknown;
}

int main(int argc, char** argv) {
    if (argc < 3 || strlen(argv[1]) == 0 || strlen(argv[2]) == 0) {
        fprintf(stderr, "Usage: %s <action> <stream uri> [options]\n", argv[0]);
        return 1;
    }

    std::string action = argv[1];
    std::string uri = argv[2];
    if (uri.back() != '/') {
        uri.push_back('/');
    }

    if (ConjureSchema(uri) == tool::DataSchema::Unknown) {
        fprintf(stderr, "Unknown schema\n");
        return 1;
    }

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    bytestore_init();

    byte::AsyncThreadPool pool;
    byte::AsyncThreadPoolOptions options;
    BYTE_ASSERT(pool.Init(options));
    BYTE_ASSERT(pool.Start());
    byte::AsyncThread* thread = pool.KthThread(0);
    bcache2::CoSyncClosure sync;
    int result = 0;
    thread->Invoke(bcache2::NewCoFuncClosure([&]() {
        tool::StreamTool tool;
        tool::StreamTool::Options opts;
        opts.uri = uri;
        opts.condition_uri = ConjureConditionUri(uri);
        opts.action = action;
        opts.data_schema = ConjureSchema(uri);
        bcache2::Status status = tool.Run(opts);
        if (!status.ok()) {
            fprintf(stderr, "%s\n", status.ToString().c_str());
            result = 1;
        }
        sync.Run();
    }));
    sync.Wait();

    bytestore_shutdown();
    return result;
}

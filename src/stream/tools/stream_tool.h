// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>

#include "common/status.h"
#include "partition/condition.h"
#include "stream/log_based_env.h"
#include "stream/tools/action/list_action.h"
#include "stream/tools/action/read_action.h"
#include "stream/tools/action/scan_action.h"
#include "stream/tools/action/stat_action.h"
#include "stream/tools/action/tailf_action.h"
#include "stream/tools/flags.h"
#include "stream/tools/utils.h"

namespace bcache2 {
namespace stream {
namespace tool {

class StreamTool {
 public:
    struct Options {
        std::string uri;
        std::string condition_uri;  // TODO(wangtai.10): remove condition_uri
        std::string action;
        DataSchema data_schema;
    };

    Status Run(Options opts) {
        if (FLAGS_verbose) {
            printf("#Action: %s\n", opts.action.c_str());
            printf("#Uri: %s\n", opts.uri.c_str());
            printf("#ConditionUri: %s\n", opts.condition_uri.c_str());
            printf("#Schema: %s\n", SchemaToString(opts.data_schema).c_str());
        }

        // init background pool
        byte::AsyncThreadPoolOptions work_options;
        byte::AsyncThreadPool pool;
        BYTE_ASSERT(pool.Init(work_options));
        BYTE_ASSERT(pool.Start());

        // init stream env
        StoreLayer store_layer(&pool);
        MetricsManager metrics_manager({}, "");
        stream::LogBasedEnv env;
        stream::LogBasedEnv::Options env_options;
        env_options.background_pool = &pool;
        env_options.store_layer = &store_layer;
        env.Init(env_options);

        // get condition
        Controller ctrl;
        stream::Env::Condition condition;
        stream::Env::ConditionData condition_data;
        env.GetCondition(&ctrl, opts.condition_uri, &condition_data);
        if (!ctrl.status().ok()) {
            return Status::Internal("Get condition failed: " + ctrl.status().ToString());
        }
        condition.name = opts.condition_uri;
        condition.data = condition_data;
        partition::ConditionInfoObserver condition_os(condition_data.data());
        if (FLAGS_verbose) {
            printf("#ConditionInfo: %s\n", condition_os.ToString().c_str());
        }

        // open stream
        ctrl.Reset();
        stream::Env::OpenOptions options;
        options.created = true;
        options.metrics_manager = &metrics_manager;
        options.readonly = true;
        stream::Stream* stream = nullptr;
        env.OpenStream(&ctrl, condition, opts.uri, options, &stream);
        if (!ctrl.status().ok()) {
            return Status::Internal("Open stream failed: " + ctrl.status().ToString());
        }

        // update stream info
        StreamInfo stream_info;
        Status status = GetStreamInfo(condition_os.RemoteIpStr(), condition_os.RemotePort(),
                                      opts.uri, condition_os.PartitionId(), &stream_info);
        if (!status.ok()) {
            return Status::Internal("Get stream info failed: " + status.ToString());
        }
        stream->RestoreInfo(stream_info);

        return DispatchAction(opts, &condition_os, &store_layer, stream)->Run();
    }

    std::unique_ptr<Action> DispatchAction(const Options& opts,
                                           partition::ConditionInfoObserver* condition_os,
                                           StoreLayer* store, Stream* stream) {
        std::unique_ptr<Action> action(new Action());
        if (opts.action == "ls") {
            action.reset(new ListAction(store, opts.uri));
        }
        if (opts.action == "scan") {
            action.reset(new ScanAction(stream, opts.uri, opts.data_schema));
        }
        if (opts.action == "tailf") {
            action.reset(new TailfAction(stream, condition_os, opts.uri, opts.data_schema));
        }
        if (opts.action == "read") {
            action.reset(
                new ReadAction(stream, opts.data_schema, opts.uri, FLAGS_address, FLAGS_size));
        }
        if (opts.action == "stat") {
            action.reset(new StatAction(stream));
        }
        return action;
    }
};

}  // namespace tool
}  // namespace stream
}  // namespace bcache2

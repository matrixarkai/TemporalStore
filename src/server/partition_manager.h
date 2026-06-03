// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "blockcache/blockcache.h"
#include "common/controller.h"
#include "partition/partition.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace server {

class Server;

class PartitionManager {
 public:
    PartitionManager(const std::string& cluster_name, Server* server,
                     byte::AsyncThreadPool* thread_pool, stream::Env* env,
                     blockcache::BlockCache* blockcache = nullptr);
    virtual ~PartitionManager();

    void Load(Controller* ctrl, const LoadRequest* request, LoadResponse* response,
              Closure<void>* callback);
    void LoadAsync(partition::Partition* partition_ptr);
    void Unload(Controller* ctrl, const UnloadRequest* request, UnloadResponse* response,
                Closure<void>* callback);
    void BatchExecuteCmd(Controller* ctrl, const BatchExecuteCmdRequest* request,
                         BatchExecuteCmdResponse* response, Closure<void>* callback);

    void GetInfo(Controller* ctrl, const GetInfoRequest* request, GetInfoResponse* response,
                 Closure<void>* callback);
    void ReadPartitionStream(Controller* ctrl, const ReadPartitionStreamRequest* request,
                             ReadPartitionStreamResponse* response, Closure<void>* callback);
    void ScanPartitionStream(Controller* ctrl, const ScanPartitionStreamRequest* request,
                             ScanPartitionStreamResponse* response, Closure<void>* callback);

    void SetConfig(Controller* ctrl, const SetConfigRequest* request, SetConfigResponse* response,
                   Closure<void>* callback);
    void GetConfig(Controller* ctrl, const GetConfigRequest* request, GetConfigResponse* response,
                   Closure<void>* callback);

    void UpdateMembership(Controller* ctrl, const UpdateMembershipRequest* request,
                          AckResponse* response, Closure<void>* callback);
    void GetStats(Controller* ctrl, const GetStatsRequest* request, GetStatsResponse* response,
                  Closure<void>* callback);

    void UnloadAll();
    absl::flat_hash_map<uint64_t, bool> GetPartitionLoadedStatus();

    void GetAllStats(google::protobuf::RepeatedPtrField<PartitionStats>*);

    void ReportLoadResult(uint64_t partitoin_id, Status result);

    void SetStopping();

 private:
    struct BatchExecuteContext {
        Controller* ctrl = nullptr;
        const BatchExecuteCmdRequest* request = nullptr;
        BatchExecuteCmdResponse* response = nullptr;
        Closure<void>* callback = nullptr;

        std::unique_ptr<Controller[]> ctrls;
        std::unique_ptr<Status[]> statuses;
        std::vector<std::unique_ptr<google::protobuf::Message>> requests;
        std::vector<std::unique_ptr<google::protobuf::Message>> responses;
        int complete_count = 0;
    };

    void BatchExecuteCmdInternal(BatchExecuteContext* context);
    void OnExecuteCmdDone(BatchExecuteContext* context, int index);

    struct ThreadLocalInfo {
        absl::flat_hash_map<uint64_t, std::unique_ptr<partition::Partition>> partition_map;
    } __attribute__((aligned(64)));

    byte::AsyncThread* GetThread(uint64_t partition_id);

    const std::string cluster_name_;
    Server* server_ = nullptr;
    byte::AsyncThreadPool* thread_pool_ = nullptr;
    stream::Env* env_ = nullptr;
    std::unique_ptr<ThreadLocalInfo[]> thread_infos_;
    static __thread ThreadLocalInfo* thread_info_;
    blockcache::BlockCache* blockcache_ = nullptr;
    bool stopping_ = false;

    DISALLOW_COPY_AND_ASSIGN(PartitionManager);
};

}  // namespace server
}  // namespace bcache2

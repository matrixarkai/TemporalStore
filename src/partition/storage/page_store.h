// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/include/macros.h>
#include <byte/string/format.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/coclosure.h"
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include "common/controller.h"
#include "common/data.h"
#include "common/time.h"
#include "partition/index/page_index.h"
#include "partition/metrics.h"
#include "protocol/config.pb.h"
#include "protocol/info.pb.h"
#include "protocol/storage.pb.h"
#include "stream/metrics.h"
#include "stream/stream.h"

namespace bcache2 {

class MetricsManager;

namespace blockcache {
class BlockCache;
}  // namespace blockcache

namespace partition {

struct PageInfo {
    uint64_t address = 0;
    uint64_t size = 0;
    storage::PageHeader header;
    std::string data;

    PageInfo() {}
    explicit PageInfo(PageIndex index) : address(index.address), size(index.page_size) {
        header.set_object_id(index.object_id);
        header.set_model_id(index.model_id);
        header.set_page_id(index.page_id);
        header.set_page_in_log(index.page_in_log);
        // we can't get more information (key, data, timestamp, version, e.t.c.)
    }
};

class Index;
class Partition;

using ObjectPages = std::vector<PageInfo>;  // an object consists of multiple pages

class PageStore {
 public:
    using ZoneInfo = storage::IndexLog::ZoneInfo;

    struct Options {
        stream::Env::Condition condition;
        std::string stream_uri_pattern;
    };

    struct ReadPathTestCounters {
        uint64_t blockcache_gets = 0;
        uint64_t blockcache_hits = 0;
        uint64_t persistent_reads = 0;
    };

    PageStore(Partition* partition, Index* index, stream::Env* env, MetricsManager* metrics_manager,
              StoreRepPolicy rep_policy, uint64_t partition_id,
              blockcache::BlockCache* blockcache);
    ~PageStore();

    void Init(Options opts);
    Status UpdateZones();

    void Close();

    Status PrepareNewZone(bool force_new_zone);
    void RecycledZone(uint32_t zone_id);
    void DestroyZone(uint32_t zone_id);
    std::vector<std::pair<uint32_t, ZoneInfo>> GetZoneInfos() const;

    void WritePage(PageInfo* info);

    void Commit(Controller* ctrl, Closure<void>* callback);

    void ReadPage(Controller* ctrl, PageIndex index, PageInfo* page_info, Closure<void>* callback);
    void SetReadPathTestCounters(ReadPathTestCounters* counters) {
        read_path_test_counters_ = counters;
    }

    void UpdateConfig(const Config& config) {
        rep_policy_.MergeFrom(config.stream_config().store_rep_policy());
        for (size_t zone_id = 1; zone_id < zones_.size(); ++zone_id) {
            if (zones_[zone_id] != nullptr) {
                zones_[zone_id]->stream->UpdateConfig(config.stream_config());
            }
        }
    }

    PageStoreInfo GetInfo() const;
    Status RestoreInfo(const PageStoreInfo& pagestore_info);
    void ReadRawStream(uint32_t zone_id, Controller* ctrl, uint64_t id, void* data, size_t size,
                       Closure<void>* callback);
    stream::ScopedIterator NewRawStreamIterator(uint32_t zone_id, uint64_t start_id,
                                                uint64_t end_id);

    void ReapMetrics() const {
        metrics_.zone_count->get()->Set(zones_.size());
        for (auto& zone : zones_) {
            if (zone) {
                zone->stream->ReapMetrics();
            }
        }
    }

 private:
    struct ZoneOpenInfo {
        uint32_t zone_id = 0;
        std::unique_ptr<stream::Stream> stream;
        ZoneMetrics metrics;

        ZoneOpenInfo(uint32_t zone_id, stream::Stream* stream, MetricsManager* metrics_manager)
            : zone_id(zone_id), stream(stream) {
            metrics.Init(metrics_manager, zone_id);
        }
    };
    struct ReadPageTask {
        ZoneOpenInfo* zone = nullptr;
        Controller* ctrl = nullptr;
        PageIndex page_index;
        PageInfo* page_info = nullptr;
        Closure<void>* callback = nullptr;
        std::string buffer;
        TimeCost timer;
        bool page_in_cache = false;
    };

    void OnReadPageDone(ReadPageTask* task);
    std::string GetUniqueID(PageIndex* page);
    std::string GetZoneUri(uint32_t zone_id);

    Partition* partition_ = nullptr;
    Index* index_ = nullptr;
    stream::Env* env_ = nullptr;
    MetricsManager* metrics_manager_ = nullptr;
    stream::Env::Condition condition_;
    std::string stream_uri_pattern_;
    bool readonly_ = false;
    StoreRepPolicy rep_policy_;
    uint64_t partition_id_;
    bcache2::blockcache::BlockCache* blockcache_ = nullptr;

    uint64_t version_ = UINT64_MAX;
    std::vector<std::unique_ptr<ZoneOpenInfo>> zones_;  // all zones
    uint32_t writing_zone_id_ = 0;                      // target zone to write

    PageStoreMetrics metrics_;
    ReadPathTestCounters* read_path_test_counters_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(PageStore);
};

}  // namespace partition
}  // namespace bcache2

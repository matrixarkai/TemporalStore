// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/fast_rand.h"
#include "common/string_utils.h"
#include "common/time.h"
#include "model/persistent_map.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_context_manager.h"

DECLARE_bool(model_deny_full_dump);
DECLARE_uint64(model_max_page_id);
DECLARE_double(model_max_space_amplification);
DECLARE_uint64(model_size_tiered_compaction_max_ignore_bucket_size);
DECLARE_uint64(model_size_tiered_compaction_min_bucket_size);
DECLARE_uint64(model_size_tiered_compaction_bucket_step);
DECLARE_uint64(model_size_tiered_compaction_max_threshold);

namespace bcache2 {
namespace model {

// OrSetImpl following this
//
//   class OrSetImpl {
//    public:
//       OrSetImpl(PersistentMap<Key, Value>* data);
//       Status OnLoaded();
//       Status OnChange(Key key, Value value);
//   };
template <typename Key, typename Value, typename OrSetImpl>
class OrSetModel {
 public:
    OrSetModel() {}
    virtual ~OrSetModel() {}

    OrSetModel(OrSetModel&& rmodel)
        : data_(std::move(rmodel.data_)),
          max_timestamp_(rmodel.max_timestamp_),
          map_(&data_, &max_timestamp_),
          orset_(&map_) {}
    OrSetModel(const OrSetModel&) = delete;
    OrSetModel& operator=(const OrSetModel&) = delete;
    OrSetModel& operator=(OrSetModel&&) = delete;

    Status Init(Allocator* allocator, std::vector<partition::PageInfo> pages) {
        data_ = BtreeMap<Key, std::pair<Property, Value>>(
            std::less<Key>(), Allocator::StlWrapper<std::pair<const Key, Value>>(allocator));
        map_ = PersistentMap<Key, Value>(&data_, &max_timestamp_);
        orset_ = OrSetImpl(&map_);
        for (size_t i = 0; i < pages.size(); ++i) {
            if (i > 0) {
                // ordered by version to protect data correctness
                // TODO(wangluping.502): version strictly increase after invalid index log trimed
                BYTE_ASSERT(pages[i].header.version() >= pages[i - 1].header.version());
                BYTE_ASSERT(pages[i].header.page_id() > pages[i - 1].header.page_id());
            }

            const auto& page = pages[i];
            google::protobuf::io::ArrayInputStream input(page.data.data(), page.data.size());
            google::protobuf::io::CodedInputStream stream(&input);
            while (stream.CurrentPosition() < static_cast<int>(page.data.size())) {
                Key key;
                Value value;
                std::string key_data;
                std::string value_data;
                uint8_t cluster_id = 0;
                bool deleted = 0;
                uint64_t timestamp = 0;
                if (!ReadKvItemFromStream(&stream, &key_data, &value_data, &cluster_id, &deleted,
                                          &timestamp)) {
                    return Status::DataLoss("Invalid kv item");
                }
                if (!ParseFromString(key_data, &key)) {
                    return Status::DataLoss("Invalid key");
                }
                if (!ParseFromString(value_data, &value)) {
                    return Status::DataLoss("Invalid value");
                }

                LOG_DEBUG("Parse kv")
                    .put("This", this)
                    .put("ObjectKey", page.header.key())
                    .put("Key", key)
                    .put("Value", value)
                    .put("ClusterId", cluster_id)
                    .put("Deleted", deleted)
                    .put("Timestamp", timestamp);

                max_timestamp_ = std::max(max_timestamp_, timestamp);
                if (!deleted) {
                    Property property;
                    property.page_id = page.header.page_id();
                    property.cluster_id = cluster_id;
                    property.deleted = deleted;
                    property.timestamp = timestamp;
                    data_[std::move(key)] = std::make_pair(property, std::move(value));
                } else {
                    data_.erase(key);
                }
                total_item_ += 1;
            }
        }
        return orset_.OnLoaded();
    }

    static Status BlindDumpNewPages(const std::vector<partition::PageIndex>& pages,
                                    const std::vector<partition::LogItem>& logs,
                                    std::vector<std::pair<uint16_t, std::string>>* new_pages) {
        uint64_t new_page_id = 0;
        bool obsolete_page = false;
        for (auto& page : pages) {
            BYTE_ASSERT(page.page_id <= UINT16_MAX);
            new_page_id = std::max(new_page_id, static_cast<uint64_t>(page.page_id) + 1);
            if (UNLIKELY(page.model_id == 0)) {
                // this page was generated before the multi-page version
                obsolete_page = true;
            }
        }

        if (obsolete_page || new_page_id >= FLAGS_model_max_page_id || pages.empty()) {
            // dump whole model to page 0 and delete other pages
            return Status::FailedPrecondition("Need dump whole page, so we need memory data");
        }

        // dump logs
        std::string new_page;
        std::unique_ptr<google::protobuf::io::StringOutputStream> output(
            new google::protobuf::io::StringOutputStream(&new_page));
        std::unique_ptr<google::protobuf::io::CodedOutputStream> stream(
            new google::protobuf::io::CodedOutputStream(output.get()));
        for (auto& item : logs) {
            if (item.log.meta_log()) {
                // we don't care meta in page
                continue;
            }
            if (item.log.object_deleted()) {
                // previous logs belongs to other object
                stream->Trim();
                new_page.clear();
                output.reset(new google::protobuf::io::StringOutputStream(&new_page));
                stream.reset(new google::protobuf::io::CodedOutputStream(output.get()));
                continue;
            }
            BYTE_ASSERT(!item.log.page_log());  // we do not support hybrid kv-log and page-log yet

            WriteKvItemToStream(stream.get(), item.log.key(), item.log.value(),
                                item.log.cluster_id(), item.log.deleted(), item.log.timestamp_ms());
        }
        stream->Trim();
        if (new_page.empty()) {
            // no page data. e.g.: all logs is meta log
            return {};
        }

        new_pages->emplace_back(new_page_id, std::move(new_page));
        return Status::OK();
    }

    std::vector<std::pair<uint16_t, std::string>> DumpNewPages(
        const std::vector<partition::PageIndex>& pages, /* stock pages */
        const std::vector<partition::LogItem>& logs /* incremental logs */) {
        uint64_t new_page_id = 0;
        uint64_t total_page_size = 0;
        bool obsolete_page = false;
        for (auto& page : pages) {
            BYTE_ASSERT(page.page_id <= UINT16_MAX);
            new_page_id = std::max(new_page_id, static_cast<uint64_t>(page.page_id) + 1);
            total_page_size += page.page_size;
            if (UNLIKELY(page.model_id == 0)) {
                // this page was generated before the multi-page version
                obsolete_page = true;
            }
        }

        std::vector<std::pair<uint16_t, std::string>> new_pages;
        std::string new_page;
        std::unique_ptr<google::protobuf::io::StringOutputStream> output(
            new google::protobuf::io::StringOutputStream(&new_page));
        std::unique_ptr<google::protobuf::io::CodedOutputStream> stream(
            new google::protobuf::io::CodedOutputStream(output.get()));
        if (!FLAGS_model_deny_full_dump &&
            (obsolete_page ||
             total_page_size <= FLAGS_model_size_tiered_compaction_min_bucket_size ||
             new_page_id >= FLAGS_model_max_page_id ||
             total_item_ >= data_.size() * FLAGS_model_max_space_amplification)) {
            // dump whole model to page 0 and delete other pages
            new_page_id = 0;
            for (const auto& item : data_) {
                LOG_DEBUG("Dump KV")
                    .put("This", this)
                    .put("Key", item.first)
                    .put("Value", item.second.second)
                    .put("ClusterId", item.second.first.cluster_id)
                    .put("Deleted", item.second.first.deleted)
                    .put("Timestamp", item.second.first.timestamp);
                std::string key = SerializeToString<Key>(item.first);
                std::string value = SerializeToString<Value>(item.second.second);
                WriteKvItemToStream(stream.get(), key, value, item.second.first.cluster_id,
                                    item.second.first.deleted, item.second.first.timestamp);
            }
            for (auto& page : pages) {
                if (page.page_id != new_page_id) {
                    new_pages.emplace_back(page.page_id, "");
                }
            }
            stream->Trim();
            total_item_ = data_.size();
        } else {
            // dump logs
            for (auto& item : logs) {
                if (item.log.meta_log()) {
                    // we don't care meta in page
                    continue;
                }
                if (item.log.object_deleted()) {
                    // previous logs belongs to other object
                    LOG_DEBUG("Delete log occured, clear all page data").put("This", this);
                    stream->Trim();
                    new_page.clear();
                    output.reset(new google::protobuf::io::StringOutputStream(&new_page));
                    stream.reset(new google::protobuf::io::CodedOutputStream(output.get()));
                    continue;
                }
                BYTE_ASSERT(
                    !item.log.page_log());  // we do not support hybrid kv-log and page-log yet

                LOG_DEBUG("Dump KV")
                    .put("This", this)
                    .put("Key", DebugRawString(item.log.key()))
                    .put("Value", DebugRawString(item.log.value()))
                    .put("ClusterId", item.log.cluster_id())
                    .put("Deleted", item.log.deleted())
                    .put("Timestamp", item.log.timestamp_ms());
                WriteKvItemToStream(stream.get(), item.log.key(), item.log.value(),
                                    item.log.cluster_id(), item.log.deleted(),
                                    item.log.timestamp_ms());
                total_item_ += 1;
            }
            stream->Trim();
            if (new_page.empty()) {
                // no page data. e.g.: all logs is meta log
                return {};
            }
        }

        LOG_DEBUG("Dump new pages")
            .put("This", this)
            .put("TotalPageSize", total_page_size)
            .put("NewPageId", new_page_id)
            .put("NewPageSize", new_page.size())
            .put("TotalItem", total_item_)
            .put("DataItem", data_.size());
        new_pages.emplace_back(new_page_id, std::move(new_page));
        return new_pages;
    }

    // returns: page_bucket, remove_tombstone
    static std::pair<std::vector<partition::PageIndex>, bool> CompactPagesHint(
        std::vector<partition::PageIndex> pages) {
        BYTE_ASSERT(!pages.empty());

        // sort by page_id
        // we assume the smaller the page_id, the earlier slot dump
        std::sort(pages.begin(), pages.end(),
                  [](const partition::PageIndex& lhs, const partition::PageIndex& rhs) {
                      return lhs.page_id < rhs.page_id;
                  });

        LOG_DEBUG("Build bucket");
        std::vector<std::vector<partition::PageIndex>> buckets;
        std::vector<size_t> bucket_size;
        buckets.reserve(10);
        bucket_size.reserve(10);
        buckets.emplace_back(std::vector<partition::PageIndex>{});
        bucket_size.emplace_back(FLAGS_model_size_tiered_compaction_min_bucket_size);
        // we assume the newer the page, the smaller the size
        // so iterate pages in descending order
        for (size_t i = pages.size() - 1; i >= 0; --i) {
            BYTE_ASSERT(pages[i].page_size > 0);
            if (pages[i].page_in_log) {
                // we do not support compact page log yet
                return {};
            }

            if (pages[i].page_size > bucket_size.back() &&
                (i == 0 || pages[i - 1].page_size > bucket_size.back())) {
                buckets.emplace_back(std::vector<partition::PageIndex>{});
                bucket_size.emplace_back(bucket_size.back() *
                                         FLAGS_model_size_tiered_compaction_bucket_step);
                buckets.back().reserve(pages.size());
            }

            buckets.back().emplace_back(pages[i]);
            if (i == 0) {
                break;
            }
        }

        // debug log
        if (UNLIKELY(byte::GetMinLogLevel() <= byte::LogLevel::LOG_LEVEL_DEBUG)) {
            for (size_t bucket_id = 0; bucket_id < buckets.size(); ++bucket_id) {
                for (auto& page : buckets[bucket_id]) {
                    LOG_DEBUG("Show pages in bucket")
                        .put("BucketId", bucket_id)
                        .put("ObjectId", page.object_id)
                        .put("ModelId", page.model_id)
                        .put("PageId", page.page_id)
                        .put("PageSize", page.page_size);
                }
            }
        }

        // get all overflow buckets
        std::vector<size_t> overflow_buckets_id;
        overflow_buckets_id.reserve(10);
        for (size_t bucket_id = 0; bucket_id < buckets.size(); ++bucket_id) {
            if (bucket_size[bucket_id] >
                FLAGS_model_size_tiered_compaction_max_ignore_bucket_size) {
                break;
            }
            if (buckets[bucket_id].size() > FLAGS_model_size_tiered_compaction_max_threshold) {
                overflow_buckets_id.emplace_back(bucket_id);
            }
        }

        // pick an overflow bucket
        if (overflow_buckets_id.empty()) {
            return {};
        }

        size_t pick_idx = butil::fast_rand_in(0UL, overflow_buckets_id.size() - 1);
        size_t pick_bucket_id = overflow_buckets_id[pick_idx];
        bool remove_tombstone = (pick_bucket_id == buckets.size() - 1);
        return {buckets[pick_bucket_id], remove_tombstone};
    }

    static std::vector<partition::PageInfo> CompactPages(
        const std::vector<partition::PageInfo>& pages, bool remove_tombstone) {
        BYTE_ASSERT(!pages.empty());

        partition::PageInfo new_page;
        new_page.header.set_slot_id(pages.front().header.slot_id());
        new_page.header.set_object_id(pages.front().header.object_id());
        new_page.header.set_page_id(pages.front().header.page_id());
        new_page.header.set_timestamp_ms(GetCurrentTimeInMs());
        new_page.header.set_key(pages.front().header.key());
        new_page.header.set_model_id(pages.front().header.model_id());
        new_page.header.set_page_in_log(false);

        // TODO(wangtai.10): merge sort instead of data
        BtreeMap<Key, std::pair<Property, Value>, std::allocator<std::pair<const Key, Value>>>
            merge_data;
        uint64_t max_oplog_sequence = 0;
        for (size_t i = 0; i < pages.size(); ++i) {
            if (i > 0) {
                // ordered by version to protect data correctness
                // TODO(wangluping.502): version strictly increase after invalid index log trimed
                BYTE_ASSERT(pages[i].header.version() >= pages[i - 1].header.version());
                BYTE_ASSERT(pages[i].header.page_id() > pages[i - 1].header.page_id());
            }

            const auto& page = pages[i];
            max_oplog_sequence = std::max(max_oplog_sequence, page.header.oplog_sequence());
            google::protobuf::io::ArrayInputStream input(page.data.data(), page.data.size());
            google::protobuf::io::CodedInputStream stream(&input);
            std::string data;
            while (stream.CurrentPosition() < static_cast<int>(page.data.size())) {
                Key key;
                Value value;
                std::string key_data;
                std::string value_data;
                uint8_t cluster_id = 0;
                bool deleted = 0;
                uint64_t timestamp = 0;

                BYTE_ASSERT(ReadKvItemFromStream(&stream, &key_data, &value_data, &cluster_id,
                                                 &deleted, &timestamp))
                    << "Invalid kv item, Data: " << DebugRawString(data.data(), data.size());
                BYTE_ASSERT(ParseFromString(key_data, &key))
                    << "Invalid kv item, Data: " << DebugRawString(data.data(), data.size())
                    << ", Key: " << DebugRawString(key_data.data(), key_data.size());
                BYTE_ASSERT(ParseFromString(value_data, &value))
                    << "Invalid kv item, Data: " << DebugRawString(data.data(), data.size())
                    << ", Key: " << DebugRawString(value_data.data(), value_data.size());

                if (deleted && remove_tombstone) {
                    merge_data.erase(key);
                } else {
                    Property property;
                    property.page_id = page.header.page_id();
                    property.cluster_id = cluster_id;
                    property.deleted = deleted;
                    property.timestamp = timestamp;
                    merge_data[std::move(key)] = std::make_pair(property, std::move(value));
                }
            }
        }
        new_page.header.set_oplog_sequence(max_oplog_sequence);
        new_page.header.set_version(pages.back().header.version());
        LOG_DEBUG("Compact Page").put("PageHeader", new_page.header.ShortDebugString());

        google::protobuf::io::StringOutputStream output(&new_page.data);
        google::protobuf::io::CodedOutputStream stream(&output);
        for (const auto& item : merge_data) {
            LOG_DEBUG("Compacted KV")
                .put("ObjectKey", new_page.header.key())
                .put("Key", item.first)
                .put("Value", item.second.second)
                .put("ClusterId", item.second.first.cluster_id)
                .put("Deleted", item.second.first.deleted)
                .put("Timestamp", item.second.first.timestamp);
            std::string key = SerializeToString<Key>(item.first);
            std::string value = SerializeToString<Value>(item.second.second);
            WriteKvItemToStream(&stream, key, value, item.second.first.cluster_id,
                                item.second.first.deleted, item.second.first.timestamp);
        }
        stream.Trim();

        return {new_page};
    }

    Status Apply(Allocator* allocator, std::string key_data, std::string value_data,
                 uint64_t cluster_id, uint64_t timestamp, bool deleted) {
        Key key = Key();
        Value value = Value();
        if (!ParseFromString(key_data, &key)) {
            return Status::InvalidArgument("Invalid key data");
        }
        if (!ParseFromString(value_data, &value)) {
            return Status::InvalidArgument("Invalid value data");
        }

        // auto it = data_.find(key);
        // TODO(zhangyuan.42): Support CRDT
        // if (it == data_.end() || timestamp > it->second.first.timestamp ||
        //     (timestamp == it->second.first.timestamp && cluster_id >
        //     it->second.first.cluster_id)) {
        Property property;
        property.page_id = 0;
        property.cluster_id = cluster_id;
        property.deleted = deleted;
        property.timestamp = timestamp;

        if (deleted) {
            data_.erase(key);
        } else {
            data_[std::move(key)] = std::make_pair(property, std::move(value));
        }
        max_timestamp_ = std::max(max_timestamp_, property.timestamp);
        // }
        return Status::OK();
    }

    Status Merge(Allocator* allocator, std::string key_data, std::string value_data,
                 uint64_t cluster_id, uint64_t timestamp, bool deleted) {
        // unimplemented!
        BYTE_ASSERT(false);  // TODO(zhangyuan.42): Support CRDT

        Key key = Key();
        Value value = Value();
        if (!ParseFromString(key_data, &key)) {
            return Status::InvalidArgument("Invalid key data");
        }
        if (!ParseFromString(value_data, &value)) {
            return Status::InvalidArgument("Invalid value data");
        }

        auto it = data_.find(key);
        if (it == data_.end() || timestamp > it->second.first.timestamp ||
            (timestamp == it->second.first.timestamp && cluster_id > it->second.first.cluster_id)) {
            Property property;
            property.page_id = 0;
            property.cluster_id = cluster_id;
            property.deleted = deleted;
            property.timestamp = timestamp;

            data_[std::move(key)] = std::make_pair(property, std::move(value));
            max_timestamp_ = std::max(max_timestamp_, property.timestamp);
        }
        return Status::OK();
    }

    OrSetImpl& OrSet() { return orset_; }

 private:
    BtreeMap<Key, std::pair<Property, Value>> data_;
    uint64_t max_timestamp_ = 0;
    PersistentMap<Key, Value> map_{&data_, &max_timestamp_};  // refactor
    OrSetImpl orset_{&map_};
    uint32_t total_item_ = 0;
};

}  // namespace model
}  // namespace bcache2

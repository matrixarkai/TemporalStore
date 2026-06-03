// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "common/allocator.h"
#include "common/macros.h"
#include "common/ring_array.h"
#include "common/status.h"
#include "partition/index/page_index.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_context_manager.h"

#define REGISTER_MODEL(ModelType, model_id) \
    INITIALIZE({ ModelManager::RegisterModel<ModelType>(model_id); })

namespace bcache2 {
namespace model {

// we use dynamic api table to avoid memory overhead of vptr:
// all models implement the same api and register to ModelManager
// example: model/dummy_model.h
struct ModelApi {
    uint8_t model_id = 0;
    size_t model_size = 0;
    size_t alignment = 0;
    std::function<void*()> creator;
    std::function<void(void*)> deleter;
    std::function<void(void*)> constructor;
    std::function<void(void*)> destructor;
    std::function<void(void*, void*)> move_constructor;

    // init object via pages
    std::function<Status(void*, Allocator*, std::vector<partition::PageInfo>)> init;

    // try to blind dump pages with no memory data,
    // so use dump_new_pages if blind dump failed
    std::function<Status(const std::vector<partition::PageIndex>&,
                         const std::vector<partition::LogItem>&,
                         std::vector<std::pair<uint16_t, std::string>>*)>
        blind_dump_new_pages;

    // dump new pages based on stock pages, logs and memory data
    std::function<std::vector<std::pair<uint16_t, std::string>>(
        void*, const std::vector<partition::PageIndex>&, const std::vector<partition::LogItem>&)>
        dump_new_pages;

    // get compact hint (e.g. which pages do we need?)
    std::function<std::pair<std::vector<partition::PageIndex>, bool>(
        std::vector<partition::PageIndex>)>
        compact_pages_hint;

    // compact pages data
    std::function<std::vector<partition::PageInfo>(const std::vector<partition::PageInfo>&, bool)>
        compact_pages;

    // apply sub-key
    std::function<Status(void*, Allocator*, std::string, std::string, uint64_t, uint64_t, bool)>
        apply;

    // conflict merge
    std::function<Status(void*, Allocator*, std::string, std::string, uint64_t, uint64_t, bool)>
        merge;
};

template <typename Model>
class ModelTraits {
 public:
    static ModelApi* api;
};
template <typename Model>
ModelApi* ModelTraits<Model>::api;

class ModelManager {
 public:
    template <typename Model>
    static void RegisterModel(uint8_t model_id) {
        BYTE_ASSERT(model_id <= UINT8_MAX);

        model_api_table_[model_id].model_id = model_id;
        model_api_table_[model_id].model_size = sizeof(Model);
        model_api_table_[model_id].alignment = alignof(Model);

        model_api_table_[model_id].creator = []() { return new Model(); };
        model_api_table_[model_id].deleter = [](void* raw_model_buf) {
            delete reinterpret_cast<Model*>(raw_model_buf);
        };

        model_api_table_[model_id].constructor = [](void* raw_model_buf) {
            new (raw_model_buf) Model();
        };

        model_api_table_[model_id].destructor = [](void* raw_model_buf) {
            reinterpret_cast<Model*>(raw_model_buf)->~Model();
        };

        model_api_table_[model_id].move_constructor = [](void* dst, void* src) {
            Model& src_model = *reinterpret_cast<Model*>(src);
            new (dst) Model(std::move(src_model));
        };

        model_api_table_[model_id].init = [](void* raw_model_buf, Allocator* allocator,
                                             std::vector<partition::PageInfo> pages) {
            return reinterpret_cast<Model*>(raw_model_buf)->Init(allocator, std::move(pages));
        };

        model_api_table_[model_id].blind_dump_new_pages =
            [](const std::vector<partition::PageIndex>& pages,
               const std::vector<partition::LogItem>& logs,
               std::vector<std::pair<uint16_t, std::string>>* new_pages) {
                return Model::BlindDumpNewPages(pages, logs, new_pages);
            };

        model_api_table_[model_id].dump_new_pages =
            [](void* raw_model_buf, const std::vector<partition::PageIndex>& pages,
               const std::vector<partition::LogItem>& logs) {
                return reinterpret_cast<Model*>(raw_model_buf)->DumpNewPages(pages, logs);
            };

        model_api_table_[model_id].compact_pages_hint =
            [](std::vector<partition::PageIndex> pages) {
                return Model::CompactPagesHint(std::move(pages));
            };

        model_api_table_[model_id].compact_pages = [](const std::vector<partition::PageInfo>& pages,
                                                      bool remove_tombstone) {
            return Model::CompactPages(pages, remove_tombstone);
        };

        model_api_table_[model_id].apply =
            [](void* raw_model_buf, Allocator* allocator, std::string key_data,
               std::string value_data, uint64_t cluster_id, uint64_t timestamp, bool deleted) {
                return reinterpret_cast<Model*>(raw_model_buf)
                    ->Apply(allocator, std::move(key_data), std::move(value_data), cluster_id,
                            timestamp, deleted);
            };

        model_api_table_[model_id].merge =
            [](void* raw_model_buf, Allocator* allocator, std::string key_data,
               std::string value_data, uint64_t cluster_id, uint64_t timestamp, bool deleted) {
                return reinterpret_cast<Model*>(raw_model_buf)
                    ->Merge(allocator, std::move(key_data), std::move(value_data), cluster_id,
                            timestamp, deleted);
            };

        ModelTraits<Model>::api = &model_api_table_[model_id];
    }

    template <typename Model>
    static uint8_t GetModelId() {
        return ModelTraits<Model>::api->model_id;
    }
    static size_t GetModelSize(uint8_t model_id) { return model_api_table_[model_id].model_size; }
    static size_t GetModelAlignment(uint8_t model_id) {
        return model_api_table_[model_id].alignment;
    }
    static void* New(uint8_t model_id) { return model_api_table_[model_id].creator(); }
    static void Delete(uint8_t model_id, void* buf) { model_api_table_[model_id].deleter(buf); }
    static void Construct(uint8_t model_id, void* buf) {
        model_api_table_[model_id].constructor(buf);
    }
    static void Destruct(uint8_t model_id, void* buf) {
        model_api_table_[model_id].destructor(buf);
    }
    static void MoveConstruct(uint8_t model_id, void* dst, void* src) {
        model_api_table_[model_id].move_constructor(dst, src);
    }
    static Status Init(uint8_t model_id, void* buf, Allocator* allocator,
                       std::vector<partition::PageInfo> pages) {
        return model_api_table_[model_id].init(buf, allocator, std::move(pages));
    }
    static Status BlindDumpNewPages(uint8_t model_id,
                                    const std::vector<partition::PageIndex>& pages,
                                    const std::vector<partition::LogItem>& logs,
                                    std::vector<std::pair<uint16_t, std::string>>* new_pages) {
        return model_api_table_[model_id].blind_dump_new_pages(pages, logs, new_pages);
    }
    static std::vector<std::pair<uint16_t, std::string>> DumpNewPages(
        uint8_t model_id, void* buf, const std::vector<partition::PageIndex>& pages,
        const std::vector<partition::LogItem>& logs) {
        return model_api_table_[model_id].dump_new_pages(buf, pages, logs);
    }
    static std::pair<std::vector<partition::PageIndex>, bool> CompactPagesHint(
        uint8_t model_id, std::vector<partition::PageIndex> pages) {
        return model_api_table_[model_id].compact_pages_hint(std::move(pages));
    }
    static std::vector<partition::PageInfo> CompactPages(
        uint8_t model_id, const std::vector<partition::PageInfo>& pages, bool remove_tombstone) {
        return model_api_table_[model_id].compact_pages(pages, remove_tombstone);
    }
    static Status Apply(uint8_t model_id, void* buf, Allocator* allocator, std::string key_data,
                        std::string value_data, uint64_t cluster_id, uint64_t timestamp,
                        bool deleted) {
        return model_api_table_[model_id].apply(buf, allocator, std::move(key_data),
                                                std::move(value_data), cluster_id, timestamp,
                                                deleted);
    }
    static Status Merge(uint8_t model_id, void* buf, Allocator* allocator, std::string key_data,
                        std::string value_data, uint64_t cluster_id, uint64_t timestamp,
                        bool deleted) {
        return model_api_table_[model_id].merge(buf, allocator, std::move(key_data),
                                                std::move(value_data), cluster_id, timestamp,
                                                deleted);
    }

 private:
    static ModelApi model_api_table_[UINT8_MAX];

    DISALLOW_INSTANTIATE(ModelManager);
};

}  // namespace model
}  // namespace bcache2

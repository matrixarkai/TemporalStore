// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/allocator.h"
#include "common/status.h"
#include "partition/index/layout/header.h"
#include "partition/index/page_index.h"
#include "partition/storage/object.h"

namespace bcache2 {
namespace partition {

// abstract class for SingleObject, SinglePageObject, MultiPageObject
// each slot has a layout
class Layout {
 public:
    struct WriteOptions {
        bool update_if_exist = false;
        Allocator* allocator = nullptr;
        bool return_if_already_exists = false;

        explicit WriteOptions(Allocator* alloc) : allocator(alloc) {}
        WriteOptions(Allocator* alloc, bool return_if_already_exists)
            : allocator(alloc), return_if_already_exists(return_if_already_exists) {}
    };

    Layout() = default;
    virtual ~Layout() {}

    virtual void ConstructFrom(const std::vector<PageIndex>& rpages,
                               const std::vector<Object>& robjs, uint32_t last_used) = 0;
    virtual void Destroy() = 0;

    virtual LayoutType CurrentLayout() = 0;

    virtual Status NewPage(const WriteOptions& opts, const PageIndex& page) = 0;
    virtual Status UpdatePage(uint8_t object_id, uint16_t page_id, const PageIndex& new_page) = 0;
    virtual Status DeletePage(uint8_t object_id, uint16_t page_id) = 0;
    virtual Status ClearPages() = 0;
    virtual Status FindPage(uint8_t object_id, uint16_t page_id, PageIndex* page) const = 0;
    virtual std::vector<PageIndex> GetPages() const = 0;
    virtual size_t GetPageNum() const = 0;

    virtual Status NewObjectWithId(const WriteOptions& opts, uint8_t object_id, uint8_t model_id,
                                   const absl::string_view& key, Object* object) = 0;
    virtual Status NewObject(const WriteOptions& opts, uint8_t model_id,
                             const absl::string_view& key, Object* object) = 0;
    virtual Status DeleteObject(const absl::string_view& key) = 0;
    virtual Status FindObject(const absl::string_view& key, Object* object) const = 0;
    virtual Status ClearObjects() = 0;
    virtual std::vector<Object> GetObjects() const = 0;
    virtual size_t GetObjectNum() const = 0;

    virtual void SetLastUsed(uint32_t last_used) = 0;
    virtual uint32_t GetLastUsed() const = 0;
};

}  // namespace partition
}  // namespace bcache2

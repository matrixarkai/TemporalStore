// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/metrics.h"
#include "common/status.h"
#include "partition/index/layout/layout.h"
#include "partition/index/layout/layout_manager.h"
#include "partition/index/page_index.h"
#include "partition/metrics.h"
#include "partition/storage/object.h"

namespace bcache2 {
namespace partition {

// single page or a pointer point to layout
class SlotNode {
 public:
    using WriteOptions = Layout::WriteOptions;

    SlotNode()
        : pointer_(false),
          ttl_pointer_(false),
          dirty_(false),
          meta_dirty_(false),
          loading_(false),
          in_memory_(false),
          page_dirty_(false),
          page_deleted_(false),
          page_in_log_(false),
          trivial_page_(true),
          page_size_(0),
          ttl_(0) {}
    ~SlotNode() = default;

    LayoutType CurrentLayout() const;

    bool Loading() const { return loading_; }
    bool Dirty() const { return dirty_; }
    bool MetaDirty() const { return meta_dirty_; }
    bool InMemory() const { return in_memory_; }

    void SetLoading(bool value) { loading_ = value; }
    void SetDirty(bool value) { dirty_ = value; }
    void SetInMemory(bool value) { in_memory_ = value; }
    void SetMetaDirty(bool value) { meta_dirty_ = value; }

    Status NewPage(const WriteOptions& opts, const PageIndex& page);
    Status DeletePage(const WriteOptions& opts, uint8_t object_id, uint16_t page_id);
    void ClearPages(const WriteOptions& opts);
    Status UpdatePage(const WriteOptions& opts, uint8_t object_id, uint16_t page_id,
                      const PageIndex& new_page);
    Status FindPage(uint8_t object_id, uint16_t page_id, PageIndex* page) const;
    std::vector<PageIndex> GetPages() const;
    size_t GetPageNum() const;

    Status NewObjectWithId(const WriteOptions& opts, uint8_t object_id, uint8_t model_id,
                           const absl::string_view& key, Object* object);
    Status NewObject(const WriteOptions& opts, uint8_t model_id, const absl::string_view& key,
                     Object* object);
    Status DeleteObject(const WriteOptions& opts, const absl::string_view& key);
    void ClearObjects(const WriteOptions& opts);
    Status FindObject(const absl::string_view& key, Object* object) const;
    std::vector<Object> GetObjects() const;
    size_t GetObjectNum() const;

    void SetLastUsed(const WriteOptions& opts, uint32_t last_used);
    uint32_t GetLastUsed() const;

    uint64_t GetMinTtl() const {
        if (LIKELY(!ttl_pointer_)) {
            return ttl_;
        }
        std::vector<uint64_t>* ttls = reinterpret_cast<std::vector<uint64_t>*>(ttl_);
        uint64_t min_ttl = (*ttls)[0];
        for (size_t i = 0; i < ttls->size(); ++i) {
            if (min_ttl > (*ttls)[i]) {
                min_ttl = (*ttls)[i];
            }
        }
        return min_ttl;
    }

    void ClearObjectTtl(const WriteOptions& opts) {
        if (UNLIKELY(ttl_pointer_)) {
            std::vector<uint64_t>* ttls = reinterpret_cast<std::vector<uint64_t>*>(ttl_);
            delete ttls;
            ttl_pointer_ = false;
        }
        ttl_ = 0;
    }

    void SetObjectTtl(const WriteOptions& opts, uint64_t object_id, uint64_t value) {
        if (UNLIKELY(ttl_pointer_)) {
            std::vector<uint64_t>* ttls = reinterpret_cast<std::vector<uint64_t>*>(ttl_);
            if (ttls->size() <= object_id) {
                ttls->resize(object_id + 1);
            }
            (*ttls)[object_id] = value;
            return;
        }

        if (LIKELY(object_id == 0)) {
            ttl_ = value;
            return;
        }
        std::vector<uint64_t>* ttls = new std::vector<uint64_t>();
        ttls->resize(object_id + 1);
        (*ttls)[0] = ttl_;
        (*ttls)[object_id] = value;
        ttl_pointer_ = true;
        ttl_ = reinterpret_cast<uint64_t>(ttls);
    }

    uint64_t GetObjectTtl(uint64_t object_id) const {
        if (LIKELY(!ttl_pointer_)) {
            if (object_id > 0) {
                return 0;
            }
            return ttl_;
        }

        std::vector<uint64_t>* ttls = reinterpret_cast<std::vector<uint64_t>*>(ttl_);
        if (ttls->size() <= object_id) {
            return 0;
        }
        return (*ttls)[object_id];
    }

    std::vector<std::pair<uint8_t, uint64_t>> GetObjectTtls() const {
        std::vector<std::pair<uint8_t, uint64_t>> ret;

        if (LIKELY(!ttl_pointer_)) {
            ret.emplace_back(std::make_pair(0, ttl_));
            return ret;
        }

        std::vector<uint64_t>* ttls = reinterpret_cast<std::vector<uint64_t>*>(ttl_);
        for (uint8_t object_id = 0; object_id < ttls->size(); ++object_id) {
            ret.emplace_back(std::make_pair(object_id, ttls->at(object_id)));
        }
        return ret;
    }

    bool HasTtl() const {
        if (LIKELY(!ttl_pointer_)) {
            return ttl_ > 0;
        }

        std::vector<uint64_t>* ttls = reinterpret_cast<std::vector<uint64_t>*>(ttl_);
        for (size_t i = 0; i < ttls->size(); ++i) {
            if ((*ttls)[i] > 0) {
                return true;
            }
        }
        return false;
    }

 private:
    void TransformLayout(Allocator* allocator, size_t with_new_page_size,
                         size_t with_new_object_size, uint8_t new_object_id);
    void TransformMultiPageObject(Allocator* allocator);

    void TransformPageIn(const PageIndex& page);
    void TransformPageOut(PageIndex* page) const;

    // pointer_ indicates whether address_ is a page address or a pointer to layout
    bool pointer_ : 1;
    bool ttl_pointer_ : 1;

    // slot status
    bool dirty_ : 1;
    bool meta_dirty_ : 1;
    bool loading_ : 1;
    bool in_memory_ : 1;

    // page index
    bool page_dirty_ : 1;
    bool page_deleted_ : 1;
    bool page_in_log_ : 1;   // in oplog
    bool trivial_page_ : 1;  // trivial_page_ == true indicates page is deleted
    uint32_t reserved : 14;
    uint32_t page_size_ = 0;
    uint8_t model_id_ = 0;
    uint64_t ttl_ = 0;
    uint64_t address_ = 0;

    ALLOW_COPY_AND_ASSIGN(SlotNode);
} __attribute__((__packed__));
static_assert(std::is_standard_layout<SlotNode>::value);
static_assert(sizeof(SlotNode) == 24);

}  // namespace partition
}  // namespace bcache2

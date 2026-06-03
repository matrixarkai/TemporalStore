// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "partition/index/layout/header.h"
#include "partition/index/layout/layout.h"
#include "partition/index/layout/layout_manager.h"
#include "partition/index/page_index.h"
#include "partition/storage/object.h"

namespace bcache2 {

class Allocator;

namespace partition {

// ┌─────────────┬────────┬────────────┐
// │ header (4B) │  page  │   object   │
// └─────────────┴────────┴────────────┘
// NOTE: both object ID and page ID MUST be of 0
class SinglePageObject : public Layout {
 public:
    SinglePageObject() {}
    SinglePageObject(uint8_t* raw_buf, Allocator* allocator)
        : structure_(reinterpret_cast<Structure*>(raw_buf)), allocator_(allocator) {}
    ~SinglePageObject() = default;

    explicit operator bool() { return structure_ != nullptr; }

    void ConstructFrom(const std::vector<PageIndex>& rpages, const std::vector<Object>& robjs,
                       uint32_t last_used) override;
    void Destroy() override;

    LayoutType CurrentLayout() override {
        return static_cast<LayoutType>(structure_->header.magic);
    }

    Status NewPage(const WriteOptions& opts, const PageIndex& page) override;
    Status UpdatePage(uint8_t object_id, uint16_t page_id, const PageIndex& new_page) override;
    Status DeletePage(uint8_t object_id, uint16_t page_id) override;
    Status ClearPages() override;
    Status FindPage(uint8_t object_id, uint16_t page_id, PageIndex* page) const override;
    std::vector<PageIndex> GetPages() const override;
    size_t GetPageNum() const override { return structure_->trivial_page ? 0 : 1; }
    // object related functions down below are quite similar to those in SingleObject layout
    Status NewObjectWithId(const WriteOptions&, uint8_t object_id, uint8_t model_id,
                           const absl::string_view& key, Object* object) override;
    Status NewObject(const WriteOptions&, uint8_t model_id, const absl::string_view& key,
                     Object* object) override;
    Status DeleteObject(const absl::string_view& key) override;
    Status FindObject(const absl::string_view& key, Object* obj) const override;
    Status ClearObjects() override;
    std::vector<Object> GetObjects() const override;
    size_t GetObjectNum() const override;

    void SetLastUsed(uint32_t last_used) override { structure_->header.last_used = last_used; }
    uint32_t GetLastUsed() const override { return structure_->header.last_used; }

    void SetAllocator(Allocator* allocator) { allocator_ = allocator; }
    void SetRawBuf(uint8_t* raw_buf) { structure_ = reinterpret_cast<Structure*>(raw_buf); }

    static size_t RawLayoutSize(size_t with_object_size) {
        return sizeof(Structure) + with_object_size;
    }

 private:
    void TransformPageIn(const PageIndex& page);
    void TransformPageOut(PageIndex* page) const;

    struct Structure {
        Header header;
        // page starts
        bool page_dirty : 1;
        bool page_deleted : 1;
        bool page_in_log : 1;
        bool trivial_page : 1;  // trivial_page_ == true indicates page is deleted
        uint8_t reserved : 4;
        uint8_t model_id = 0;
        uint32_t page_size = 0;
        uint64_t page_address = 0;
        // page ends
        uint8_t raw_object_buf[];
    } __attribute__((__packed__));
    static_assert(std::is_standard_layout<Structure>::value, "for reinterpret cast");
    static_assert(sizeof(Structure) == 18);

    Structure* structure_ = nullptr;
    Allocator* allocator_ = nullptr;

    ALLOW_COPY_AND_ASSIGN(SinglePageObject);
};

REGISTER_LAYOUT(SinglePageObject, LayoutType::kSinglePageObject);

}  // namespace partition
}  // namespace bcache2

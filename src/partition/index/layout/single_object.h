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

// ┌──────────────┬─────────────────┐
// │  header (4B) │     object      │
// └──────────────┴─────────────────┘
// NOTE: object id MUST be 0
class SingleObject : public Layout {
 public:
    SingleObject() {}
    SingleObject(uint8_t* raw_buf, Allocator* allocator)
        : structure_(reinterpret_cast<Structure*>(raw_buf)), allocator_(allocator) {}
    ~SingleObject() = default;

    explicit operator bool() { return structure_ != nullptr; }

    void ConstructFrom(const std::vector<PageIndex>& rpages, const std::vector<Object>& robjs,
                       uint32_t last_used) override;
    void Destroy() override;

    LayoutType CurrentLayout() override {
        return static_cast<LayoutType>(structure_->header.magic);
    }
    // no pages at all
    Status NewPage(const WriteOptions& opts, const PageIndex& page) override {
        return Status::FailedPrecondition("invalid layout");
    }
    Status UpdatePage(uint8_t object_id, uint16_t page_id, const PageIndex& new_page) override {
        return Status::NotFound("page not found");
    }
    Status ClearPages() override { return Status::OK(); }
    Status DeletePage(uint8_t object_id, uint16_t page_id) override {
        return Status::NotFound("page not found");
    }
    Status FindPage(uint8_t object_id, uint16_t page_id, PageIndex* page) const override {
        return Status::NotFound("page not found");
    }
    std::vector<PageIndex> GetPages() const override { return {}; }
    size_t GetPageNum() const override { return 0; }
    // object_id == 0
    Status NewObjectWithId(const WriteOptions&, uint8_t object_id, uint8_t model_id,
                           const absl::string_view& key, Object* object) override;
    Status NewObject(const WriteOptions&, uint8_t model_id, const absl::string_view& key,
                     Object* object) override;
    Status DeleteObject(const absl::string_view& key) override;
    Status FindObject(const absl::string_view& key, Object* object) const override;
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
    struct Structure {
        Header header;
        uint8_t raw_buf[];
    } __attribute__((__packed__));
    static_assert(std::is_standard_layout<Structure>::value, "for reinterpret cast");
    static_assert(sizeof(Structure) == 4);

    Structure* structure_ = nullptr;
    Allocator* allocator_ = nullptr;

    ALLOW_COPY_AND_ASSIGN(SingleObject);
};

REGISTER_LAYOUT(SingleObject, LayoutType::kSingleObject);

}  // namespace partition
}  // namespace bcache2

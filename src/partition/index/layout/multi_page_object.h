// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/allocator.h"
#include "common/status.h"
#include "partition/index/layout/header.h"
#include "partition/index/layout/layout.h"
#include "partition/index/layout/layout_manager.h"
#include "partition/index/page_index.h"
#include "partition/storage/object.h"

namespace bcache2 {
namespace partition {

// NOLINT                                                        ┌────────┬────────┐
// NOLINT                                                ┌──────>│  page  │  page  │
// NOLINT                                                │       └────────┴────────┘
// NOLINT                                                │
// NOLINT   ┌─────────────┬──────────────────┬───────────┴────┐
// NOLINT   │ header (4B) │ object list (8B) │ page list (8B) │
// NOLINT   └─────────────┴────────┬─────────┴────────────────┘
// NOLINT                          │
// NOLINT                          │         ┌──────────┬──────────┬─────────┐
// NOLINT                          └────────>│  pointer │  pointer │ pointer │
// NOLINT                                    └──────────┴────┬─────┴─────────┘
// NOLINT                                                    │
// NOLINT                                                    │          ┌────────────┬──────────┐
// NOLINT                                                    └─────────>│  object id │  object  │
// NOLINT                                                               └────────────┴──────────┘
// TODO(wangtai.10): layout optimize
// TODO(wangtai.10): rethink allocator
class MultiPageObject : public Layout {
 public:
    MultiPageObject() {}
    MultiPageObject(uint8_t* raw_buf, Allocator* allocator)
        : structure_(reinterpret_cast<Structure*>(raw_buf)), allocator_(allocator) {}
    ~MultiPageObject() = default;

    explicit operator bool() { return structure_ != nullptr; }

    void ConstructFrom(const std::vector<PageIndex>& rpages, const std::vector<Object>& robjs,
                       uint32_t last_used) override;
    void Destroy() override;

    LayoutType CurrentLayout() override {
        return static_cast<LayoutType>(structure_->header.magic);
    }

    Status NewPage(const WriteOptions& opts, const PageIndex& new_page) override;
    Status UpdatePage(uint8_t object_id, uint16_t page_id, const PageIndex& new_page) override;
    Status DeletePage(uint8_t object_id, uint16_t page_id) override;
    Status ClearPages() override;
    Status FindPage(uint8_t object_id, uint16_t page_id, PageIndex* ret) const override;
    std::vector<PageIndex> GetPages() const override;
    size_t GetPageNum() const override;

    Status NewObjectWithId(const WriteOptions&, uint8_t object_id, uint8_t model_id,
                           const absl::string_view& key, Object* ret) override;
    Status NewObject(const WriteOptions&, uint8_t model_id, const absl::string_view& key,
                     Object* ret) override;
    Status DeleteObject(const absl::string_view& key) override;
    Status FindObject(const absl::string_view& key, Object* ret) const override;
    Status ClearObjects() override;
    std::vector<Object> GetObjects() const override;
    size_t GetObjectNum() const override;

    void SetLastUsed(uint32_t last_used) override { structure_->header.last_used = last_used; }
    uint32_t GetLastUsed() const override { return structure_->header.last_used; }

    void SetAllocator(Allocator* allocator) { allocator_ = allocator; }
    void SetRawBuf(uint8_t* raw_buf) { structure_ = reinterpret_cast<Structure*>(raw_buf); }

    static constexpr size_t RawLayoutSize(size_t with_object_size) { return sizeof(Structure); }

 private:
    struct ObjectWithId {
        uint8_t object_id = 0;
        uint8_t raw_object_buf[];
    };
    struct Structure {
        Header header;
        std::vector<ObjectWithId*, Allocator::StlWrapper<ObjectWithId*>> objects;
        std::vector<PageIndex, Allocator::StlWrapper<PageIndex>> pages;
        uint8_t object_num = 0;  // we need this var as not all objects in `objects` are valid, some
                                 // of which are just placeholders

        explicit Structure(Allocator* allocator)
            : objects(Allocator::StlWrapper<ObjectWithId*>(allocator)),
              pages(Allocator::StlWrapper<PageIndex>(allocator)) {}
    };
    static_assert(!std::is_standard_layout<Structure>::value, "Structure is not standard layout");
    Structure* structure_ = nullptr;
    Allocator* allocator_ = nullptr;

    ALLOW_COPY_AND_ASSIGN(MultiPageObject);
};

REGISTER_LAYOUT(MultiPageObject, LayoutType::kMultiPageObject);

}  // namespace partition
}  // namespace bcache2

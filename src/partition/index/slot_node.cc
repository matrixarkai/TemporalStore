// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/index/slot_node.h"

#include <string>
#include <utility>

#include "partition/index/index.h"
#include "partition/index/layout/multi_page_object.h"
#include "partition/index/layout/single_object.h"
#include "partition/index/layout/single_page_object.h"

namespace bcache2 {
namespace partition {

LayoutType SlotNode::CurrentLayout() const {
    if (pointer_ == 0) {
        return LayoutType::kSinglePage;
    }
    return LayoutManager::ExtractLayoutType(reinterpret_cast<const uint8_t*>(address_));
}

Status SlotNode::NewPage(const WriteOptions& opts, const PageIndex& page) {
    if ((page.page_id != 0 || page.object_id != 0) &&
        CurrentLayout() != LayoutType::kMultiPageObject) {
        // need MultiPageObject layout to accept page/object id is not 0
        TransformMultiPageObject(opts.allocator);
        return NewPage(opts, page);
    }

    if (pointer_ == 0) {
        if (!trivial_page_ && page.page_id == 0 && page.object_id == 0) {
            if (opts.update_if_exist) {
                TransformPageIn(page);
                return Status::OK();
            }
            return Status::AlreadyExists("page already exists");
        }

        if (!trivial_page_) {
            // current page is used
            TransformLayout(opts.allocator, sizeof(page), 0, UINT8_MAX);
            return NewPage(opts, page);
        }

        TransformPageIn(page);
        return Status::OK();
    }

    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));
    Status status = layout->NewPage(opts, page);
    if (status.IsFailedPrecondition()) {
        TransformLayout(opts.allocator, sizeof(page), 0, UINT8_MAX);
        return NewPage(opts, page);
    }
    return status;
}

Status SlotNode::UpdatePage(const WriteOptions& opts, uint8_t object_id, uint16_t page_id,
                            const PageIndex& new_page) {
    if (pointer_ == 0) {
        if (!trivial_page_ && object_id == 0 && page_id == 0) {
            TransformPageIn(new_page);
            return Status::OK();
        }
        return Status::NotFound("page not found");
    }

    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));
    return layout->UpdatePage(object_id, page_id, new_page);
}

Status SlotNode::DeletePage(const WriteOptions& opts, uint8_t object_id, uint16_t page_id) {
    if (pointer_ == 0) {
        if (!trivial_page_ && object_id == 0 && page_id == 0) {
            trivial_page_ = true;
            address_ = 0;
            return Status::OK();
        }
        return Status::NotFound("page not found");
    }

    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));
    Status status = layout->DeletePage(object_id, page_id);
    if (status.IsUnmatched()) {
        TransformLayout(opts.allocator, 0, 0, UINT8_MAX);
        return Status::OK();
    }
    return status;
}

void SlotNode::ClearPages(const WriteOptions& opts) {
    if (pointer_ == 0) {
        trivial_page_ = true;
        return;
    }
    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));
    Status status = layout->ClearPages();
    if (LIKELY(status.IsUnmatched())) {
        TransformLayout(opts.allocator, 0, 0, UINT8_MAX);
    }
}

Status SlotNode::FindPage(uint8_t object_id, uint16_t page_id, PageIndex* page) const {
    if (pointer_ == 0) {
        if (!trivial_page_ && object_id == 0 && page_id == 0) {
            TransformPageOut(page);
            return Status::OK();
        }
        return Status::NotFound("page not found");
    }

    auto layout =
        LayoutManager::GenReadonlyLayout(CurrentLayout(), reinterpret_cast<uint8_t*>(address_));
    return layout->FindPage(object_id, page_id, page);
}

std::vector<PageIndex> SlotNode::GetPages() const {
    std::vector<PageIndex> pages;
    if (pointer_ == 0) {
        if (!trivial_page_) {
            pages.emplace_back(PageIndex{});
            TransformPageOut(&pages.back());
        }
        return pages;
    }
    auto layout =
        LayoutManager::GenReadonlyLayout(CurrentLayout(), reinterpret_cast<uint8_t*>(address_));
    return layout->GetPages();
}

size_t SlotNode::GetPageNum() const {
    if (pointer_ == 0) {
        return trivial_page_ ? 0 : 1;
    }
    auto layout =
        LayoutManager::GenReadonlyLayout(CurrentLayout(), reinterpret_cast<uint8_t*>(address_));
    return layout->GetPageNum();
}

Status SlotNode::NewObjectWithId(const WriteOptions& opts, uint8_t object_id, uint8_t model_id,
                                 const absl::string_view& key, Object* object) {
    if (pointer_ == 0) {
        TransformLayout(opts.allocator, 0, Object::ComputeRawObjectSize(key.size(), model_id),
                        object_id);
        return NewObjectWithId(opts, object_id, model_id, key, object);
    }

    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));
    Status status;
    if (object_id < UINT8_MAX) {
        status = layout->NewObjectWithId(opts, object_id, model_id, key, object);
    } else {
        status = layout->NewObject(opts, model_id, key, object);
    }

    if (status.IsFailedPrecondition()) {
        TransformLayout(opts.allocator, 0, Object::ComputeRawObjectSize(key.size(), model_id),
                        object_id);
        return NewObjectWithId(opts, object_id, model_id, key, object);
    }
    return status;
}

Status SlotNode::NewObject(const WriteOptions& opts, uint8_t model_id, const absl::string_view& key,
                           Object* object) {
    return NewObjectWithId(opts, UINT8_MAX, model_id, key, object);
}

Status SlotNode::DeleteObject(const WriteOptions& opts, const absl::string_view& key) {
    if (UNLIKELY(pointer_ == 0)) {
        return Status::NotFound("object not found");
    }

    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));

    Object object;
    Status status = layout->FindObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    SetObjectTtl(opts, object.ObjectId(), 0);

    status = layout->DeleteObject(key);
    if (status.IsUnmatched()) {
        TransformLayout(opts.allocator, 0, 0, UINT8_MAX);
        return Status::OK();
    }
    return status;
}

Status SlotNode::FindObject(const absl::string_view& key, Object* object) const {
    std::vector<Object> objs;
    if (UNLIKELY(pointer_ == 0)) {
        return Status::NotFound("object not found");
    }

    auto layout =
        LayoutManager::GenReadonlyLayout(CurrentLayout(), reinterpret_cast<uint8_t*>(address_));
    return layout->FindObject(key, object);
}

std::vector<Object> SlotNode::GetObjects() const {
    std::vector<Object> objs;
    if (UNLIKELY(pointer_ == 0)) {
        return objs;
    }
    auto layout =
        LayoutManager::GenReadonlyLayout(CurrentLayout(), reinterpret_cast<uint8_t*>(address_));
    return layout->GetObjects();
}

size_t SlotNode::GetObjectNum() const {
    if (UNLIKELY(pointer_ == 0)) {
        return 0;
    }
    auto layout =
        LayoutManager::GenReadonlyLayout(CurrentLayout(), reinterpret_cast<uint8_t*>(address_));
    return layout->GetObjectNum();
}

void SlotNode::ClearObjects(const WriteOptions& opts) {
    if (UNLIKELY(pointer_ == 0)) {
        return;
    }
    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));
    Status status = layout->ClearObjects();
    if (LIKELY(status.IsUnmatched())) {
        TransformLayout(opts.allocator, 0, 0, UINT8_MAX);
    }
}

void SlotNode::SetLastUsed(const WriteOptions& opts, uint32_t last_used) {
    if (UNLIKELY(pointer_ == 0)) {
        // last_used is meaningless in single page layout
        return;
    }
    auto layout = LayoutManager::GenLayout(CurrentLayout(), opts.allocator,
                                           reinterpret_cast<uint8_t*>(address_));
    return layout->SetLastUsed(last_used);
}

uint32_t SlotNode::GetLastUsed() const {
    if (UNLIKELY(pointer_ == 0)) {
        // last_used is meaningless in single page layout
        return 0;
    }
    auto layout =
        LayoutManager::GenReadonlyLayout(CurrentLayout(), reinterpret_cast<uint8_t*>(address_));
    return layout->GetLastUsed();
}

// given new pages & objects, decide which layout to use and transform into it
void SlotNode::TransformLayout(Allocator* allocator, size_t with_new_page_size,
                               size_t with_new_object_size, uint8_t new_object_id) {
    static_assert(sizeof(uint64_t) == sizeof(void*));

    LayoutType origin_layout = CurrentLayout();
    uint64_t origin_address = address_;
    BYTE_DEFER({
        if (origin_layout == LayoutType::kSinglePage) {
            // no need to destroy
            return;
        }
        LayoutManager::GenLayout(origin_layout, allocator,
                                 reinterpret_cast<uint8_t*>(origin_address))
            ->Destroy();
    });

    uint32_t last_used = GetLastUsed();
    std::vector<PageIndex> pages = GetPages();
    std::vector<Object> objects = GetObjects();
    LayoutType target_layout = LayoutManager::GetFitLayoutType(
        pages, objects, with_new_page_size != 0, with_new_object_size != 0, new_object_id);

    if (target_layout == LayoutType::kSinglePage) {
        BYTE_ASSERT(objects.size() == 0) << "invalid target layout";
        BYTE_ASSERT(pages.size() <= 1) << "invalid target layout";
        if (!pages.empty()) {
            const auto& page = pages.front();
            BYTE_ASSERT(page.page_id == 0) << "invalid target layout";
            BYTE_ASSERT(page.object_id == 0) << "invalid target layout";
            TransformPageIn(pages.front());
        } else {
            address_ = 0;
            trivial_page_ = true;
        }
        pointer_ = 0;
        return;
    }

    size_t with_object_size = with_new_object_size;
    if (target_layout != LayoutType::kMultiPageObject && !objects.empty()) {
        BYTE_ASSERT(with_object_size == 0);
        with_object_size = objects.front().RawSize();
    }

    uint8_t* raw_layout_buf =
        LayoutManager::GenRawLayoutBuf(target_layout, allocator, with_object_size);
    LayoutManager::GenLayout(target_layout, allocator, raw_layout_buf)
        ->ConstructFrom(pages, objects, last_used);
    address_ = reinterpret_cast<uint64_t>(raw_layout_buf);
    pointer_ = 1;
}

void SlotNode::TransformMultiPageObject(Allocator* allocator) {
    LayoutType origin_layout = CurrentLayout();
    uint64_t origin_address = address_;
    BYTE_DEFER({
        if (origin_layout == LayoutType::kSinglePage) {
            // no need to destroy
            return;
        }
        LayoutManager::GenLayout(origin_layout, allocator,
                                 reinterpret_cast<uint8_t*>(origin_address))
            ->Destroy();
    });

    uint8_t* raw_layout_buf =
        LayoutManager::GenRawLayoutBuf(LayoutType::kMultiPageObject, allocator, 0);
    LayoutManager::GenLayout(LayoutType::kMultiPageObject, allocator, raw_layout_buf)
        ->ConstructFrom(GetPages(), GetObjects(), GetLastUsed());
    address_ = reinterpret_cast<uint64_t>(raw_layout_buf);
    pointer_ = 1;
}

void SlotNode::TransformPageIn(const PageIndex& page) {
    BYTE_ASSERT(page.object_id == 0);
    BYTE_ASSERT(page.page_id == 0);
    page_dirty_ = page.dirty;
    page_in_log_ = page.page_in_log;
    page_size_ = page.page_size;
    address_ = page.address;
    trivial_page_ = false;
    model_id_ = page.model_id;
}

void SlotNode::TransformPageOut(PageIndex* page) const {
    page->object_id = 0;
    page->page_id = 0;
    page->dirty = page_dirty_;
    page->page_in_log = page_in_log_;
    page->page_size = page_size_;
    page->address = address_;
    page->model_id = model_id_;
}

}  // namespace partition
}  // namespace bcache2

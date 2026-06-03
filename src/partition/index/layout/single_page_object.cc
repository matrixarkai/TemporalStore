// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/index/layout/single_page_object.h"

#include <string>
#include <utility>

#include "common/allocator.h"

namespace bcache2 {
namespace partition {

void SinglePageObject::ConstructFrom(const std::vector<PageIndex>& rpages,
                                     const std::vector<Object>& robjs, uint32_t last_used) {
    BYTE_ASSERT(rpages.size() <= 1);
    BYTE_ASSERT(robjs.size() <= 1);

    // header
    structure_->header.magic = LayoutType::kSinglePageObject;
    structure_->header.last_used = last_used;

    // page
    if (rpages.empty()) {
        structure_->page_address = 0;
        structure_->trivial_page = true;
    } else {
        TransformPageIn(rpages.front());
    }

    // object
    if (robjs.empty()) {
        Object::Construct(structure_->raw_object_buf);
    } else {
        Object::ConstructFrom(structure_->raw_object_buf, robjs.front());
    }
}

void SinglePageObject::Destroy() {
    Object obj(0, structure_->raw_object_buf);
    if (!obj.Trivial()) {
        Object::Destroy(obj.RawBuf());
    }
    allocator_->Deallocate(structure_);
}

Status SinglePageObject::NewPage(const WriteOptions& opts, const PageIndex& page) {
    if (page.object_id != 0 || page.page_id != 0) {
        return Status::FailedPrecondition("invalid layout");
    }

    if (structure_->trivial_page || opts.update_if_exist) {
        TransformPageIn(page);
        return Status::OK();
    }
    return Status::AlreadyExists("page already exist");
}

Status SinglePageObject::UpdatePage(uint8_t object_id, uint16_t page_id,
                                    const PageIndex& new_page) {
    if (!structure_->trivial_page && object_id == 0 && page_id == 0) {
        TransformPageIn(new_page);
        return Status::OK();
    }
    return Status::NotFound("page not found");
}

Status SinglePageObject::DeletePage(uint8_t object_id, uint16_t page_id) {
    if (!structure_->trivial_page && object_id == 0 && page_id == 0) {
        structure_->page_address = 0;
        structure_->trivial_page = true;
        return Status::Unmatched("delete success but layout unmatched");
    }
    return Status::NotFound("page not found");
}

Status SinglePageObject::ClearPages() {
    structure_->trivial_page = true;
    // SinglePageObject -> SingleObject
    return Status::Unmatched("clear success but layout unmatched");
}

Status SinglePageObject::FindPage(uint8_t object_id, uint16_t page_id, PageIndex* page) const {
    if (!structure_->trivial_page && object_id == 0 && page_id == 0) {
        TransformPageOut(page);
        return Status::OK();
    }
    return Status::NotFound("page not found");
}

std::vector<PageIndex> SinglePageObject::GetPages() const {
    std::vector<PageIndex> pages;
    if (!structure_->trivial_page) {
        pages.emplace_back(PageIndex{});
        TransformPageOut(&pages.back());
    }
    return pages;
}

Status SinglePageObject::NewObjectWithId(const WriteOptions& opts, uint8_t object_id,
                                         uint8_t model_id, const absl::string_view& key,
                                         Object* object) {
    if (object_id > 0) {
        return Status::FailedPrecondition("invalid layout");
    }
    Object obj(0, structure_->raw_object_buf);
    if (obj.Trivial()) {
        // current object not used
        Object::ConstructWithValues(obj.RawBuf(), model_id, key);
        if (object != nullptr) {
            *object = obj;
        }
        return Status::OK();
    }

    if (obj.Key() == key) {
        if (opts.return_if_already_exists) {
            *object = obj;
            return Status::OK();
        }
        return Status::AlreadyExists("object already exist");
    }
    return Status::FailedPrecondition("invalid layout");
}

Status SinglePageObject::NewObject(const WriteOptions& opts, uint8_t model_id,
                                   const absl::string_view& key, Object* object) {
    return NewObjectWithId(opts, 0, model_id, key, object);
}

Status SinglePageObject::DeleteObject(const absl::string_view& key) {
    Object obj(0, structure_->raw_object_buf);

    if (obj.Trivial() || obj.Key() != key) {
        return Status::NotFound("object not found");
    }

    Object::Destroy(obj.RawBuf());
    return Status::Unmatched("delete success but layout unmatched");
}

Status SinglePageObject::FindObject(const absl::string_view& key, Object* object) const {
    Object obj(0, structure_->raw_object_buf);

    if (obj.Trivial() || obj.Key() != key) {
        return Status::NotFound("object not found");
    }

    if (object != nullptr) {
        *object = obj;
    }
    return Status::OK();
}

Status SinglePageObject::ClearObjects() {
    Object obj(0, structure_->raw_object_buf);
    if (!obj.Trivial()) {
        Object::Destroy(obj.RawBuf());
    }
    return Status::Unmatched("delete success but layout unmatched");
}

std::vector<Object> SinglePageObject::GetObjects() const {
    std::vector<Object> objs;
    Object obj(0, structure_->raw_object_buf);
    if (!obj.Trivial()) {
        objs.emplace_back(std::move(obj));
    }
    return objs;
}

size_t SinglePageObject::GetObjectNum() const {
    Object obj(0, structure_->raw_object_buf);
    if (!obj.Trivial()) {
        return 1;
    }
    return 0;
}

void SinglePageObject::TransformPageIn(const PageIndex& page) {
    BYTE_ASSERT(page.object_id == 0);
    BYTE_ASSERT(page.page_id == 0);
    structure_->page_dirty = page.dirty;
    structure_->page_in_log = page.page_in_log;
    structure_->page_size = page.page_size;
    structure_->page_address = page.address;
    structure_->trivial_page = false;
    structure_->model_id = page.model_id;
}

void SinglePageObject::TransformPageOut(PageIndex* page) const {
    page->object_id = 0;
    page->page_id = 0;
    page->dirty = structure_->page_dirty;
    page->page_in_log = structure_->page_in_log;
    page->page_size = structure_->page_size;
    page->address = structure_->page_address;
    page->model_id = structure_->model_id;
}

}  // namespace partition
}  // namespace bcache2

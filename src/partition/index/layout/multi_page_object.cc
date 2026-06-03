// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/index/layout/multi_page_object.h"

#include <byte/include/assert.h>

#include <string>
#include <utility>

namespace bcache2 {
namespace partition {

void MultiPageObject::ConstructFrom(const std::vector<PageIndex>& rpages,
                                    const std::vector<Object>& robjs, uint32_t last_used) {
    new (structure_) Structure(allocator_);

    // header
    structure_->header.magic = LayoutType::kMultiPageObject;
    structure_->header.last_used = last_used;

    // pages
    for (auto& rpage : rpages) {
        structure_->pages.emplace_back(rpage);
    }

    // objects
    for (auto& robj : robjs) {
        size_t size = sizeof(ObjectWithId) + robj.RawSize();
        auto* object =
            reinterpret_cast<ObjectWithId*>(allocator_->Allocate(size));  // allocated here
        object->object_id = robj.ObjectId();
        Object::ConstructFrom(object->raw_object_buf, robj);
        structure_->objects.emplace_back(object);
    }
    structure_->object_num = robjs.size();
}

void MultiPageObject::Destroy() {
    // destroy objects
    for (size_t i = 0; i < structure_->objects.size(); ++i) {
        if (structure_->objects[i] == nullptr) {
            // placeholder, no need destroy
            continue;
        }
        Object::Destroy(structure_->objects[i]->raw_object_buf);
        allocator_->Deallocate(structure_->objects[i]);
    }

    // clear objects&pages
    structure_->objects.clear();
    structure_->pages.clear();

    // clear layout buffer
    structure_->~Structure();  // <- invoke the destructor manually
    allocator_->Deallocate(structure_);
}

Status MultiPageObject::NewPage(const WriteOptions& opts, const PageIndex& new_page) {
    // go through all pages
    for (size_t i = 0; i < structure_->pages.size(); ++i) {
        PageIndex* page = &structure_->pages[i];
        if (page->object_id != new_page.object_id || page->page_id != new_page.page_id) {
            continue;
        }

        if (opts.update_if_exist) {
            *page = new_page;
            return Status::OK();
        }

        return Status::AlreadyExists("page already exists");
    }

    structure_->pages.emplace_back(new_page);
    return Status::OK();
}

Status MultiPageObject::UpdatePage(uint8_t object_id, uint16_t page_id, const PageIndex& new_page) {
    for (size_t i = 0; i < structure_->pages.size(); ++i) {
        PageIndex& page = structure_->pages[i];
        if (page.object_id == object_id && page.page_id == page_id) {
            page = new_page;
            return Status::OK();
        }
    }
    return Status::NotFound("page not found");
}

Status MultiPageObject::DeletePage(uint8_t object_id, uint16_t page_id) {
    bool found = false;
    for (size_t i = 0; i < structure_->pages.size(); ++i) {
        const PageIndex& page = structure_->pages[i];
        if (page.object_id == object_id && page.page_id == page_id) {
            std::swap(structure_->pages[i], structure_->pages.back());
            structure_->pages.pop_back();
            found = true;
            break;
        }
    }

    if (!found) {
        return Status::NotFound("page not found");
    }

    if (GetObjectNum() > 1 || GetPageNum() > 1) {
        return Status::OK();
    }
    return Status::Unmatched("success but layout unmatched");
}

Status MultiPageObject::ClearPages() {
    structure_->pages.clear();
    if (GetObjectNum() > 1) {
        return Status::OK();
    }
    return Status::Unmatched("success but layout unmatched");
}

Status MultiPageObject::FindPage(uint8_t object_id, uint16_t page_id, PageIndex* ret) const {
    for (size_t i = 0; i < structure_->pages.size(); ++i) {
        const PageIndex& page = structure_->pages[i];
        if (page.object_id == object_id && page.page_id == page_id) {
            if (ret != nullptr) {
                *ret = page;
            }
            return Status::OK();
        }
    }
    return Status::NotFound("page not found");
}

std::vector<PageIndex> MultiPageObject::GetPages() const {
    std::vector<PageIndex> pages;
    for (size_t i = 0; i < structure_->pages.size(); ++i) {
        pages.emplace_back(structure_->pages[i]);
    }
    return pages;
}

size_t MultiPageObject::GetPageNum() const { return structure_->pages.size(); }

Status MultiPageObject::NewObjectWithId(const WriteOptions& opts, uint8_t object_id,
                                        uint8_t model_id, const absl::string_view& key,
                                        Object* ret) {
    BYTE_ASSERT(object_id < UINT8_MAX) << "object id overflow";
    if (object_id >= structure_->objects.size()) {
        structure_->objects.resize(object_id + 1, nullptr);
    }
    // 1. objects[object_id] should be nullptr
    // 2. no other objects with the same key
    for (size_t i = 0; i < structure_->objects.size(); ++i) {
        if (structure_->objects[i] == nullptr) {
            continue;
        }
        if (object_id == i) {
            return Status::AlreadyExists("object_id already exists");
        }

        auto* object_with_id = structure_->objects[i];
        Object obj(object_with_id->object_id, object_with_id->raw_object_buf);
        BYTE_ASSERT(!obj.Trivial());
        if (obj.Key() == key) {
            if (opts.return_if_already_exists) {
                *ret = obj;
                return Status::OK();
            }
            return Status::AlreadyExists("object already exists");
        }
    }

    size_t size = sizeof(ObjectWithId) + Object::ComputeRawObjectSize(key.size(), model_id);
    auto* object = reinterpret_cast<ObjectWithId*>(allocator_->Allocate(size));
    object->object_id = object_id;
    Object::ConstructWithValues(object->raw_object_buf, model_id, key);

    ++structure_->object_num;
    structure_->objects[object_id] = object;
    if (ret != nullptr) {
        *ret = Object(object->object_id, object->raw_object_buf);
    }

    return Status::OK();
}

Status MultiPageObject::NewObject(const WriteOptions& opts, uint8_t model_id,
                                  const absl::string_view& key, Object* ret) {
    uint8_t new_object_id = structure_->objects.size();
    // try to assign the smallest object ID
    for (size_t i = 0; i < structure_->objects.size(); ++i) {
        if (structure_->objects[i] == nullptr) {
            new_object_id = i;
            break;
        }
    }
    return NewObjectWithId(opts, new_object_id, model_id, key, ret);
}

Status MultiPageObject::DeleteObject(const absl::string_view& key) {
    bool deleted = false;
    for (size_t i = 0; i < structure_->objects.size(); ++i) {
        if (structure_->objects[i] == nullptr) {
            continue;
        }

        auto* object_with_id = structure_->objects[i];
        Object obj(object_with_id->object_id, object_with_id->raw_object_buf);
        BYTE_ASSERT(!obj.Trivial());

        if (obj.Key() == key) {
            // destruct
            Object::Destroy(obj.RawBuf());

            // delete buffer and make a placeholder
            // TODO(wangtai.10): placeholder GC
            allocator_->Deallocate(structure_->objects[i]);
            structure_->objects[i] = nullptr;
            --structure_->object_num;
            deleted = true;
            break;  // only the first one will be deleted
        }
    }

    if (!deleted) {
        return Status::NotFound("object not found");
    }

    if (GetObjectNum() > 1 || GetPageNum() > 1) {
        return Status::OK();
    }
    if (GetObjectNum() == 1 && structure_->objects[0] == nullptr) {
        return Status::OK();
    }

    return Status::Unmatched("success but layout unmatched");
}

Status MultiPageObject::FindObject(const absl::string_view& key, Object* ret) const {
    for (size_t i = 0; i < structure_->objects.size(); ++i) {
        if (structure_->objects[i] == nullptr) {
            continue;
        }

        auto* object_with_id = structure_->objects[i];
        Object obj(object_with_id->object_id, object_with_id->raw_object_buf);
        BYTE_ASSERT(!obj.Trivial());

        if (obj.Key() == key) {
            if (ret != nullptr) {
                *ret = obj;
            }
            return Status::OK();
        }
    }
    return Status::NotFound("object not found");
}

Status MultiPageObject::ClearObjects() {
    for (size_t i = 0; i < structure_->objects.size(); ++i) {
        if (structure_->objects[i] == nullptr) {
            continue;
        }

        auto* object_with_id = structure_->objects[i];
        Object obj(object_with_id->object_id, object_with_id->raw_object_buf);
        BYTE_ASSERT(!obj.Trivial());

        Object::Destroy(obj.RawBuf());
        allocator_->Deallocate(structure_->objects[i]);
    }
    structure_->objects.clear();
    structure_->object_num = 0;

    if (GetPageNum() > 1) {
        return Status::OK();
    }
    return Status::Unmatched("success but layout unmatched");
}

std::vector<Object> MultiPageObject::GetObjects() const {
    std::vector<Object> objs;
    for (size_t i = 0; i < structure_->objects.size(); ++i) {
        if (structure_->objects[i] == nullptr) {
            continue;
        }

        auto* object_with_id = structure_->objects[i];
        Object obj(object_with_id->object_id, object_with_id->raw_object_buf);
        BYTE_ASSERT(!obj.Trivial());
        objs.emplace_back(std::move(obj));
    }
    return objs;
}

size_t MultiPageObject::GetObjectNum() const { return structure_->object_num; }
}  // namespace partition
}  // namespace bcache2

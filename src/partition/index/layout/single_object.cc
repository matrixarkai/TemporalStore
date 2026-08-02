// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/index/layout/single_object.h"

#include <string>
#include <utility>

#include "common/allocator.h"

namespace bcache2 {
namespace partition {

void SingleObject::ConstructFrom(const std::vector<PageIndex>& rpages,
                                 const std::vector<Object>& robjs, uint32_t last_used) {
    BYTE_ASSERT(rpages.size() == 0);  // no pages allowed
    BYTE_ASSERT(robjs.size() <= 1);   // at most one object allowed

    structure_->header.magic = LayoutType::kSingleObject;
    structure_->header.last_used = last_used;

    if (robjs.empty()) {
        Object::Construct(structure_->raw_buf);
    } else {
        Object::ConstructFrom(structure_->raw_buf, robjs.front());
    }
}

void SingleObject::Destroy() {
    Object obj(0, structure_->raw_buf);
    if (!obj.Trivial()) {
        Object::Destroy(obj.RawBuf());
    }
    allocator_->Deallocate(structure_);
}

Status SingleObject::NewObjectWithId(const WriteOptions& opts, uint8_t object_id, uint8_t model_id,
                                     const absl::string_view& key, Object* object) {
    if (object_id > 0) {
        return Status::FailedPrecondition("invalid layout");
    }
    Object obj(0, structure_->raw_buf);
    if (obj.Trivial()) {
        // current object not used
        Object::ConstructWithValues(structure_->raw_buf, model_id, key);
        if (object != nullptr) {
            *object = obj;
        }
        return Status::OK();
    }
    // as this is a single object layout, we cannot create another object
    if (obj.Key() == key) {
        if (opts.return_if_already_exists) {
            *object = obj;
            return Status::OK();
        }
        return Status::AlreadyExists("object already exist");
    }
    return Status::FailedPrecondition("invalid layout");
}

Status SingleObject::NewObject(const WriteOptions& opts, uint8_t model_id,
                               const absl::string_view& key, Object* object) {
    return NewObjectWithId(opts, 0, model_id, key, object);
}

Status SingleObject::DeleteObject(const absl::string_view& key) {
    Object obj(0, structure_->raw_buf);

    if (obj.Trivial() || obj.Key() != key) {
        return Status::NotFound("object not found");
    }

    Object::Destroy(structure_->raw_buf);
    return Status::Unmatched("delete success but layout unmatched");
}

Status SingleObject::FindObject(const absl::string_view& key, Object* object) const {
    Object obj(0, structure_->raw_buf);

    if (obj.Trivial() || obj.Key() != key) {
        return Status::NotFound("object not found");
    }

    if (object != nullptr) {
        *object = obj;
    }
    return Status::OK();
}

Status SingleObject::ClearObjects() {
    Object obj(0, structure_->raw_buf);
    if (!obj.Trivial()) {
        Object::Destroy(obj.RawBuf());
    }
    return Status::Unmatched("delete success but layout unmatched");
}

std::vector<Object> SingleObject::GetObjects() const {
    std::vector<Object> objs;
    Object obj(0, structure_->raw_buf);
    if (!obj.Trivial()) {
        objs.emplace_back(std::move(obj));
    }
    return objs;
}

size_t SingleObject::GetObjectNum() const {
    Object obj(0, structure_->raw_buf);
    if (!obj.Trivial()) {
        return 1;
    }
    return 0;
}

}  // namespace partition
}  // namespace bcache2

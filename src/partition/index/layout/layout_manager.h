// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <vector>

#include "partition/index/layout/header.h"
#include "partition/index/layout/layout.h"
#include "partition/index/page_index.h"
#include "partition/storage/object.h"

#define REGISTER_LAYOUT(LayoutClass, LayoutType) \
    INITIALIZE({ LayoutManager::RegisterLayout<LayoutClass>(LayoutType); })

namespace bcache2 {
namespace partition {

class LayoutManager {
 public:
    template <typename LayoutClass>
    static void RegisterLayout(LayoutType type) {
        GetLayoutConstructor(type) = [](Allocator* allocator, uint8_t* raw_layout_buf) {
            static thread_local LayoutClass layout;
            layout.SetAllocator(allocator);
            layout.SetRawBuf(raw_layout_buf);
            return &layout;
        };

        GetRawLayoutCreator(type) = [](Allocator* allocator, size_t with_object_size) {
            return reinterpret_cast<uint8_t*>(
                allocator->Allocate(LayoutClass::RawLayoutSize(with_object_size)));
        };
    }

    static Layout* GenLayout(LayoutType type, Allocator* allocator, uint8_t* raw_layout_buf) {
        return GetLayoutConstructor(type)(allocator, raw_layout_buf);
    }

    static const Layout* GenReadonlyLayout(LayoutType type, uint8_t* raw_layout_buf) {
        return GenLayout(type, nullptr, raw_layout_buf);
    }

    static uint8_t* GenRawLayoutBuf(LayoutType type, Allocator* allocator,
                                    size_t with_object_size) {
        return GetRawLayoutCreator(type)(allocator, with_object_size);
    }
    // from uint8_t* to Header* (4 Bytes), and then read the LayoutType field
    static LayoutType ExtractLayoutType(const uint8_t* raw_layout_buf) {
        return reinterpret_cast<const Header*>(raw_layout_buf)->magic;
    }
    // decides which layout to use
    static LayoutType GetFitLayoutType(const std::vector<PageIndex>& origin_pages,
                                       const std::vector<Object>& origin_objects,
                                       bool with_new_page, bool with_new_object,
                                       uint8_t new_object_id) {
        size_t target_page_num = origin_pages.size() + (with_new_page ? 1 : 0);
        size_t target_object_num = origin_objects.size() + (with_new_object ? 1 : 0);

        if (target_object_num > 1 || target_page_num > 1 ||
            (new_object_id > 0 && new_object_id < UINT8_MAX)) {
            return LayoutType::kMultiPageObject;
        }

        if (!origin_objects.empty() && origin_objects.front().ObjectId() > 0) {
            return LayoutType::kMultiPageObject;
        }

        if (!origin_pages.empty() &&
            (origin_pages.front().object_id > 0 || origin_pages.front().page_id > 0)) {
            return LayoutType::kMultiPageObject;
        }

        if (target_object_num == 1 && target_page_num == 1) {
            return LayoutType::kSinglePageObject;
        }

        if (target_object_num == 1 && target_page_num == 0) {
            return LayoutType::kSingleObject;
        }

        return LayoutType::kSinglePage;
    }

 private:
    static std::function<Layout*(Allocator*, uint8_t*)>& GetLayoutConstructor(LayoutType type) {
        static std::function<Layout*(Allocator*, uint8_t*)> layout_constructor[kLayoutCount];
        return layout_constructor[type];
    }
    static std::function<uint8_t*(Allocator*, size_t)>& GetRawLayoutCreator(LayoutType type) {
        static std::function<uint8_t*(Allocator*, size_t)> raw_layout_creator[kLayoutCount];
        return raw_layout_creator[type];
    }

    DISALLOW_INSTANTIATE(LayoutManager);
};

}  // namespace partition
}  // namespace bcache2

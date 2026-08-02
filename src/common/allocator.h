// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <malloc.h>
#include <protocol/info.pb.h>

#include <limits>
#include <memory>
#include <utility>

namespace bcache2 {

// just a wrapper of malloc with stats
class Allocator {
 public:
    template <typename T>
    class StlWrapper;

    Allocator() = default;
    ~Allocator() = default;

    void* Allocate(size_t size) {
        void* p = malloc(size);
        stats_.set_alloced_size(stats_.alloced_size() +
                                malloc_usable_size(p));  // TODO(wangtai.10): cross platform
        stats_.set_alloc_cnt(stats_.alloc_cnt() + 1);
        return p;
    }

    void Deallocate(void* p) {
        stats_.set_alloced_size(stats_.alloced_size() -
                                malloc_usable_size(p));  // TODO(wangtai.10): cross platform
        stats_.set_dealloc_cnt(stats_.dealloc_cnt() + 1);
        free(p);
    }

    const AllocatorStats& GetStats() const { return stats_; }

    static Allocator* DefaultAllocator() {
        static Allocator allocator;
        return &allocator;
    }

 private:
    AllocatorStats stats_;

    DISALLOW_COPY_AND_ASSIGN(Allocator);
};

// for adapts std::allocator
template <typename T>
class Allocator::StlWrapper {
 public:
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using value_type = T;

    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_copy_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;
    using is_always_equal = std::false_type;

    template <typename U>
    struct rebind {
        using other = StlWrapper<U>;
    };

    StlWrapper() {}
    explicit StlWrapper(Allocator* impl) : impl_(impl) {}

    // TODO(wangtai.10): use rebind to count overhead memory
    template <typename U>
    StlWrapper(const StlWrapper<U>& other) : impl_(other.Impl()) {}

    pointer address(reference x) const { return std::addressof(x); }

    const_pointer address(const_reference x) const { return std::addressof(x); }

    pointer allocate(size_type n) {
        return reinterpret_cast<pointer>(impl_->Allocate(n * sizeof(T)));
    }

    void deallocate(pointer p, size_type) { return impl_->Deallocate(p); }

    size_type max_size() const { return std::numeric_limits<size_type>::max() / sizeof(T); }

    template <class U, class... Args>
    void construct(U* p, Args&&... args) {
        ::new (p) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* p) {
        p->~U();
    }

    Allocator* Impl() const { return impl_; }

 private:
    Allocator* impl_ = nullptr;
};

template <class T1, class T2>
bool operator!=(const Allocator::StlWrapper<T1>& lhs, const Allocator::StlWrapper<T2>& rhs) {
    return lhs.Impl() != rhs.Impl();
}

template <class T1, class T2>
bool operator==(const Allocator::StlWrapper<T1>& lhs, const Allocator::StlWrapper<T2>& rhs) {
    return lhs.Impl() == rhs.Impl();
}

}  // namespace bcache2

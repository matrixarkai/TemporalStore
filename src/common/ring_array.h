// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#if __has_include(<gtest/gtest_prod.h>)
#include <gtest/gtest_prod.h>
#else
#ifndef FRIEND_TEST
#define FRIEND_TEST(test_case_name, test_name)
#endif
#endif
#include <stdlib.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "common/algorithm.h"
#include "common/allocator.h"

namespace bcache2 {

template <typename T, typename Alloc = std::allocator<T>>
class RingArray {
 public:
    explicit RingArray(size_t size_bits) : RingArray(size_bits, Alloc()) {}
    RingArray(size_t size_bits, const Alloc& alloc)
        : allocator_(alloc), size_(1UL << size_bits), data_(allocator_.allocate(size_)) {
        for (size_t i = 0; i < size_; ++i) {
            allocator_.construct(data_ + i);
        }
    }
    ~RingArray() {
        for (size_t i = 0; i < size_; ++i) {
            allocator_.destroy(data_ + Index(front_ + i));
        }
        allocator_.deallocate(data_, size_);
    }

    void Push(T element) {
        if (UNLIKELY(rear_ >= front_ + size_)) {
            auto new_size = NextPowerOfTwo(size_ + 1);
            Resize(new_size);
        }

        data_[Index(rear_++)] = std::move(element);
    }

    void Pop() {
        BYTE_ASSERT(rear_ > front_);
        allocator_.destroy(data_ + Index(front_));
        allocator_.construct(data_ + Index(front_));
        front_++;
        if (UNLIKELY(Size() < (size_ / 3))) {
            Resize(size_ >> 1);
        }
    }

    T& operator[](size_t index) const {
        BYTE_ASSERT(index >= front_ && index - front_ < size_);
        return data_[Index(index)];
    }

    T& Front() {
        BYTE_ASSERT(rear_ > front_);
        return data_[Index(front_)];
    }

    const T& Front() const {
        BYTE_ASSERT(rear_ > front_);
        return data_[Index(front_)];
    }

    T& Back() {
        BYTE_ASSERT(rear_ > front_);
        return data_[Index(rear_ - 1)];
    }

    const T& Back() const {
        BYTE_ASSERT(rear_ > front_);
        return data_[Index(rear_ - 1)];
    }

    bool Empty() const { return rear_ == front_; }
    size_t FrontIndex() const { return front_; }
    size_t RearIndex() const { return rear_; }
    size_t Size() const { return rear_ - front_; }

 private:
    size_t Index(size_t index) const { return index % size_; }

    void Resize(size_t new_size) {
        BYTE_ASSERT(new_size >= Size());
        // alloc new data
        T* new_data = allocator_.allocate(new_size);
        for (size_t i = 0; i < new_size; ++i) {
            if (i < size_) {
                allocator_.construct(new_data + (front_ + i) % new_size,
                                     std::move(data_[Index(front_ + i)]));
            } else {
                allocator_.construct(new_data + (front_ + i) % new_size);
            }
        }

        // dealloc old data
        for (size_t i = 0; i < size_; ++i) {
            allocator_.destroy(data_ + Index(front_ + i));
        }
        allocator_.deallocate(data_, size_);

        // swap buffer
        size_ = new_size;
        data_ = new_data;
    }

    Alloc allocator_;
    size_t size_ = 1UL;
    size_t front_ = 0;
    size_t rear_ = 0;
    T* data_ = nullptr;

    FRIEND_TEST(RingArray, resize);
    DISALLOW_COPY_AND_ASSIGN(RingArray);
};

}  // namespace bcache2

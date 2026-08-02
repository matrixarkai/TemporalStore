// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <stddef.h>
#include <string.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

namespace bcache2 {

// space efficient but slow
template <typename T>
class CompactArray {
 public:
    using SizeType = uint16_t;

    CompactArray() = default;
    ~CompactArray() { Clear(); }

    size_t Size() const {
        if (buf_ == nullptr) {
            return 0;
        }
        return *reinterpret_cast<SizeType*>(buf_.get());
    }
    bool Empty() const { return Size() == 0; }

    void Resize(size_t new_size) {
        BYTE_ASSERT(new_size <= std::numeric_limits<SizeType>::max()) << "size too large";

        if (new_size == 0) {
            Clear();
            return;
        }

        size_t buf_size = sizeof(SizeType) + new_size * sizeof(T);
        std::unique_ptr<uint8_t[]> new_buf(new uint8_t[buf_size]);

        // set size
        SizeType* size = reinterpret_cast<SizeType*>(new_buf.get());
        *size = static_cast<SizeType>(new_size);

        // move origin elements
        MoveElements(new_buf.get(), new_size);
        buf_.swap(new_buf);
    }

    void PushBack(T&& ele) {
        Resize(Size() + 1);
        Back() = std::move(ele);
    }

    void PushBack(const T& ele) {
        Resize(Size() + 1);
        Back() = ele;
    }

    void PopBack() {
        BYTE_ASSERT(!Empty()) << "empty array";
        Resize(Size() - 1);
    }

    T& At(size_t idx) {
        BYTE_ASSERT(idx < Size()) << "idx overflow";
        T* start = reinterpret_cast<T*>(buf_.get() + sizeof(SizeType));
        return start[idx];
    }
    T& operator[](size_t idx) { return At(idx); }
    T& Back() { return At(Size() - 1); }

    void Clear() {
        size_t size = Size();
        if (UNLIKELY(size == 0)) {
            return;
        }

        T* start = reinterpret_cast<T*>(buf_.get() + sizeof(SizeType));
        for (size_t i = 0; i < size; ++i) {
            start[i].~T();
        }
        buf_.reset();
    }

 private:
    void MoveElements(uint8_t* new_buf, size_t new_size) {
        size_t size = Size();
        T* start = reinterpret_cast<T*>(new_buf + sizeof(SizeType));
        for (size_t i = 0; i < new_size; ++i) {
            if (i < size) {
                new (start + i) T(std::move(At(i)));
            } else {
                new (start + i) T();
            }
        }

        if (std::is_trivially_destructible<T>::value) {
            return;
        }

        for (size_t i = new_size; i < size; ++i) {
            At(i).~T();
        }
    }

    // [0, 2]: size
    // [3, ...): elements
    // TODO(wangtai.10): varint size
    std::unique_ptr<uint8_t[]> buf_;

    DISALLOW_COPY_AND_ASSIGN(CompactArray);
};
static_assert(sizeof(CompactArray<int>) == 8);

}  // namespace bcache2

// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <stddef.h>
#include <string.h>

namespace bcache2 {

// const char* with auto clean-up and deep copy
class Data {
 public:
    Data() : data_(nullptr), size_(0) {}
    ~Data() { delete[] data_; }

    Data(const void* data, size_t size) : data_(reinterpret_cast<const char*>(data)), size_(size) {}

    Data(Data& other) { Move(&other); }  // NOLINT(runtime/references)
    Data& operator=(Data& other) {       // NOLINT(runtime/references)
        Move(&other);
        return *this;
    }
    Data(Data&& other) { Move(&other); }
    Data& operator=(Data&& other) {
        Move(&other);
        return *this;
    }

    Data Copy() const;

    const char* data() const { return data_; }
    size_t size() const { return size_; }

 private:
    void Move(Data* other) {
        data_ = other->data_;
        size_ = other->size_;
        other->data_ = nullptr;
        other->size_ = 0;
    }
    const char* data_ = nullptr;
    size_t size_ = 0;
};

inline Data Data::Copy() const {
    char* d = nullptr;
    if (LIKELY(data_ != nullptr)) {
        d = new char[size_];
        memcpy(d, data_, size_);
    }
    return Data(d, size_);
}

}  // namespace bcache2

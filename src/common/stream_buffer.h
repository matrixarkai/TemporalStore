// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "common/ring_array.h"

namespace bcache2 {

template <typename Delimiter>
class StreamBuffer {
 public:
    explicit StreamBuffer(size_t start) : real_start_(start), start_(start), length_(start) {}
    ~StreamBuffer() {}

    void Append(const char* data, size_t size) {
        std::string s = std::string(data, size);
        Append(std::move(s));
    }

    void Append(std::string data) {
        if (LIKELY(!data.empty())) {
            length_ += data.size();
            ring_datas_.Push(std::move(data));
        }
    }

    void AppendV(std::vector<std::string> datas) {
        for (auto& data : datas) {
            Append(std::move(data));
        }
    }

    void GetFrontData(void* data, size_t size) {
        BYTE_ASSERT(start_ + size <= length_);
        BYTE_ASSERT(start_ >= real_start_);
        size_t left_size = size;
        size_t buffer_index = ring_datas_.FrontIndex();
        size_t buffer_pos = start_ - real_start_;
        while (left_size > 0) {
            BYTE_ASSERT(buffer_pos < ring_datas_[buffer_index].size());
            size_t buffer_size = std::min(left_size, ring_datas_[buffer_index].size() - buffer_pos);
            memcpy(reinterpret_cast<char*>(data) + size - left_size,
                   ring_datas_[buffer_index].data() + buffer_pos, buffer_size);

            left_size -= buffer_size;
            buffer_index++;
            buffer_pos = 0;
        }
        BYTE_ASSERT(left_size == 0);
    }

    void PushDelimiter(Delimiter delimiter) {
        delimiters_.Push(std::make_pair(std::move(delimiter), length_));
    }

    bool HasDelimiter() const { return !delimiters_.Empty(); }

    const std::pair<Delimiter, size_t>& BackDelimiter() const { return delimiters_.Back(); }

    const std::pair<Delimiter, size_t>& FrontDelimiter() const { return delimiters_.Front(); }

    size_t DistanceWithLastDelimiter() const {
        return HasDelimiter() ? length_ - delimiters_.Back().second : length_ - start_;
    }

    size_t DistanceWithFirstDelimiter() const {
        return HasDelimiter() ? delimiters_.Front().second - start_ : length_ - start_;
    }

    // trim data and delimiters before `length` cursor
    void Trim(size_t length) {
        BYTE_ASSERT(length >= start_ && length <= length_);

        start_ = length;
        while (!ring_datas_.Empty() && length >= real_start_ + ring_datas_.Front().size()) {
            real_start_ += ring_datas_.Front().size();
            ring_datas_.Pop();
        }

        while (!delimiters_.Empty() && delimiters_.Front().second <= length) {
            delimiters_.Pop();
        }
    }

    size_t Start() const { return start_; }
    size_t Length() const { return length_; }
    size_t Size() const { return length_ - start_; }

 private:
    RingArray<std::string> ring_datas_{0};
    RingArray<std::pair<Delimiter, size_t>> delimiters_{0};
    size_t real_start_ = 0;
    size_t start_ = 0;
    size_t length_ = 0;

    DISALLOW_COPY_AND_ASSIGN(StreamBuffer);
};

}  // namespace bcache2

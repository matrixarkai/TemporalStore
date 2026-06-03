// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <cstdlib>
#include <cstring>

#include "blockcache/rdmareadcache/allocator/std_allocator.h"

namespace bcache2 {
class RDMAResponse {
    StdAllocator alloc_;
    size_t buf_size_;
    char* ptr_;

 public:
    RDMAResponse() : buf_size_(-1), ptr_(nullptr) {}
    explicit RDMAResponse(size_t sz);
    ~RDMAResponse();

    /**
     * @brief Init the response object
     *
     * @param sz allocate a buffer of capacity sz, which will be used
     * to store data
     */
    void Init(size_t sz);

    /**
     * @brief Fill the response object with data pointed by str
     *
     * @param str is the address of the data to be stored in response object
     * @param sz is the size of the data
     */
    void Fill(char* str, size_t sz);

    /**
     * @brief clear the data stored in response, reset the buf_size_ field to 0
     *
     */
    void Clear();

    /**
     * @brief Get the response object size
     *
     * @return size_t is the size of data stored in response, which is buf_size_
     */
    size_t GetRespSize();

    /**
     * @brief Get the data pointer stored in response
     *
     * @return char* is the pointer that points to the data stored in response
     */
    char* GetResponse();
};
}  // namespace bcache2

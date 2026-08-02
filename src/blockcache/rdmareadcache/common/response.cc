// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/common/response.h"

namespace bcache2 {

RDMAResponse::RDMAResponse(size_t sz) {
    buf_size_ = sz;
    ptr_ = alloc_.allocate(sz);
}

RDMAResponse::~RDMAResponse() {
    alloc_.free(ptr_, buf_size_);
    buf_size_ = -1;
}

void RDMAResponse::Init(size_t sz) {
    buf_size_ = sz;
    ptr_ = alloc_.allocate(sz);
}

void RDMAResponse::Fill(char* str, size_t sz) {
    if (ptr_ != nullptr) {
        if (buf_size_ != 0) {
            alloc_.free(ptr_, buf_size_);
        }
        buf_size_ = 0;
        ptr_ = nullptr;
    }
    buf_size_ = sz;
    ptr_ = alloc_.allocate(sz);
    memcpy(ptr_, str, sz);
}

void RDMAResponse::Clear() {
    alloc_.free(ptr_, buf_size_);
    buf_size_ = -1;
    ptr_ = nullptr;
}

size_t RDMAResponse::GetRespSize() { return buf_size_; }

char* RDMAResponse::GetResponse() { return ptr_; }

}  // namespace bcache2

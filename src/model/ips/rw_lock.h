// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

// #include <shared_mutex>
#include "model/ips/bthread_rwlock.h"

namespace bcache2 {
namespace ips {

using RWLock = GeneralBthreadRWLock;

class RW_shared_lock {
 public:
    explicit RW_shared_lock(RWLock& rl) : rl_(rl) { rl_.lock_shared(); }
    ~RW_shared_lock() { rl_.unlock_shared(); }

 private:
    RWLock& rl_;
};

class RW_unique_lock {
 public:
    explicit RW_unique_lock(RWLock& rl) : rl_(rl) { rl_.lock(); }
    ~RW_unique_lock() { rl_.unlock(); }

 private:
    RWLock& rl_;
};

}  // namespace ips
}  // namespace bcache2

// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <bthread/bthread.h>
#include <bthread/butex.h>
#include <butil/atomicops.h>

#include <cassert>

namespace bcache2 {
namespace ips {

class SingleWriterBthreadRWLock {
 public:
    SingleWriterBthreadRWLock() : flag_(bthread::butex_create_checked<unsigned int>()) {
        *flag_ = 0;
    }

    ~SingleWriterBthreadRWLock() { bthread::butex_destroy(flag_); }

    void lock_shared() { CHECK_EQ(0, ReadLock()); }

    void unlock_shared() { ReadUnlock(); }

    void lock() { CHECK_EQ(0, WriteLock()); }

    void unlock() { WriteUnlock(); }

 private:
    int ReadLock() {
        auto& f = *reinterpret_cast<butil::atomic<unsigned int>*>(flag_);
        unsigned int state;
    wait_stage:
        while ((state = f.load(std::memory_order_relaxed)) & kLockMask) {
            unsigned int after = state | kWaitMask;
            while (!(state & kWaitMask) &&
                   !f.compare_exchange_weak(state, after, std::memory_order_relaxed,
                                            std::memory_order_relaxed)) {
                if (!(state & kLockMask)) {
                    goto lock_stage;
                }
                after = state | kWaitMask;
            }
            if (bthread::butex_wait(flag_, after, nullptr) < 0 && errno != EWOULDBLOCK &&
                errno != EINTR) {
                return errno;
            }
        }
    lock_stage:
        if (f.fetch_add(1, std::memory_order_acquire) & kLockMask) {
            state = f.fetch_sub(1, std::memory_order_relaxed) - 1;
            if ((state & (kWaitMask | kReaderCountMask)) == kWaitMask) {
                state = f.fetch_and(~kWaitMask, std::memory_order_relaxed);
                if (state & kWaitMask) {
                    bthread::butex_wake_all(flag_);
                }
            }
            if (state & kLockMask) {
                goto wait_stage;
            } else {
                goto lock_stage;
            }
        }
        return 0;
    }

    void ReadUnlock() {
        auto& f = *reinterpret_cast<butil::atomic<unsigned int>*>(flag_);
        unsigned int state = f.fetch_sub(1, std::memory_order_release) - 1;
        if ((state & (kWaitMask | kReaderCountMask)) == kWaitMask) {
            assert(state & kLockMask);
            state = f.fetch_and(~kWaitMask, std::memory_order_relaxed);
            if (state & kWaitMask) {
                bthread::butex_wake_all(flag_);
            }
        }
    }

    int WriteLock() {
        auto& f = *reinterpret_cast<butil::atomic<unsigned int>*>(flag_);
        unsigned int state;
        if ((state = f.fetch_or(kLockMask, std::memory_order_acquire))) {
            assert(!(state & (kLockMask | kWaitMask)));
            state |= kLockMask;
            do {
                assert(state & kLockMask);
                unsigned int after = state | kWaitMask;
                while (!(state & kWaitMask) &&
                       !f.compare_exchange_weak(state, after, std::memory_order_relaxed,
                                                std::memory_order_acquire)) {
                    assert(state & kLockMask);
                    if (!(state & kReaderCountMask)) {
                        return 0;
                    }
                    after = state | kWaitMask;
                }
                if (bthread::butex_wait(flag_, after, nullptr) < 0 && errno != EWOULDBLOCK &&
                    errno != EINTR) {
                    WriteUnlock();
                    return errno;
                }
            } while ((state = f.load(std::memory_order_acquire)) & kReaderCountMask);
        }
        return 0;
    }

    void WriteUnlock() {
        auto& f = *reinterpret_cast<butil::atomic<unsigned int>*>(flag_);
        unsigned int state = f.fetch_and(~(kLockMask | kWaitMask), std::memory_order_release);
        assert(state & kLockMask);
        if (state & kWaitMask) {
            bthread::butex_wake_all(flag_);
        }
    }

 private:
    static constexpr unsigned int kLockMask = 1U << 31U;
    static constexpr unsigned int kWaitMask = 1U << 30U;
    static constexpr unsigned int kReaderCountMask = (1U << 30U) - 1U;

    unsigned int* const flag_;
};

class GeneralBthreadRWLock {
 public:
    void lock_shared() { internal_rwlock_.lock_shared(); }

    void unlock_shared() { internal_rwlock_.unlock_shared(); }

    void lock() {
        writer_mtx_.lock();
        internal_rwlock_.lock();
    }

    void unlock() {
        internal_rwlock_.unlock();
        writer_mtx_.unlock();
    }

 private:
    bthread::Mutex writer_mtx_;
    SingleWriterBthreadRWLock internal_rwlock_;
};

}  // namespace ips
}  // namespace bcache2

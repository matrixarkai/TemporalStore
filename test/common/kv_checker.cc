// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "test/common/kv_checker.h"

#include "common/time.h"

namespace bcache2 {

KvChecker::KvChecker() {}

KvChecker::~KvChecker() {
    BYTE_ASSERT(inflight_writes_.empty());
    BYTE_ASSERT_DEBUG(inflight_reads_count_ == 0);
}

uint64_t KvChecker::NewWrite(uint64_t* value) {
    WriteOp op;
    op.version = ++version_;
    op.start_time = GetCurrentTimeInNs();
    *value = op.version;
    inflight_writes_[op.version] = op;
    return op.version;
}

uint64_t KvChecker::NewRead() {
    ReadOp* op = new ReadOp;
    op->version = version_;
    op->start_time = GetCurrentTimeInNs();
    for (const auto& write : inflight_writes_) {
        op->candidate_values.push_back(write.second.version);
    }
    for (size_t i = candidate_writes_.FrontIndex(); i < candidate_writes_.RearIndex(); ++i) {
        op->candidate_values.push_back(candidate_writes_[i].version);
    }
    return reinterpret_cast<uint64_t>(op);
}

void KvChecker::FinishWrite(uint64_t handle, bool success) {
    auto it = inflight_writes_.find(handle);
    BYTE_ASSERT(it != inflight_writes_.end());
    WriteOp op = it->second;
    inflight_writes_.erase(it);

    if (op.end_time != 0) {
        return;
    }
    op.end_time = GetCurrentTimeInNs();
    candidate_writes_.Push(op);
    if (success) {
        ClearOldWrites(op.start_time, 0);
    }
}

bool KvChecker::FinishRead(uint64_t handle, bool success, uint64_t value) {
    std::unique_ptr<ReadOp> op(reinterpret_cast<ReadOp*>(handle));
    if (!success) {
        return true;
    }

    bool ok = value > op->version && value <= version_;
    auto it = inflight_writes_.find(value);
    if (it != inflight_writes_.end()) {
        it->second.end_time = GetCurrentTimeInNs();
        candidate_writes_.Push(it->second);
        ClearOldWrites(it->second.start_time, 0);
        ok = true;
    }
    for (const auto item : op->candidate_values) {
        if (item == value) {
            ok = true;
            break;
        }
    }

    if (!ok) {
        return ok;
    }

    ClearOldWrites(op->start_time, value);
    return true;
}

void KvChecker::ClearOldWrites(uint64_t time, uint64_t retain_version) {
    size_t retain_index = candidate_writes_.RearIndex();
    size_t index = candidate_writes_.FrontIndex();
    for (; index < candidate_writes_.RearIndex() && candidate_writes_[index].end_time <= time;
         ++index) {
        if (candidate_writes_[index].version == retain_version) {
            retain_index = index;
        }
    }
    if (retain_index != candidate_writes_.RearIndex()) {
        candidate_writes_[index - 1] = candidate_writes_[retain_index];
        index--;
    }
    while (candidate_writes_.FrontIndex() < index) candidate_writes_.Pop();
}

}  // namespace bcache2

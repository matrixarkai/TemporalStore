// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "stream/stream.h"

namespace bcache2 {
namespace partition {

class MemStream : public stream::Stream {
 public:
    explicit MemStream(std::shared_ptr<std::map<uint64_t, std::string>> committed) {
        uncommitted_.reset(new std::map<uint64_t, std::string>());
        committed_ = std::move(committed);
        if (!committed_->empty()) {
            idx_ = std::prev(committed_->end())->first;
            ++idx_;
        }
    }

    virtual ~MemStream() {}

    Status Load() override { return Status::OK(); }

    void Close(Closure<void>* callback) override;

    void Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                Closure<void>* callback) override;

    void AppendV(std::vector<std::string> data, uint64_t* id) override;

    void Append(std::string str, uint64_t* id) override;

    void Commit(Controller* ctrl, Closure<void>* callback) override;

    void Read(Controller* ctrl, uint64_t id, void* data, size_t size,
              Closure<void>* callback) override;
    stream::Stats Stat() override { return stream::Stats(); }
    void Truncate(uint64_t id) override {}
    StreamInfo GetInfo() override {
        StreamInfo tmp_;
        return tmp_;
    }
    Status RestoreInfo(const StreamInfo&) override {
        BYTE_ASSERT(false);
        return Status::OK();
    }
    stream::ScopedIterator NewIterator(size_t start_id, size_t end_id) override;

    std::shared_ptr<std::map<uint64_t, std::string>> Data() { return committed_; }

    void ReapMetrics() const override {}

    void UpdateConfig(const StreamConfig& config) override {}

 private:
    std::unique_ptr<std::map<uint64_t, std::string>> uncommitted_;
    std::shared_ptr<std::map<uint64_t, std::string>> committed_;
    uint64_t idx_ = 0;

    DISALLOW_COPY_AND_ASSIGN(MemStream);
};

class MemStreamIterator : public stream::Iterator {
 public:
    virtual ~MemStreamIterator() {}

    Status Next() override {
        auto data = stream_->Data();
        if (!seeked_) {
            iter_ = data->find(start_offset_);  // fixme
            seeked_ = true;
        }
        if (iter_ == data->end() || iter_->first == end_offset_) {
            return Status::OutOfRange("");
        }
        record_offset_ = iter_->first;
        record_ = absl::string_view(iter_->second);
        ++iter_;
        return Status::OK();
    }

    uint64_t Id() const override { return record_offset_; }
    absl::string_view Data() const override { return record_; }

 private:
    friend class MemStream;

    MemStreamIterator(MemStream* stream, size_t start_offset, size_t end_offset)
        : start_offset_(start_offset), end_offset_(end_offset), stream_(stream) {}

    uint64_t start_offset_ = 0;
    uint64_t end_offset_ = 0;

    uint64_t record_offset_ = 0;
    absl::string_view record_;

    MemStream* stream_;
    bool seeked_ = false;
    std::map<uint64_t, std::string>::iterator iter_;
};

inline void MemStream::Close(Closure<void>* callback) {
    committed_->insert(uncommitted_->cbegin(), uncommitted_->cend());
    uncommitted_->clear();
    if (callback != nullptr) {
        callback->Run();
    }
}

inline void MemStream::Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                              Closure<void>* callback) {
    std::string str(static_cast<const char*>(data), size);
    Append(str, id);
    ctrl->set_status(Status::OK());
    if (callback != nullptr) {
        callback->Run();
    }
}

inline void MemStream::AppendV(std::vector<std::string> data, uint64_t* id) {
    std::string str;
    for (auto& d : data) {
        str += d;
    }
    Append(str, id);
}

inline void MemStream::Append(std::string str, uint64_t* id) {
    *id = idx_++;
    (*uncommitted_)[*id] = str;
}

inline void MemStream::Commit(Controller* ctrl, Closure<void>* callback) {
    committed_->insert(uncommitted_->cbegin(), uncommitted_->cend());
    uncommitted_->clear();

    ctrl->set_status(Status::OK());
    if (callback != nullptr) {
        callback->Run();
    }
}

inline void MemStream::Read(Controller* ctrl, uint64_t id, void* data, size_t size,
                            Closure<void>* callback) {
    BYTE_ASSERT(false);
}

inline stream::ScopedIterator MemStream::NewIterator(size_t start_id, size_t end_id) {
    stream::ScopedIterator iter;
    iter.reset(new MemStreamIterator(this, start_id, end_id));
    return iter;
}

class PartitionMemEnv : public stream::Env {
 public:
    PartitionMemEnv()
        : index_data_(std::make_shared<std::map<uint64_t, std::string>>()),
          page_data_(std::make_shared<std::map<uint64_t, std::string>>()),
          oplog_data_(std::make_shared<std::map<uint64_t, std::string>>()) {}

    virtual ~PartitionMemEnv() {}

    void SetCondition(Controller* ctrl, const stream::Env::Condition& condition,
                      const std::string& condition_name, const std::string& condition_value) {
        BYTE_ASSERT(false);
    }

    void GetCondition(Controller* ctrl, const std::string& condition_name,
                      std::string* condition_value) {
        BYTE_ASSERT(false);
    }

    void OpenStream(Controller* ctrl, const stream::Env::Condition& condition,
                    const std::string& uri, const stream::Env::OpenOptions& options,
                    stream::Stream** stream) {
        Status status = Status::OK();
        if (uri.find("index") != std::string::npos) {
            *stream = new MemStream(index_data_);
        } else if (uri.find("page") != std::string::npos) {
            *stream = new MemStream(page_data_);
        } else if (uri.find("oplog") != std::string::npos) {
            *stream = new MemStream(oplog_data_);
        } else {
            status = Status::NotFound("");
        }
        ctrl->set_status(status);
    }

    void OpenStreamReader(Controller* ctrl, const std::string& uri,
                          const stream::Env::OpenOptions& options, stream::Stream** stream,
                          const StreamInfo&) {
        BYTE_ASSERT(false);
    }

 private:
    std::shared_ptr<std::map<uint64_t, std::string>> index_data_;
    std::shared_ptr<std::map<uint64_t, std::string>> page_data_;
    std::shared_ptr<std::map<uint64_t, std::string>> oplog_data_;
};

}  // namespace partition
}  // namespace bcache2

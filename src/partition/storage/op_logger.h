// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/include/assert.h>
#include <byte/include/macros.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/controller.h"
#include "model/property.h"
#include "partition/index/index.h"
#include "partition/storage/page_store.h"
#include "protocol/storage.pb.h"
#include "stream/stream.h"

namespace bcache2 {
namespace partition {

const uint32_t kOpLogVersion = 1UL;

struct LogItem;
class SlotContextManager;
struct CmdContext;
class Partition;

class OpLogger {
 public:
    class Iterator;
    using ScopedIterator = std::unique_ptr<Iterator>;

    struct Options {
        stream::Stream* stream = nullptr;
    };

    OpLogger(Partition* partition, Index* index, SlotContextManager* slot_context_manager,
             MetricsManager* metrics_manager);
    virtual ~OpLogger();

    void Init(Options opts) { stream_.reset(opts.stream); }
    void Close(Closure<void>* callback) {
        if (stream_) {
            stream_->Close(callback);
        } else if (callback) {
            callback->Run();
        }
    }

    // TODO(wangtai.10): delete page
    virtual void WriteKvLog(CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                            const absl::string_view& object_key, uint16_t model_id, std::string key,
                            std::string value, model::Property property);
    void WritePage(CmdContext* ctx, uint64_t slot_id, const absl::string_view& object_key,
                   uint16_t model_id, uint16_t object_id, uint16_t page_id, std::string page,
                   uint64_t ttl);
    void DeleteObject(uint64_t slot_id, uint64_t object_id, const absl::string_view& object_key,
                      uint16_t model_id);

    void WriteTtlLog(uint64_t slot_id, const absl::string_view& object_key, uint8_t model_id,
                     uint16_t object_id, uint64_t ttl);

    void Commit(Controller* ctrl, Closure<void>* callback);
    Status AppendReplayedLog(const storage::OpLog& oplog, uint64_t* log_id, uint32_t* log_size);

    void Truncate(uint64_t log_id);

    ScopedIterator NewIterator(uint64_t log_id);
    stream::ScopedIterator NewRawStreamIterator(uint64_t start_id, uint64_t end_id) {
        return stream_->NewIterator(start_id, end_id);
    }
    void ReadRawStream(Controller* ctrl, uint64_t id, void* data, size_t size,
                       Closure<void>* callback) {
        stream_->Read(ctrl, id, data, size, callback);
    }

    void ReadPage(Controller* ctrl, uint64_t slot_id, PageIndex index, PageInfo* page_info,
                  Closure<void>* callback);

    uint64_t Length() const { return stream_->Stat().length; }
    uint64_t PersistentLength() const { return stream_->Stat().persistent_length; }
    uint64_t Start() const { return stream_->Stat().start_record_id; }
    uint64_t CurrentSequence() const { return current_sequence_; }
    uint64_t UnDumpLength() const { return Length() - index_->GetDumpedLogId(); }

    OpLoggerInfo GetInfo() const {
        OpLoggerInfo info;
        *info.mutable_stream_info() = stream_->GetInfo();
        uint64_t durable_sequence = 0;
        if (info.stream_info().blob_infos_size() > 0) {
            durable_sequence = info.stream_info().blob_infos(
                info.stream_info().blob_infos_size() - 1).end_record_sequence();
        }
        info.set_current_sequence(current_sequence_ < durable_sequence
                                      ? current_sequence_
                                      : durable_sequence);
        return info;
    }

    Status RestoreInfo(const OpLoggerInfo& oplogger_info) {
        return stream_->RestoreInfo(oplogger_info.stream_info());
    }

    void UpdateConfig(const Config& config) { stream_->UpdateConfig(config.stream_config()); }

    void SetCurrentSequence(uint64_t current_sequence) { current_sequence_ = current_sequence; }
    void ReapMetrics() const { stream_->ReapMetrics(); }

    static PageInfo Log2Page(uint64_t log_id, uint64_t log_sequence, uint64_t log_size,
                             const storage::OpLog::LogItem& log_item) {
        PageInfo page;
        page.address = log_id;
        page.size = log_size;
        page.header.set_slot_id(log_item.slot_id());
        page.header.set_object_id(log_item.object_id());
        page.header.set_page_id(log_item.page_id());
        page.header.set_raw_data_size(log_item.page().size());
        page.header.set_data_size(log_item.page().size());  // we do not compress page data in oplog
        page.header.set_timestamp_ms(log_item.timestamp_ms());
        page.header.set_key(log_item.object_key());
        page.header.set_model_id(log_item.model_id());
        page.header.set_page_in_log(true);  // page_in_log is always true
        page.header.set_oplog_sequence(log_sequence);
        // we use log_sequence indicates version now,
        // so we do not support multi-page in same oplog
        page.header.set_version(log_sequence);
        page.data = log_item.page();
        return page;
    }

 private:
    struct ReadPageTask {
        uint64_t slot_id = 0;
        Controller* ctrl = nullptr;
        PageIndex page_index;
        PageInfo* page_info = nullptr;
        Closure<void>* callback = nullptr;
        std::string buffer;
        TimeCost timer;
    };
    struct StagedPage {
        PageInfo page_info;
        partition::LogItem* log = nullptr;  // point to log in SlotContext,
                                            // so BE CAREFUL WITH LOG LIFE CYCLE
        StagedPage(PageInfo _page_info, partition::LogItem* _log)
            : page_info(std::move(_page_info)), log(_log) {
            BYTE_ASSERT(log);
        }
    };

    void OnReadPageDone(ReadPageTask* task);

    Partition* partition_ = nullptr;
    Index* index_ = nullptr;

    std::unique_ptr<stream::Stream> stream_;

    storage::OpLog current_oplog_;
    uint64_t current_sequence_ = 0;
    std::vector<StagedPage> stage_pages_;
    OploggerMetrics metrics_;

    SlotContextManager* slot_context_manager_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(OpLogger);
};

class OpLogger::Iterator {
 public:
    explicit Iterator(stream::ScopedIterator iter) : iter_(std::move(iter)) {}
    Status Next();
    uint64_t GetLogId() const { return log_id_; }
    uint32_t GetSize() const { return size_; }
    const storage::OpLog& GetLog() const { return log_; }

 private:
    stream::ScopedIterator iter_;
    uint64_t log_id_ = 0;
    uint32_t size_ = 0;
    storage::OpLog log_;

    DISALLOW_COPY_AND_ASSIGN(Iterator);
};

}  // namespace partition
}  // namespace bcache2

// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <byte/container/intrusive_list.h>
#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <byte/thread/async_thread.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/controller.h"
#include "common/ring_array.h"
#include "protocol/storage.pb.h"
#include "stream/log_based_util.h"
#include "stream/store_layer.h"
#include "stream/stream.h"

namespace bcache2 {
namespace stream {

class StoreLayer;
class Blob;
class LogBasedEnv;

// A common base for both R/W and read-only log streams
class StreamBaseImpl {
 public:
    StreamBaseImpl(LogBasedEnv* env, const std::string& uri, MetricsManager* metrics_manager);
    virtual ~StreamBaseImpl();

    void Read(Controller* ctrl, uint64_t id, void* data, size_t size, Closure<void>* callback);
    ScopedIterator NewIterator(size_t start_id, size_t end_id);

    Status UpdateBlobHeader(const storage::BlobHeader& blob_header);
    const storage::BlobHeader& BlobHeader() const { return blob_header_; }
    void UpdateLength(uint64_t offset, uint64_t blob_length, uint64_t sequence,
                      uint64_t truncated_offset) {
        LOG_DEBUG("Update length")
            .put("Uri", uri_)
            .put("BlobLength", blob_length)
            .put("Sequence", sequence);
        auto& blob_info = readable_blobs_.back()->blob_info;
        BYTE_ASSERT(truncated_offset >= blob_info.truncated_offset());
        BYTE_ASSERT(offset >= blob_info.start_offset());
        blob_info.set_end_record_sequence(sequence);
        blob_info.set_blob_end_offset(blob_length);
        blob_info.set_truncated_offset(truncated_offset);
        blob_info.set_end_offset(offset);
    }
    const storage::BlobInfo& LastBlobInfo() const {
        static const storage::BlobInfo blob_info;
        if (readable_blobs_.empty()) {
            return blob_info;
        }
        return readable_blobs_.back()->blob_info;
    }

    StreamInfo GetInfo() const;
    Status RestoreInfo(const StreamInfo& stream_info);

    Status ListBlobs(std::vector<BlobInfo>* tmp_blobs, std::vector<BlobInfo>* data_blobs);
    Status ReadBlobHeader(Blob* blob, size_t blob_size, storage::BlobHeader* blob_header);

    const std::string& Uri() const { return uri_; }
    StoreLayer* GetStoreLayer() const { return store_layer_; }

 private:
    // iterator hierarchy: Block <- Record <- IteratorImpl
    class BlockIterator;
    class RecordIterator;
    class IteratorImpl;

    struct Task {
        Controller* ctrl = nullptr;
        uint64_t id = 0;
        void* data = nullptr;
        size_t size = 0;
        Closure<void>* callback = nullptr;

        bool inplace = false;                // should be renamed to something like `ongoing`
        std::vector<char> tmp_buffer;  // Record with header and block footer
        std::vector<std::pair<size_t, size_t>> tmp_iovec;  // For cross block object

        byte::intrusive_list_node link;

        TimeCost cost;
    };

    using TaskList = byte::intrusive_list<Task, &Task::link>;

    // In memory info of a Blob
    enum BlobOpenStatus {
        kNotOpen,
        kOpening,
        kOpened,
    };

    struct BlobOpenInfo {
        storage::BlobInfo blob_info;
        BlobOpenStatus state = kNotOpen;
        std::unique_ptr<Blob> blob;
        std::vector<Closure<void>*> wait_list;  // TryOpen only
        bool discard = false;

        BlobOpenInfo() {}
        DISALLOW_COPY_AND_ASSIGN(BlobOpenInfo);  // Can't copy and assign for wait_list
    };

    void ReadInternal(Task* task);
    void ReadBlob(Task* task, uint64_t blob_id, uint64_t blob_offset);
    void OnReadDone(Task* task);
    void TryOpenBlob(BlobOpenInfo* open_info, Closure<void>* callback);
    void OpenBlobForRead(BlobOpenInfo* open_info);
    Status GetBlobAndOffset(uint64_t offset, BlobOpenInfo** blob, uint64_t* blob_offset);
    BlobOpenInfo* GetBlob(uint64_t blob_id) {
        if (UNLIKELY(blob_id < readable_blobs_[0]->blob_info.blob_id())) return nullptr;
        return readable_blobs_[blob_id - readable_blobs_[0]->blob_info.blob_id()].get();
    }

    LogBasedEnv* env_ = nullptr;
    std::string uri_;
    StoreLayer* store_layer_ = nullptr;
    MetricsManager* metrics_manager_ = nullptr;

    storage::BlobHeader blob_header_;
    std::vector<std::unique_ptr<BlobOpenInfo>> readable_blobs_;
    std::vector<uint64_t> end_offset_list_;
    TaskList task_list_;

    DISALLOW_COPY_AND_ASSIGN(StreamBaseImpl);
};

class StreamBaseImpl::BlockIterator {
 public:
    explicit BlockIterator(StreamBaseImpl* stream) : stream_(stream) {}

    void Seek(uint64_t offset) { cur_start_offset_ = LowerAlign(offset, kBlockSize); }
    Status NextBlock(uint64_t* offset, uint64_t* last_record_sequence,
                     uint64_t* last_record_left_size, absl::string_view* data);

 private:
    Status NextBlockInternal(uint64_t* offset, uint64_t* last_record_sequence,
                             uint64_t* last_record_left_size, absl::string_view* data);
    Status Read(uint32_t blob_id, uint32_t offset, size_t size, char* buf);

    StreamBaseImpl* stream_ = nullptr;

    uint64_t cur_start_offset_ = 0;
    char block_buffer_[kBlockSize];

    DISALLOW_COPY_AND_ASSIGN(BlockIterator);
};

class StreamBaseImpl::RecordIterator {
 public:
    explicit RecordIterator(StreamBaseImpl* stream) : block_iter_(stream) {}
    Status Seek(uint64_t record_offset);
    Status NextRecord(uint64_t* record_offset, absl::string_view* record);
    uint64_t NextOffset() { return data_offset_; }

 private:
    BlockIterator block_iter_;

    uint64_t record_sequence_ = 0;
    uint64_t data_offset_ = 0;
    absl::string_view data_;
    uint64_t last_sequence_in_block_ = 0;

    std::unique_ptr<char[]> tmp_buffer_;

    DISALLOW_COPY_AND_ASSIGN(RecordIterator);
};

class StreamBaseImpl::IteratorImpl : public Iterator {
 public:
    Status Next() override;
    uint64_t Id() const override { return record_offset_; }
    absl::string_view Data() const override { return record_; }

 private:
    friend class StreamBaseImpl;

    IteratorImpl(StreamBaseImpl* stream, size_t start_offset, size_t end_offset)
        : start_offset_(start_offset), end_offset_(end_offset), record_iter_(stream) {}

    uint64_t start_offset_ = 0;
    uint64_t end_offset_ = 0;
    RecordIterator record_iter_;
    bool seeked_ = false;

    uint64_t record_offset_ = 0;
    absl::string_view record_;

    DISALLOW_COPY_AND_ASSIGN(IteratorImpl);
};

}  // namespace stream
}  // namespace bcache2

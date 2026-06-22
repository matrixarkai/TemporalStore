// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/log_based_stream_base.h"

#include <absl/strings/numbers.h>
#include <byte/algorithm/crc32.h>
#include <byte/include/assert.h>
#include <byte/string/format.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <algorithm>
#include <utility>

#include "common/coclosure.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/time.h"
#include "protocol/storage.pb.h"
#include "stream/log_based_env.h"
#include "stream/stream.h"

namespace bcache2 {
namespace stream {

StreamBaseImpl::StreamBaseImpl(LogBasedEnv* env, const std::string& uri,
                               MetricsManager* metrics_manager)
    : env_(env), uri_(uri), store_layer_(env->store_layer_), metrics_manager_(metrics_manager) {}

StreamBaseImpl::~StreamBaseImpl() {}

void StreamBaseImpl::Read(Controller* ctrl, uint64_t id, void* data, size_t size,
                          Closure<void>* callback) {
    // TODO(zhangyuan.42): Malloc from ctrl->pool()
    Task* task = new Task;
    task->ctrl = ctrl;
    task->id = id;
    task->data = data;
    task->size = size;
    task->callback = callback;

    ReadInternal(task);
}

ScopedIterator StreamBaseImpl::NewIterator(size_t start_id, size_t end_id) {
    return ScopedIterator(new IteratorImpl(this, start_id, end_id));
}

Status StreamBaseImpl::UpdateBlobHeader(const storage::BlobHeader& blob_header) {
    // TODO(zhangyuan.42): Check blob header
    if (blob_header_.magic() != 0 && !CheckBlobHeader(blob_header_, blob_header)) {
        return Status::Internal("BlobHeader data error");
    }
    size_t index = 0;
    // discard blobs whose IDs are smaller than the first index in the new blob_header
    while (index < readable_blobs_.size()) {
        std::unique_ptr<BlobOpenInfo>& open_info = readable_blobs_[index];  // reference here
        if (blob_header.data_blobs_size() > 0 &&
            open_info->blob_info.blob_id() >= blob_header.data_blobs(0).blob_id()) {
            break;
        }
        if (open_info->state == BlobOpenStatus::kOpening) {
            open_info->discard = true;
            static_cast<void>(open_info.release());  // OpenBlobForRead() will destroy it
        } else {
            open_info.reset();
        }
        index++;
    }

    blob_header_ = blob_header;
    std::vector<std::unique_ptr<BlobOpenInfo>> blobs(blob_header_.data_blobs_size() + 1);
    end_offset_list_.resize(blob_header_.data_blobs_size() + 1);  // for binary search later
    for (int i = 0; i < blob_header_.data_blobs_size(); ++i) {
        const storage::BlobInfo& blob_info = blob_header_.data_blobs(i);
        std::unique_ptr<BlobOpenInfo>& open_info = blobs[i];
        if (index < readable_blobs_.size()) {
            BYTE_ASSERT(readable_blobs_[index]->blob_info.blob_id() == blob_info.blob_id());
            open_info = std::move(readable_blobs_[index]);
            index++;
        } else {
            open_info.reset(new BlobOpenInfo);
        }
        open_info->blob_info = blob_info;
        end_offset_list_[i] =
            blob_info.start_offset() + blob_info.blob_end_offset() - blob_info.blob_start_offset();
    }
    BYTE_ASSERT(index == readable_blobs_.size());  // all previous readable blobs should be checked
    std::unique_ptr<BlobOpenInfo>& open_info = blobs.back();
    open_info.reset(new BlobOpenInfo);
    open_info->blob_info = FillBlobInfo(blob_header_, false);
    end_offset_list_.back() = UINT64_MAX;
    readable_blobs_ = std::move(blobs);  // update readable_blobs_
    return Status::OK();
}

Status StreamBaseImpl::ListBlobs(std::vector<BlobInfo>* tmp_blobs,
                                 std::vector<BlobInfo>* data_blobs) {
    // list
    std::vector<Store::BlobInfo> blobs;
    Controller ctrl;
    store_layer_->List(&ctrl, uri_, &blobs);
    if (!ctrl.status().ok()) {
        LOG_ERROR("List blobs failed").put("Uri", uri_).put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    tmp_blobs->clear();
    data_blobs->clear();
    // just check blob names
    for (size_t i = 0; i < blobs.size(); ++i) {
        BlobInfo blob;
        if (!BlobNameToInfo(blobs[i].name, &blob)) {
            LOG_ERROR("Blob name invalid").put("Uri", uri_).put("BlobName", blobs[i].name);
            return Status::Internal("Data blob error");
        }
        if (blob.type == BlobType::kTmpBlob) {
            tmp_blobs->push_back(blob);
        } else if (blob.type == BlobType::kDataBlob) {
            data_blobs->push_back(blob);
        }
        LOG_DEBUG("List blobs").put("Uri", uri_).put("BlobName", blobs[i].name);
    }
    sort(data_blobs->begin(), data_blobs->end());
    return Status::OK();
}

Status StreamBaseImpl::ReadBlobHeader(Blob* blob, size_t blob_size,
                                      storage::BlobHeader* blob_header) {
    if (blob_size <= sizeof(ProtoHeader)) {
        LOG_ERROR("Blob size error, may be bug")
            .put("Uri", uri_)
            .put("BlobSize", blob_size)
            .put("ProtoHeaderSize", sizeof(ProtoHeader));
        return Status::Internal("Blob size error");
    }
    size_t try_size = std::min(blob_size, kBlockSize);
    std::unique_ptr<char[]> buffer(new char[try_size]);
    Controller ctrl;
    SYNC_CALL(blob->Read, &ctrl, 0, buffer.get(), try_size);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Read blob failed")
            .put("Uri", uri_)
            .put("Blob", blob)
            .put("ExpectedSize", try_size)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    ProtoHeader* proto_header = reinterpret_cast<ProtoHeader*>(buffer.get());
    size_t need_size = proto_header->proto_size + sizeof(ProtoHeader);
    if (need_size > try_size) {
        char* new_buf = new char[need_size];
        memcpy(new_buf, buffer.get(), try_size);
        buffer.reset(new_buf);
    }
    size_t offset = try_size;
    // read from blob
    while (offset < need_size) {
        size_t io_size = std::min(need_size - offset, kBlockSize);
        ctrl.Reset();
        SYNC_CALL(blob->Read, &ctrl, offset, buffer.get() + offset, io_size);
        if (!ctrl.status().ok()) {
            LOG_ERROR("Read blob failed")
                .put("Uri", uri_)
                .put("Blob", blob)
                .put("Offset", offset)
                .put("ExpectedSize", io_size)
                .put("Error", ctrl.status().ToString());
            return ctrl.status();
        }
        offset += io_size;
    }

    proto_header = reinterpret_cast<ProtoHeader*>(buffer.get());
    if (!blob_header->ParseFromArray(buffer.get() + sizeof(ProtoHeader),
                                     proto_header->proto_size)) {
        LOG_ERROR("Parse blob header failed")
            .put("Uri", uri_)
            .put("Blob", blob)
            .put("Size", proto_header->proto_size);
        return Status::Unknown("Parse failed");
    }
    uint32_t crc32c = byte::CRCUtil::ComputeCRC32(0, buffer.get() + sizeof(ProtoHeader),
                                                  proto_header->proto_size);
    if (crc32c != proto_header->proto_crc) {
        LOG_ERROR("Blob header crc mismatch")
            .put("Uri", uri_)
            .put("Blob", blob)
            .put("ExpectedCrc", proto_header->proto_crc)
            .put("RealCrc", crc32c);
        return Status::DataLoss("Crc mismatch");
    }

    BYTE_ASSERT_DEBUG(blob_header->magic() == kMagic);
    if (blob_header->magic() != kMagic) {
        LOG_ERROR("Blob header magic number mismatch")
            .put("Uri", uri_)
            .put("Blob", blob)
            .put("ExpectedMagic", kMagic)
            .put("BlobHeader", blob_header->ShortDebugString());
        return Status::DataLoss("Magic mismatch");
    }
    BYTE_ASSERT_DEBUG(blob_header->header_size() == proto_header->proto_size);
    if (blob_header->header_size() != proto_header->proto_size) {
        LOG_ERROR("Blob header size mismatch")
            .put("Uri", uri_)
            .put("Blob", blob)
            .put("ExpectedSize", proto_header->proto_size)
            .put("BlobHeader", blob_header->ShortDebugString());
        return Status::DataLoss("Blob header size mismatch");
    }

    return Status::OK();
}

void StreamBaseImpl::ReadInternal(Task* task) {
    LOG_CALL_DEBUG().put("Uri", uri_).put("Offset", task->id).put("Size", task->size);
    BlobOpenInfo* blob = nullptr;
    uint64_t blob_offset = 0;
    Status status = GetBlobAndOffset(task->id, &blob, &blob_offset);
    if (UNLIKELY(!status.ok())) {
        LOG_ERROR("Read offset out of range")
            .put("Uri", uri_)
            .put("Offset", task->id)
            .put("Size", task->size);
        task->ctrl->set_status(Status::OutOfRange("Offset invalid"));
        byte::InvokeInCurrentThread(NewClosure(this, &StreamBaseImpl::OnReadDone, task));
        return;
    }

    if (UNLIKELY(blob->state != kOpened)) {
        LOG_DEBUG("Wait blob for read")
            .put("Uri", uri_)
            .put("Blob", blob->blob_info.ShortDebugString());
        TryOpenBlob(blob, NewClosure(this, &StreamBaseImpl::ReadBlob, task,
                                     blob->blob_info.blob_id(), blob_offset));
        return;
    }

    ReadBlob(task, blob->blob_info.blob_id(), blob_offset);
}

void StreamBaseImpl::ReadBlob(Task* task, uint64_t blob_id, uint64_t blob_offset) {
    LOG_CALL_DEBUG().put("Uri", uri_).put("BlobId", blob_id).put("BlobOffset", blob_offset);
    BlobOpenInfo* blob = GetBlob(blob_id);
    if (UNLIKELY(blob == nullptr)) {
        LOG_ERROR("Data has been truncate")
            .put("BlobId", blob_id)
            .put("BlobHeader", blob_header_.ShortDebugString());
        task->ctrl->set_status(Status::OutOfRange("Data has been truncated"));
        OnReadDone(task);
        return;
    }
    if (UNLIKELY(blob->state != kOpened)) {
        task->ctrl->set_status(Status::Internal("Open failed"));
        OnReadDone(task);
        return;
    }

    size_t header_size = RecordHeaderLength(task->size);
    size_t real_size = task->size + header_size;

    size_t block_offset = blob_offset % kBlockSize;
    size_t end_offset = block_offset + header_size + task->size;
    if (end_offset + kBlockFooterSize > kBlockSize) {  // Cross block
        size_t left_size = task->size;
        real_size = 0;
        while (left_size > 0) {
            size_t frag_size =
                std::min(left_size, kBlockSize - block_offset - header_size - kBlockFooterSize);
            task->tmp_iovec.emplace_back(std::make_pair(real_size + header_size, frag_size));
            real_size += header_size + frag_size;
            left_size -= frag_size;
            block_offset = 0;
            header_size = 0;
            if (left_size > 0) {
                real_size += kBlockFooterSize;
            }
        }
    }

    task->tmp_buffer.resize(real_size);
    char *buf = task->tmp_buffer.data();

    if (UNLIKELY(blob_offset + real_size > blob->blob_info.blob_end_offset())) {
        task->ctrl->set_status(Status::Internal("Out of range"));
        OnReadDone(task);
        return;
    }

    task->inplace = true;
    blob->blob->Read(task->ctrl, blob_offset, buf, real_size,
                     NewClosure(this, &StreamBaseImpl::OnReadDone, task));
    task->inplace = false;
}

void StreamBaseImpl::OnReadDone(Task* task) {
    LOG_CALL_DEBUG().put("Uri", uri_).put("Offset", task->id).put("Size", task->size);
    BYTE_ASSERT_DEBUG(!task->inplace);
    std::unique_ptr<Task> task_guard(task);
    ScopedCallback done(task->callback);
    if (!task->ctrl->status().ok()) {
        LOG_ERROR("Read blob failed")
            .put("Uri", uri_)
            .put("Id", task->id)
            .put("Size", task->size)
            .put("Error", task->ctrl->status().ToString());
        return;
    }

    uint32_t record_length = 0;
    uint32_t record_crc32c = 0;
    uint32_t consumed_size = 0;
    if (!ReadRecordHeader(task->tmp_buffer.data(), task->tmp_buffer.size(),
        &record_length, &record_crc32c, &consumed_size) || record_length != task->size) {
        LOG_ERROR("Read record header failed")
            .put("tatol length", task->tmp_buffer.size())
            .put("record length", record_length)
            .put("read size", task->size);
        task->ctrl->set_status(Status::DataLoss("Record header length not enought"));
        return;
    }

    if (UNLIKELY(task->tmp_iovec.size())) {
        // Copy fragments from task->tmp_buffer to task->data
        size_t offset = 0;
        char* data = static_cast<char*>(task->data);
        for (auto& pair : task->tmp_iovec) {
            memcpy(&data[offset], &task->tmp_buffer[pair.first], pair.second);
            offset += pair.second;
        }
    } else {
        // trim record header
        memcpy(task->data, task->tmp_buffer.data() + consumed_size, record_length);
    }

    uint32_t real_crc32c =
        byte::CRCUtil::ComputeCRC32(0, static_cast<char*>(task->data), record_length);
    if (record_crc32c != 0 && real_crc32c != record_crc32c) {
        LOG_ERROR("Record crc mismatch")
            .put("RecordOffset", task->id)
            .put("Length", record_length)
            .put("ExpectedCrc32c", record_crc32c)
            .put("RealCrc32c", real_crc32c);
        task->ctrl->set_status(Status::DataLoss("Record crc mismatch"));
        return;
    }
    LOG_DEBUG("Read blob done").put("Uri", uri_).put("Id", task->id).put("Size", task->size);
}

void StreamBaseImpl::TryOpenBlob(BlobOpenInfo* open_info, Closure<void>* callback) {
    open_info->wait_list.push_back(callback);

    if (open_info->state == kOpening) {
        return;
    }
    BYTE_ASSERT(open_info->state == kNotOpen);
    open_info->state = kOpening;

    byte::InvokeInCurrentThread(NewCoClosure(this, &StreamBaseImpl::OpenBlobForRead, open_info));
}

void StreamBaseImpl::OpenBlobForRead(BlobOpenInfo* open_info) {
    LOG_CALL_DEBUG().put("Uri", uri_);
    BYTE_ASSERT(open_info->state == kOpening);

    BlobInfo blob_info(BlobType::kDataBlob, open_info->blob_info.blob_id());
    blob_info.name = BlobInfoToName(blob_info);  // TMP/DAT-xxxx
    std::string blob_uri = uri_ + blob_info.name;
    Controller ctrl;
    Store::OpenOptions options;
    options.mode = Store::OpenMode::kRead;
    options.metrics_manager = metrics_manager_;
    Blob* blob = nullptr;
    store_layer_->Open(&ctrl, blob_uri, options, &blob);  // sync
    LOG_INFO("Open blob for read")
        .put("Uri", uri_)
        .put("BlobInfo", open_info->blob_info.ShortDebugString())
        .put("BlobUri", blob_uri)
        .put("Blob", blob)
        .put("Status", ctrl.status().ToString());

    BYTE_ASSERT(open_info->state == kOpening);
    if (ctrl.status().ok()) {
        open_info->state = kOpened;
        open_info->blob.reset(blob);
    } else {
        open_info->state = kNotOpen;
    }

    std::vector<Closure<void>*> wait_list = std::move(open_info->wait_list);
    for (auto& callback : wait_list) {
        callback->Run();
    }

    if (open_info->discard) {
        LOG_WARNING("Blob has been truncate")
            .put("Uri", uri_)
            .put("BlobInfo", open_info->blob_info.ShortDebugString())
            .put("BlobHeader", blob_header_.ShortDebugString());
        delete open_info;
    }
}

Status StreamBaseImpl::GetBlobAndOffset(uint64_t offset, BlobOpenInfo** blob,
                                        uint64_t* blob_offset) {
    if (UNLIKELY(end_offset_list_.empty() || readable_blobs_.empty())) {
        return Status::OutOfRange("Stream has no readable blobs");
    }
    size_t low = 0;
    // TODO(zkwu): just use std
    size_t high = end_offset_list_.size();
    while (low < high) {
        size_t mid = (low + high) / 2;
        if (offset < end_offset_list_[mid]) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    BYTE_ASSERT(low < end_offset_list_.size());
    *blob = readable_blobs_[low].get();
    if (UNLIKELY(offset < (*blob)->blob_info.start_offset())) {
        return Status::OutOfRange("Offset has been truncated");
    }
    if (UNLIKELY(offset >= (*blob)->blob_info.start_offset() +
                               (*blob)->blob_info.blob_end_offset() -
                               (*blob)->blob_info.blob_start_offset())) {
        return Status::OutOfRange("Out of range");
    }
    *blob_offset =
        offset - (*blob)->blob_info.start_offset() + (*blob)->blob_info.blob_start_offset();
    return Status::OK();
}

Status StreamBaseImpl::BlockIterator::NextBlock(uint64_t* offset, uint64_t* last_record_sequence,
                                                uint64_t* last_record_left_size,
                                                absl::string_view* data) {
    Status status;
    do {
        status = NextBlockInternal(offset, last_record_sequence, last_record_left_size, data);
    } while (status.ok() && data->empty());
    return status;
}

Status StreamBaseImpl::BlockIterator::NextBlockInternal(uint64_t* offset,
                                                        uint64_t* last_record_sequence,
                                                        uint64_t* last_record_left_size,
                                                        absl::string_view* data) {
    BlobOpenInfo* blob = nullptr;
    uint64_t blob_offset = 0;
    Status status = stream_->GetBlobAndOffset(cur_start_offset_, &blob, &blob_offset);
    if (!status.ok()) {
        if (!status.IsOutOfRange()) {
            LOG_ERROR("Find blob failed")
                .put("Uri", stream_->uri_)
                .put("Offset", cur_start_offset_)
                .put("Error", status.ToString());
        }
        return status;
    }
    if (blob_offset % kBlockSize + kBlockFooterSize > kBlockSize) {
        LOG_ERROR("Data error")
            .put("Uri", stream_->uri_)
            .put("CurStartOffset", cur_start_offset_)
            .put("BlobId", blob->blob_info.blob_id())
            .put("BlobOffset", blob_offset);
        return Status::DataLoss("Data error");
    }

    storage::BlobInfo blob_info = blob->blob_info;
    size_t size =
        std::min(blob_info.blob_end_offset() - blob_offset, kBlockSize - blob_offset % kBlockSize);
    status = Read(blob_info.blob_id(), blob_offset, size, block_buffer_ + blob_offset % kBlockSize);
    if (!status.ok()) {
        LOG_ERROR("Read data failed")
            .put("BlobId", blob_info.blob_id())
            .put("BlobOffset", blob_offset)
            .put("Size", size);
        return status;
    }

    if (blob_offset % kBlockSize + size == kBlockSize) {
        storage::BlockFooter footer;
        if (!GetBlockFooter(block_buffer_ + kBlockSize - kBlockFooterSize, &footer)) {
            LOG_ERROR("Get block footer failed")
                .put("BlobOffset", blob_offset)
                .put("Size", size)
                .put("BlobId", blob_info.blob_id())
                .put("Offset", cur_start_offset_);
            return Status::DataLoss("");
        }
        if (footer.block_end() < blob_offset % kBlockSize) {
            LOG_ERROR("Data error")
                .put("Uri", stream_->uri_)
                .put("CurStartOffset", cur_start_offset_)
                .put("BlobId", blob->blob_info.blob_id())
                .put("BlobOffset", blob_offset)
                .put("BlobFooter", footer.ShortDebugString());
            return Status::DataLoss("Data error");
        }
        *last_record_sequence = footer.last_record_sequence();
        *last_record_left_size = footer.last_record_left_size();
        *data = absl::string_view(block_buffer_ + blob_offset % kBlockSize,
                                  footer.block_end() - blob_offset % kBlockSize);
        uint32_t block_crc32c = byte::CRCUtil::ComputeCRC32(0, block_buffer_, footer.block_end());
        if (block_crc32c != footer.block_crc()) {
            LOG_ERROR("Block crc32c mismatch")
                .put("BlobOffset", blob_offset)
                .put("Size", size)
                .put("BlobId", blob_info.blob_id())
                .put("Offset", cur_start_offset_)
                .put("BlockFooter", footer.ShortDebugString())
                .put("RealCrc", block_crc32c);
            BYTE_ASSERT_DEBUG(false);
            return Status::DataLoss("");
        }
    } else {
        *last_record_sequence = blob_info.end_record_sequence();
        *last_record_left_size = 0;
        *data = absl::string_view(block_buffer_ + blob_offset % kBlockSize, size);
    }
    *offset = blob_info.start_offset() + blob_offset - blob_info.blob_start_offset();
    cur_start_offset_ =
        blob_info.start_offset() + blob_offset + size - blob_info.blob_start_offset();
    LOG_DEBUG("Get block data")
        .put("BlockIter", this)
        .put("CurStartOffset", cur_start_offset_)
        .put("BlobOffset", blob_offset)
        .put("Offset", *offset)
        .put("BlobId", blob_info.blob_id())
        .put("LastRecordSequence", *last_record_sequence)
        .put("LastRecordLeftSize", *last_record_left_size)
        .put("Size", data->size());
    return Status::OK();
}

Status StreamBaseImpl::BlockIterator::Read(uint32_t blob_id, uint32_t offset, size_t size,
                                           char* buf) {
    BlobOpenInfo* open_info = stream_->GetBlob(blob_id);
    if (open_info == nullptr) {
        LOG_ERROR("Data has been truncated").put("BlobId", blob_id);
        return Status::OutOfRange("Data has been truncated");
    }
    if (open_info->state != kOpened) {
        CoSyncClosure sync;
        stream_->TryOpenBlob(open_info, &sync);
        sync.Wait();
    }
    if (open_info->state != kOpened) {
        LOG_ERROR("Blob null").put("BlobId", blob_id);
        return Status::InvalidArgument("");
    }

    Controller ctrl;
    SYNC_CALL(open_info->blob->Read, &ctrl, offset, buf, size);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Read blob failed")
            .put("Blob", blob_id)
            .put("Size", size)
            .put("Offset", offset)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }
    LOG_DEBUG("Read data").put("BlobId", blob_id).put("Offset", offset).put("Size", size);
    return Status::OK();
}

Status StreamBaseImpl::RecordIterator::Seek(uint64_t record_offset) {
    size_t seek_offset = record_offset > kBlockSize ? record_offset - kBlockSize : 0UL;
    block_iter_.Seek(seek_offset);
    if (seek_offset == 0) {
        record_sequence_ = 0;
        data_offset_ = 0;
        data_ = absl::string_view();
        last_sequence_in_block_ = 0;
        return Status::OK();
    }

    uint64_t offset = 0;
    uint64_t last_record_sequence = 0;
    uint64_t last_record_left_size = 0;
    absl::string_view data;
    Status status =
        block_iter_.NextBlock(&offset, &last_record_sequence, &last_record_left_size, &data);
    if (!status.ok()) {
        if (!status.IsOutOfRange()) {
            LOG_ERROR("Next block failed").put("Error", status.ToString());
        }
        return status;
    }
    last_sequence_in_block_ = last_record_sequence;
    record_sequence_ = last_record_sequence;
    data_offset_ = 0;
    if (last_record_left_size == 0) {
        return Status::OK();
    }

    char buf[16];
    uint32_t consumed_size = 0;
    BYTE_ASSERT(WriteRecordHeader(last_record_left_size, 0, buf, sizeof(buf), &consumed_size));
    data_ = absl::string_view(buf, consumed_size);

    return NextRecord(&offset, &data);
}

Status StreamBaseImpl::RecordIterator::NextRecord(uint64_t* record_offset,
                                                  absl::string_view* record) {
    if (data_.size() == 0) {
        BYTE_ASSERT_DEBUG(record_sequence_ == last_sequence_in_block_);
        uint64_t offset = 0;
        uint64_t last_record_sequence = 0;
        uint64_t last_record_left_size = 0;
        absl::string_view data;
        Status status =
            block_iter_.NextBlock(&offset, &last_record_sequence, &last_record_left_size, &data);
        if (!status.ok()) {
            if (!status.IsOutOfRange()) {
                LOG_ERROR("Next block failed").put("Error", status.ToString());
            }
            return status;
        }
        data_ = data;
        data_offset_ = offset;
        last_sequence_in_block_ = last_record_sequence;
    }

    uint32_t record_length = 0;
    uint32_t record_crc32c = 0;
    uint32_t consumed_size = 0;
    if (!ReadRecordHeader(data_.data(), data_.size(), &record_length, &record_crc32c,
                          &consumed_size)) {
        LOG_ERROR("Read record header failed").put("Size", data_.size());
        return Status::DataLoss("Record header length not enought");
    }
    uint64_t total_length = consumed_size + record_length;

    if (data_.size() >= total_length) {
        uint32_t real_crc32c =
            byte::CRCUtil::ComputeCRC32(0, data_.data() + consumed_size, record_length);
        if (record_crc32c != 0 && real_crc32c != record_crc32c) {
            BYTE_ASSERT_DEBUG(false);
            LOG_ERROR("Record crc mismatch")
                .put("RecordOffset", data_offset_)
                .put("RecordSequence", record_sequence_)
                .put("Length", record_length)
                .put("ExpectedCrc32c", record_crc32c)
                .put("RealCrc32c", real_crc32c);
            return Status::DataLoss("Record crc mismatch");
        }
        *record_offset = data_offset_;
        *record = data_.substr(consumed_size, record_length);
        data_offset_ += total_length;
        data_ = data_.substr(total_length);
        record_sequence_++;
        return Status::OK();
    }

    char* new_buffer = new char[total_length];
    memcpy(new_buffer, data_.data(), data_.size());
    tmp_buffer_.reset(new_buffer);
    data_ = absl::string_view(&tmp_buffer_[0], data_.size());

    while (data_.size() < total_length) {
        BYTE_ASSERT_DEBUG(record_sequence_ == last_sequence_in_block_);
        uint64_t offset = 0;
        uint64_t last_record_sequence = 0;
        uint64_t last_record_left_size = 0;
        absl::string_view data;
        Status status =
            block_iter_.NextBlock(&offset, &last_record_sequence, &last_record_left_size, &data);
        if (!status.ok()) {
            if (!status.IsOutOfRange()) {
                LOG_ERROR("Next block failed").put("Error", status.ToString());
            }
            return status;
        }
        last_sequence_in_block_ = last_record_sequence;
        if (data_.size() + data.size() < total_length) {
            memcpy(&tmp_buffer_[data_.size()], data.data(), data.size());
            data_ = absl::string_view(&tmp_buffer_[0], data_.size() + data.size());
        } else {
            size_t moved_data_size = total_length - data_.size();
            memcpy(&tmp_buffer_[data_.size()], data.data(), moved_data_size);
            data_ = absl::string_view(&tmp_buffer_[0], total_length);

            *record_offset = data_offset_;
            *record = data_.substr(consumed_size, record_length);
            data_offset_ = offset + moved_data_size;
            data_ = data.substr(moved_data_size);
            record_sequence_++;
            break;
        }
    }

    return Status::OK();
}

Status StreamBaseImpl::IteratorImpl::Next() {
    // first iterate
    if (UNLIKELY(!seeked_)) {
        Status status = record_iter_.Seek(start_offset_);
        if (!status.ok()) {
            LOG_ERROR("Seek failed").put("Error", status.ToString());
            return status;
        }
        while (record_iter_.NextOffset() < start_offset_) {
            uint64_t record_offset = 0;
            absl::string_view record;
            Status status = record_iter_.NextRecord(&record_offset, &record);
            if (!status.ok()) {
                if (!status.IsOutOfRange()) {
                    LOG_ERROR("Next record failed").put("Error", status.ToString());
                }
                return status;
            }
            record_offset_ = record_offset;
            record_ = record;

            LOG_DEBUG("Current record")
                .put("Iter", this)
                .put("Id", record_offset_)
                .put("Size", record_.size())
                .put("Next Offset", record_iter_.NextOffset());
        }
        seeked_ = true;
    }

    uint64_t record_offset = 0;
    absl::string_view record;
    Status status = record_iter_.NextRecord(&record_offset, &record);
    if (!status.ok()) {
        if (!status.IsOutOfRange()) {
            LOG_ERROR("Next record failed").put("Error", status.ToString());
        }
        return status;
    }
    if (record_offset > end_offset_) {
        return Status::OutOfRange("");
    }

    record_offset_ = record_offset;
    record_ = record;
    LOG_DEBUG("Current record")
        .put("Iter", this)
        .put("Id", record_offset_)
        .put("Size", record_.size());
    return Status::OK();
}

StreamInfo StreamBaseImpl::GetInfo() const {
    StreamInfo info;
    info.set_uri(uri_);
    info.mutable_blob_header()->CopyFrom(blob_header_);
    for (const auto& blob_open_info : readable_blobs_) {
        info.add_blob_infos()->CopyFrom(blob_open_info->blob_info);
    }
    return info;
}

Status StreamBaseImpl::RestoreInfo(const StreamInfo& stream_info) {
    if (blob_header_.blob_id() != stream_info.blob_header().blob_id()) {
        Status status = UpdateBlobHeader(stream_info.blob_header());
        if (!status.ok()) {
            LOG_ERROR("Failed to restore info")
                .put("Uri", uri_)
                .put("CurrInfo", blob_header_.ShortDebugString())
                .put("NewInfo", stream_info.blob_header().ShortDebugString())
                .put("Status", status);
            return status;
        }
    }

    if (readable_blobs_.size() !=
        static_cast<size_t>(stream_info.blob_header().data_blobs_size()) + 1) {
        LOG_ERROR("Invalid blob header")
            .put("CurrBlobSize", readable_blobs_.size())
            .put("NewBlobSize", stream_info.blob_header().data_blobs_size());
        return Status::InvalidArgument("Invalid blob header");
    }

    auto& curr_last_blob_info = readable_blobs_.back()->blob_info;
    auto& new_last_blob_info = *stream_info.blob_infos().rbegin();
    if (!CheckBlobInfo(curr_last_blob_info, new_last_blob_info)) {
        LOG_ERROR("Invalid blob info")
            .put("Uri", uri_)
            .put("CurrLastBlobInfo", curr_last_blob_info.ShortDebugString())
            .put("NewLastBlobInfo", new_last_blob_info.ShortDebugString());
        return Status::InvalidArgument("Invalid blob info");
    }

    // update the last blob info
    curr_last_blob_info.set_end_record_sequence(new_last_blob_info.end_record_sequence());
    curr_last_blob_info.set_end_offset(new_last_blob_info.end_offset());
    curr_last_blob_info.set_truncated_offset(new_last_blob_info.truncated_offset());
    curr_last_blob_info.set_blob_end_offset(new_last_blob_info.blob_end_offset());

    LOG_DEBUG("Restore info success").put("Uri", uri_).put("Info", stream_info.ShortDebugString());
    return Status::OK();
}

}  // namespace stream
}  // namespace bcache2

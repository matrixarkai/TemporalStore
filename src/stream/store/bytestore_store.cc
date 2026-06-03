// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/store/bytestore_store.h"

#include <absl/strings/str_format.h>
#include <byte/string/format.h>

#include <algorithm>

#include "common/string_utils.h"
#include "stream/store/bytestore_wrapper.h"

namespace bcache2 {
namespace stream {

namespace {

inline std::string BytestoreStatusString(bytestore_status status) {
    switch (status) {
    case STATUS_OK:
        return "BYTESTORE_OK";
    case STATUS_NOT_FOUND:
        return "BYTESTORE_NOT_FOUND";
    case STATUS_ALREADY_EXIST:
        return "BYTESTORE_ALREADY_EXIST";
    case STATUS_CORRUPTION:
        return "BYTESTORE_CORRUPTION";
    case STATUS_NOT_SUPPORTED:
        return "BYTESTORE_NOT_SUPPORTED";
    case STATUS_INVALID_ARGUMENT:
        return "BYTESTORE_INVALID_ARGUMENT";
    case STATUS_IO_ERROR:
        return "BYTESTORE_IO_ERROR";
    case STATUS_TIMEOUT:
        return "BYTESTORE_TIMEOUT";
    case STATUS_PERMISSION_DENIED:
        return "BYTESTORE_PERMISSION_DENIED";
    case STATUS_IO_BUSY:
        return "BYTESTORE_IO_BUSY";
    case STATUS_END_FILE:
        return "BYTESTORE_END_FILE";
    case STATUS_INTERNAL_ERR:
        return "BYTESTORE_INTERNAL_ERR";
    case STATUS_HANDLE_CLOSED:
        return "BYTESTORE_HANDLE_CLOSED";
    case STATUS_NEED_REOPEN:
        return "BYTESTORE_NEED_REOPEN";
    case STATUS_NOT_READY:
        return "BYTESTORE_NOT_READY";
    case STATUS_NO_SPACE:
        return "BYTESTORE_NO_SPACE";
    case STATUS_POWER_OFF:
        return "BYTESTORE_POWER_OFF";
    case STATUS_OVERFLOW:
        return "BYTESTORE_OVERFLOW";
    case STATUS_CONDITION_VERIFY_FAILED:
        return "BYTESTORE_CONDITION_VERIFY_FAILED";
    case STATUS_NEED_REWRITE:
        return "BYTESTORE_NEED_REWRITE";
    case STATUS_DATA_LOST:
        return "BYTESTORE_DATA_LOST";
    case STATUS_CHECKSUM_FAIL:
        return "BYTESTORE_CHECKSUM_FAIL";
    case STATUS_HARDLINK_OUT_LIMIT:
        return "BYTESTORE_HARDLINK_OUT_LIMIT";
    case STATUS_UNKNOWN_ERR:
        return "BYTESTORE_UNKNOWN_ERR";
    default:
        return "BYTESTORE_" + std::to_string(static_cast<int>(status));
    }
}

inline Status ByteStoreMessageToStatus(const bytestore_message& message) {
    switch (message.status_) {
    case STATUS_CONDITION_VERIFY_FAILED:
        return Status::StoreConditionFailed("Condition changed");
    case STATUS_NOT_FOUND:
        return Status::StoreNotFound("Not found");
    default:
        return Status::StoreInternal(BytestoreStatusString(message.status_));
    }
}

}  // namespace

BlobImpl::BlobImpl(const std::string& uri, MetricsManager* metrics_manager, bytestore_blob* blob)
    : uri_(uri), blob_(blob) {
    metrics_.Init(metrics_manager, uri);
}

BlobImpl::~BlobImpl() {}

void BlobImpl::Close() {
    LOG_CALL_DEBUG().put("Uri", uri_);
    bytestore_message message;
    BYTESTORE_CLOSE(blob_, &message);
    if (message.status_ != STATUS_OK) {
        LOG_WARNING("Close blob failed")
            .put("Uri", uri_)
            .put("Status", ByteStoreMessageToStatus(message));
    }
    LOG_INFO("Close blob").put("Uri", uri_);
}

void BlobImpl::Append(Controller* ctrl, const void* data, size_t size, Closure<void>* callback) {
    LOG_CALL_DEBUG().put("Uri", uri_).put("Size", size);
    bytestore_io_options options = bytestore_default_write_options();
    options.timeout_in_ms_ = ctrl->timeout_ms();
    Task* task = new Task;
    task->ctrl = ctrl;
    task->callback = callback;
    task->thread = byte::GetCurrentThread();
    task->read = false;
    task->metrics = &metrics_;
    bytestore_message message;
    metrics_.append_qps->get()->Increment();
    metrics_.append_throughput->get()->Add(size);
    BYTESTORE_ASYNC_WRITE(blob_, data, size, &options, &message, &BlobImpl::IoCallback, task);
    if (message.status_ != STATUS_OK) {
        std::unique_ptr<Task> scoped_ptr(task);
        LOG_ERROR("Append to bytestore: async write blob failed")
            .put("Uri", uri_)
            .put("Size", size)
            .put("Status", ByteStoreMessageToStatus(message));
        task->ctrl->set_status(ByteStoreMessageToStatus(message));
        task->thread->Invoke(callback);
        return;
    }
}

void BlobImpl::Read(Controller* ctrl, size_t offset, void* data, size_t size,
                    Closure<void>* callback) {
    LOG_CALL_DEBUG().put("Uri", uri_).put("Size", size).put("Offset", offset);
    bytestore_io_options read_options = bytestore_default_read_options();
    read_options.timeout_in_ms_ = ctrl->timeout_ms();
    Task* task = new Task;
    task->ctrl = ctrl;
    task->callback = callback;
    task->thread = byte::GetCurrentThread();
    task->read = true;
    task->metrics = &metrics_;
    metrics_.read_qps->get()->Increment();
    metrics_.read_throughput->get()->Add(size);
    bytestore_message message;
    BYTESTORE_ASYNC_PREAD(blob_, data, size, offset, &read_options, &message, &BlobImpl::IoCallback,
                          task);
    if (message.status_ != STATUS_OK) {
        std::unique_ptr<Task> scoped_ptr(task);
        LOG_ERROR("Async pread blob failed")
            .put("Uri", uri_)
            .put("Offset", offset)
            .put("Size", size)
            .put("Status", ByteStoreMessageToStatus(message));
        task->ctrl->set_status(ByteStoreMessageToStatus(message));
        task->thread->Invoke(callback);
        return;
    }
}

void BlobImpl::IoCallback(ssize_t size, struct bytestore_message* message, void* args) {
    Task* task = static_cast<Task*>(args);
    // BYTE_ASSERT(!task->inplace);
    std::unique_ptr<Task> scoped_ptr(task);
    LOG_DEBUG("Callback").put("Size", size).put("Code", message->status_);
    if (task->read) {
        task->metrics->read_latency->get()->Set(task->cost.GetElapsedInUs());
    } else {
        task->metrics->append_latency->get()->Set(task->cost.GetElapsedInUs());
    }
    task->ctrl->set_status(message->status_ == STATUS_OK ? Status::OK()
                                                         : ByteStoreMessageToStatus(*message));
    task->thread->Invoke(task->callback);
}

ByteStoreImpl::ByteStoreImpl() {}

ByteStoreImpl::~ByteStoreImpl() {}

void ByteStoreImpl::SetCondition(Controller* ctrl, const std::string& uri,
                                 const ConditionData& data, const SetConditionOptions& options) {
    bytestore_update_inline_blob_options update_inline_blob_options =
        bytestore_default_update_inline_blob_options();
    update_inline_blob_options.timeout_in_ms_ = ctrl->timeout_ms();
    bytestore_inline_blob_stat stat;
    BYTE_ASSERT(data.size() == k_inline_blob_content_size);
    memcpy(stat.content_, data.data(), data.size());
    bytestore_message message;
    bool res =
        BYTESTORE_UPDATE_INLINE_BLOB(uri.c_str(), &stat, &update_inline_blob_options, &message);
    // TODO(guogaofeng): bytestore_update_inline_blob should return false when status is not_found
    if (message.status_ == STATUS_NOT_FOUND) {
        bytestore_create_inline_blob_options create_options =
            bytestore_default_create_inline_blob_options();
        create_options.timeout_in_ms_ = ctrl->timeout_ms();
        res = BYTESTORE_CREATE_INLINE_BLOB(uri.c_str(), &create_options, &message);
        if (!res) {
            LOG_ERROR("Create inline blob failed")
                .put("Uri", uri)
                .put("Status", ByteStoreMessageToStatus(message));
            ctrl->set_status(ByteStoreMessageToStatus(message));
            return;
        }
        res =
            BYTESTORE_UPDATE_INLINE_BLOB(uri.c_str(), &stat, &update_inline_blob_options, &message);
    }

    // TODO(guogaofeng): bytestore_update_inline_blob should return false when status is not_found
    if (!res || message.status_ == STATUS_NOT_FOUND) {
        LOG_ERROR("Update inline blob failed")
            .put("Uri", uri)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }
    LOG_INFO("Update inline blob success")
        .put("Uri", uri)
        .put("Data", DebugRawString(data.data(), data.size()));
    ctrl->set_status(Status::OK());
}

void ByteStoreImpl::StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) {
    bytestore_stat_inline_blob_options options = bytestore_default_stat_inline_blob_options();
    options.timeout_in_ms_ = ctrl->timeout_ms();
    bytestore_inline_blob_stat stat;
    bytestore_message message;
    bool res = BYTESTORE_STAT_INLINE_BLOB(uri.c_str(), &stat, &options, &message);
    if (!res) {
        LOG_ERROR("Stat inline blob failed")
            .put("Uri", uri)
            .put("Status", ByteStoreMessageToStatus(message));
        if (message.status_ == STATUS_NOT_FOUND) {
            ctrl->set_status(ByteStoreMessageToStatus(message));
        } else {
            ctrl->set_status(ByteStoreMessageToStatus(message));
        }
        return;
    }
    BYTE_ASSERT(data->size() == k_inline_blob_content_size);
    std::copy(std::begin(stat.content_), std::end(stat.content_), data->begin());
    ctrl->set_status(Status::OK());
    LOG_INFO("Stat inline blob success")
        .put("Uri", uri)
        .put("Data", DebugRawString(data->data(), data->size()));
}

void ByteStoreImpl::List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) {
    if (path.size() <= 2 || path.back() != '/') {
        LOG_ERROR("Invalid blob path").put("Uri", path);
        ctrl->set_status(Status::InvalidArgument("Path must end with '/'"));
        return;
    }

    std::string::size_type pos = path.find_last_of('/', path.size() - 2);
    if (pos == std::string::npos) {
        LOG_ERROR("Invalid blob path").put("Uri", path);
        ctrl->set_status(Status::InvalidArgument("Dir not found"));
        return;
    }
    std::string pool_name = path.substr(0, pos + 1);
    std::string dir_name = path.substr(pos + 1);

    bytestore_open_pool_options options = bytestore_default_open_pool_options();
    // Truncate data that exceeds prefix length
    snprintf(options.prefix_blob_name_,
             sizeof(options.prefix_blob_name_) / sizeof(options.prefix_blob_name_[0]), "%s",
             dir_name.c_str());
    snprintf(options.start_blob_name_,
             sizeof(options.start_blob_name_) / sizeof(options.start_blob_name_[0]), "%s",
             dir_name.c_str());
    options.timeout_in_ms_ = ctrl->timeout_ms();

    bytestore_message message;
    bytestore_pool* pool = BYTESTORE_OPEN_POOL(pool_name.c_str(), &options, &message);
    if (pool == nullptr) {
        LOG_ERROR("Open bytestore pool failed")
            .put("PoolName", pool_name)
            .put("PrefixBlobName", dir_name)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }

    files->clear();
    bytestore_traverse_options traverse_options = bytestore_default_traverse_options();
    traverse_options.timeout_in_ms_ = 5000L;
    traverse_options.return_blob_id = true;
    bytestore_entry entry;
    while (BYTESTORE_TRAVERSE_POOL(pool, &entry, &traverse_options, &message)) {
        if (message.status_ != STATUS_OK) {
            LOG_ERROR("Traverse bytestore pool failed")
                .put("PoolName", pool_name)
                .put("PrefixBlobName", pool_name)
                .put("Status", ByteStoreMessageToStatus(message));
            ctrl->set_status(ByteStoreMessageToStatus(message));
            return;
        }
        std::string origin_name = entry.name_;
        pos = origin_name.find('/');
        if (pos == std::string::npos) {
            LOG_ERROR("Blob name invalid").put("Uri", path).put("BlobName", origin_name);
            ctrl->set_status(Status::Internal("Blob name invalid"));
            return;
        }
        BlobInfo info;
        info.name = origin_name.substr(pos + 1);
        files->push_back(info);
    }
    if (message.status_ != STATUS_OK) {
        LOG_ERROR("Traverse bytestore pool failed")
            .put("PoolName", pool_name)
            .put("PrefixBlobName", pool_name)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }

    BYTESTORE_CLOSE_POOL(pool, &message);
    if (message.status_ != STATUS_OK) {
        LOG_ERROR("Close bytestore pool failed")
            .put("PoolName", pool_name)
            .put("PrefixBlobName", pool_name)
            .put("Status", ByteStoreMessageToStatus(message));
    }

    ctrl->set_status(Status::OK());
    return;
}

void ByteStoreImpl::Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
                         Blob** blob) {
    LOG_CALL_DEBUG().put("Uri", uri);
    TimeCost time_cost;
    int open_mode = 0;
    if (options.mode == OpenMode::kRead) {
        open_mode = BS_O_RDONLY;
    } else if (options.mode == OpenMode::kWrite) {
        open_mode = BS_O_RDWR | BS_O_CREATE | BS_O_ASYNC;
    } else {
        ctrl->set_status(Status::InvalidArgument("Invalid open mode"));
        return;
    }

    bytestore_open_options open_options = bytestore_default_open_options();
    SetCondition(options.condition, &open_options.condition_);
    open_options.timeout_in_ms_ = ctrl->timeout_ms();
    switch (options.rep_policy.value()) {
    case StoreRepPolicy::REP_FLAT:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_FLAT;
        break;
    case StoreRepPolicy::REP_G2:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_G2;
        break;
    case StoreRepPolicy::REP_G3:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_G3;
        break;
    case StoreRepPolicy::REP_G1:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_G1;
        break;
    default:
        LOG_ERROR("Open blob failed, invalid rep policy")
            .put("Uri", uri)
            .put("RepPolicy", options.rep_policy.value());
        ctrl->set_status(Status::InvalidArgument("invalid rep policy"));
        return;
    }

    bytestore_message message;
    bytestore_blob* bs_blob = BYTESTORE_OPEN(uri.c_str(), open_mode, &open_options, &message);
    if (bs_blob == nullptr) {
        LOG_ERROR("Open blob failed")
            .put("Uri", uri)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }

    LOG_INFO("Open blob success").put("Uri", uri);
    *blob = new BlobImpl(uri, options.metrics_manager, bs_blob);
    ctrl->set_status(Status::OK());
}

void ByteStoreImpl::Delete(Controller* ctrl, const std::string& uri, const DeleteOptions& options) {
    LOG_CALL_DEBUG().put("Uri", uri);
    TimeCost time_cost;
    bytestore_delete_options delete_options = bytestore_default_delete_options();
    SetCondition(options.condition, &delete_options.condition_);
    delete_options.permanent_delete_ = true;
    delete_options.timeout_in_ms_ = ctrl->timeout_ms();
    bytestore_message message;
    bool success = BYTESTORE_DELETE(uri.c_str(), &delete_options, &message);
    if (!success) {
        LOG_ERROR("Delete blob failed")
            .put("Uri", uri)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }
    LOG_INFO("Delete blob success").put("Uri", uri);
    ctrl->set_status(Status::OK());
}

void ByteStoreImpl::Freeze(Controller* ctrl, const std::string& uri, const FreezeOptions& options) {
    LOG_CALL_DEBUG().put("Uri", uri);
    TimeCost time_cost;
    bytestore_open_options open_options = bytestore_default_open_options();
    SetCondition(options.condition, &open_options.condition_);
    open_options.timeout_in_ms_ = ctrl->timeout_ms();
    bytestore_message message;
    int open_mode = BS_O_RDWR | BS_O_AEXCL;
    bytestore_blob* bs_blob = BYTESTORE_OPEN(uri.c_str(), open_mode, &open_options, &message);
    if (bs_blob == nullptr) {
        LOG_ERROR("Open blob failed")
            .put("Uri", uri)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }

    BYTESTORE_CLOSE(bs_blob, &message);
    if (message.status_ != STATUS_OK) {
        LOG_WARNING("Close blob failed")
            .put("Uri", uri)
            .put("Status", ByteStoreMessageToStatus(message));
        // The failure of close does not affect freeze
    }

    LOG_INFO("Freeze blob success").put("Uri", uri);
    ctrl->set_status(Status::OK());
}

void ByteStoreImpl::Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
                         BlobStat* stat) {
    TimeCost time_cost;
    bytestore_stat_options stat_options = bytestore_default_stat_options();
    stat_options.timeout_in_ms_ = ctrl->timeout_ms();
    bytestore_stat_t bs_stat;
    bytestore_message message;
    bool success = BYTESTORE_STAT(uri.c_str(), &bs_stat, &stat_options, &message);
    if (!success) {
        LOG_ERROR("Stat blob failed")
            .put("Uri", uri)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }
    stat->size = bs_stat.size_;
    ctrl->set_status(Status::OK());
}

void ByteStoreImpl::Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                           const RenameOptions& options) {
    LOG_CALL_DEBUG().put("SrcUri", src_uri).put("DstUri", dst_uri);
    bytestore_rename_options rename_options = bytestore_default_rename_options();
    SetCondition(options.condition, &rename_options.condition_);
    rename_options.timeout_in_ms_ = ctrl->timeout_ms();
    bytestore_message message;
    if (!BYTESTORE_RENAME(src_uri.c_str(), dst_uri.c_str(), &rename_options, &message)) {
        LOG_ERROR("Rename blob failed")
            .put("SrcUri", src_uri)
            .put("DstUri", dst_uri)
            .put("Status", ByteStoreMessageToStatus(message));
        ctrl->set_status(ByteStoreMessageToStatus(message));
        return;
    }
    LOG_INFO("Rename blob success").put("SrcUri", src_uri).put("DstUri", dst_uri);
    ctrl->set_status(Status::OK());
}

void ByteStoreImpl::SetCondition(const Condition& condition,
                                 bytestore_blob_condition* blob_condition) {
    blob_condition->lock_name_ = condition.name.c_str();
    memcpy(blob_condition->content_, condition.data.data(), Env::kInlineBlobSize);
}

}  // namespace stream
}  // namespace bcache2
